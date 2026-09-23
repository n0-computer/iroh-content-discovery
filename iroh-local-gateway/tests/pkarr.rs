//! Pkarr redirects over a local DHT and a real HTTP listener.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use iroh::{Endpoint, address_lookup::memory::MemoryLookup, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, store::mem::MemStore};
use iroh_local_gateway::Gateway;
use iroh_mainline_endpoint_discovery::{
    AddrIndex, BLAKE3_DOMAIN, Resolver, SignedRecord, infohash_from_blake3,
};
use n0_mainline::{Dht, MutableItem, SigningKey};
use reqwest::{Client, StatusCode};
use simple_dns::{
    CLASS, Packet, ResourceRecord,
    rdata::{HTTPS, RData, SVCB},
};
use udp_addr_index::{Limits, Server};

async fn publish(dht: &Dht, key: &SigningKey, target: &str) {
    publish_ttl(dht, key, target, 0).await;
}

async fn publish_ttl(dht: &Dht, key: &SigningKey, target: &str, ttl: u32) {
    let name = z32::encode(key.verifying_key().as_bytes());
    let mut packet = Packet::new_reply(0);
    packet.answers.push(ResourceRecord::new(
        name.as_str().try_into().unwrap(),
        CLASS::IN,
        ttl,
        RData::HTTPS(HTTPS(SVCB::new(0, target.try_into().unwrap()))),
    ));
    publish_bytes(dht, key, &packet.build_bytes_vec_compressed().unwrap()).await;
}

async fn publish_bytes(dht: &Dht, key: &SigningKey, bytes: &[u8]) {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    dht.put_mutable(MutableItem::new(key, bytes, timestamp, None), None)
        .await
        .unwrap();
}

#[tokio::test]
async fn redirects_signed_records() {
    tokio::time::timeout(Duration::from_secs(60), run())
        .await
        .unwrap();
}

async fn run() {
    let network = n0_mainline::Testnet::new(3).await.unwrap();
    let node = || {
        Dht::builder()
            .bootstrap(&network.bootstrap)
            .port(0)
            .build()
            .unwrap()
    };
    let publisher = node();
    let gateway_dht = node();
    // A content-addressed target is served inline, so the gateway needs a
    // index server and a provider as well as the DHT.
    let server = Server::new(Limits::for_tests());
    let server_handle = server.attach(node()).await.unwrap();
    let server_addr = std::net::SocketAddrV4::new(
        std::net::Ipv4Addr::LOCALHOST,
        server_handle.local_addr().port(),
    );
    let text = b"served through a Pkarr name\n".to_vec();
    let store = MemStore::new();
    let text_tag = store.blobs().add_bytes(text.clone()).await.unwrap();
    let provider = Endpoint::builder(presets::Minimal)
        .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
        .unwrap()
        .bind()
        .await
        .unwrap();
    let provider_router = Router::builder(provider.clone())
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&store, None))
        .spawn();
    let provider_dht = node();
    AddrIndex::udp(provider_dht.clone(), server_addr)
        .await
        .unwrap()
        .publish(&SignedRecord::sign(provider.secret_key()))
        .await
        .unwrap();
    provider_dht
        .announce_peer(
            infohash_from_blake3(&blake3::Hash::from_bytes(*text_tag.hash.as_bytes())).into(),
            None,
        )
        .await
        .unwrap();
    let index = AddrIndex::udp(gateway_dht.clone(), server_addr)
        .await
        .unwrap();
    let resolver = Resolver::bind(gateway_dht, index).await.unwrap();
    let endpoint = Endpoint::builder(presets::Minimal)
        .address_lookup(MemoryLookup::from_endpoint_info([provider.addr()]))
        .bind()
        .await
        .unwrap();
    let gateway = Gateway::new(endpoint.clone(), resolver);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen_addr = listener.local_addr().unwrap();
    let base = format!("http://{listen_addr}/pkarr");
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        gateway
            .serve(listener, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let key = SigningKey::from_bytes(&[42; 32]);
    let url = format!("{base}/{}", z32::encode(key.verifying_key().as_bytes()));
    publish(&publisher, &key, "example.com").await;
    for (suffix, path) in [
        ("", "/"),
        ("/", "/"),
        ("?a=b", "/?a=b"),
        (
            "/a%2Fb/file%20name?x=%2F&y=1",
            "/a%2Fb/file%20name?x=%2F&y=1",
        ),
    ] {
        let response = client.get(format!("{url}{suffix}")).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            response.headers()["location"],
            format!("https://example.com{path}")
        );
        assert_eq!(response.headers()["cache-control"], "no-store");
    }
    // The per-key origin resolves the same record as the path route.
    let encoded_key = z32::encode(key.verifying_key().as_bytes());
    let origin_client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .resolve(&format!("{encoded_key}.pkarr.localhost"), listen_addr)
        .build()
        .unwrap();
    let response = origin_client
        .get(format!(
            "http://{encoded_key}.pkarr.localhost:{}/a/b?x=1",
            listen_addr.port()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        response.headers()["location"],
        "https://example.com/a/b?x=1"
    );

    // A content-addressed hostname is served here, not redirected to.
    let target = format!("{}.{BLAKE3_DOMAIN}", z32::encode(text_tag.hash.as_bytes()));
    publish(&publisher, &key, &target).await;
    let response = client.get(&url).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    // The key names content that changes, so the response must revalidate.
    assert_eq!(response.headers()["cache-control"], "public, no-cache");
    assert_eq!(response.bytes().await.unwrap().as_ref(), text);
    let response = origin_client
        .get(format!(
            "http://{encoded_key}.pkarr.localhost:{}/",
            listen_addr.port()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().as_ref(), text);
    // Content that the key does not name is still a miss.
    let response = client.get(format!("{url}/missing")).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    publish(&publisher, &key, ".").await;
    assert_eq!(
        client.get(&url).send().await.unwrap().status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        client
            .get(format!("{base}/invalid"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let missing = SigningKey::from_bytes(&[43; 32]);
    assert_eq!(
        client
            .get(format!(
                "{base}/{}",
                z32::encode(missing.verifying_key().as_bytes())
            ))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );

    // A valid BEP44 signature does not imply valid DNS data.
    publish_bytes(&publisher, &key, b"not a DNS packet").await;
    assert_eq!(
        client.get(&url).send().await.unwrap().status(),
        StatusCode::BAD_GATEWAY
    );

    // A warm cache serves the verified packet, applying each request's path
    // independently, even after a different record is published.
    let cached_key = SigningKey::from_bytes(&[44; 32]);
    let cached_url = format!(
        "{base}/{}",
        z32::encode(cached_key.verifying_key().as_bytes())
    );
    publish_ttl(&publisher, &cached_key, "cached.example", 300).await;
    let response = client.get(&cached_url).send().await.unwrap();
    assert_eq!(response.headers()["location"], "https://cached.example/");
    publish_ttl(&publisher, &cached_key, "changed.example", 300).await;
    let response = client
        .get(format!("{cached_url}/other?x=1"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.headers()["location"],
        "https://cached.example/other?x=1"
    );

    shutdown.send(()).unwrap();
    task.await.unwrap();
    endpoint.close().await;
    provider_router.shutdown().await.unwrap();
}
