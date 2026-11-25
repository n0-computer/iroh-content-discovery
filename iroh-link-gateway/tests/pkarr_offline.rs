//! A gateway with a store opens a Pkarr site it has seen without a network.

use std::{
    net::{Ipv4Addr, SocketAddrV4},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use iroh::{Endpoint, address_lookup::memory::MemoryLookup, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, format::collection::Collection, store::mem::MemStore};
use iroh_blobs_cache::{BlobCache, CacheConfig};
use iroh_link_gateway::Gateway;
use iroh_mainline_endpoint_discovery::{AddrIndex, BLAKE3_DOMAIN, Resolver, infohash_from_blake3};
use n0_mainline::{Dht, MutableItem, SigningKey};
use reqwest::{Client, StatusCode};
use simple_dns::{
    CLASS, Packet, ResourceRecord,
    rdata::{HTTPS, RData, SVCB},
};
use udp_addr_index::{Limits, Server};

#[tokio::test]
async fn kept_records_open_sites_offline() {
    tokio::time::timeout(Duration::from_secs(90), run())
        .await
        .expect("offline Pkarr test timed out");
}

async fn run() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let network = n0_mainline::Testnet::new(3).await.unwrap();
    let node = || {
        Dht::builder()
            .bootstrap(&network.bootstrap)
            .port(0)
            .build()
            .unwrap()
    };
    let server = Server::new(Limits::for_tests());
    let server_handle = server.attach(node()).await.unwrap();
    let server_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, server_handle.local_addr().port());

    let provider_store = MemStore::new();
    let page = provider_store
        .blobs()
        .add_bytes(b"<p>named</p>".to_vec())
        .await
        .unwrap();
    let website = Collection::from_iter([("index.html", page.hash)])
        .store(&provider_store)
        .await
        .unwrap();
    let provider = Endpoint::builder(presets::Minimal)
        .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
        .unwrap()
        .bind()
        .await
        .unwrap();
    let provider_router = Router::builder(provider.clone())
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&provider_store, None))
        .spawn();
    let provider_dht = node();
    AddrIndex::udp(provider_dht.clone(), server_addr)
        .await
        .unwrap()
        .publish(provider.secret_key())
        .await
        .unwrap();
    provider_dht
        .announce_peer(
            infohash_from_blake3(&blake3::Hash::from_bytes(*website.hash().as_bytes())).into(),
            None,
        )
        .await
        .unwrap();

    // The name points at the website.
    let key = SigningKey::from_bytes(&[7; 32]);
    let name = z32::encode(key.verifying_key().as_bytes());
    let target = format!("{}.{BLAKE3_DOMAIN}", z32::encode(website.hash().as_bytes()));
    let mut packet = Packet::new_reply(0);
    packet.answers.push(ResourceRecord::new(
        name.as_str().try_into().unwrap(),
        CLASS::IN,
        300,
        RData::HTTPS(HTTPS(SVCB::new(0, target.as_str().try_into().unwrap()))),
    ));
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    let bytes = packet.build_bytes_vec_compressed().unwrap();
    node()
        .put_mutable(MutableItem::new(&key, &bytes, timestamp, None), None)
        .await
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let cache = BlobCache::load(dir.path(), CacheConfig::new(1 << 30))
        .await
        .unwrap();
    let endpoint = Endpoint::builder(presets::Minimal)
        .address_lookup(MemoryLookup::from_endpoint_info([provider.addr()]))
        .bind()
        .await
        .unwrap();
    let serve = |gateway: Gateway| async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { gateway.serve(listener, std::future::pending()).await });
        base
    };
    let client = Client::builder().no_proxy().build().unwrap();

    // Online: the name resolves, and the record and the site are kept.
    let gateway_dht = node();
    let index = AddrIndex::udp(gateway_dht.clone(), server_addr)
        .await
        .unwrap();
    let base = serve(Gateway::with_cache(
        endpoint.clone(),
        Some(Resolver::new(gateway_dht, index)),
        cache.clone(),
    ))
    .await;
    let res = client
        .get(format!("{base}/pkarr/{name}/"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.bytes().await.unwrap().as_ref(), b"<p>named</p>");

    // Offline: no provider, and a gateway that has no resolver at all.
    provider_router.shutdown().await.unwrap();
    provider.close().await;
    let base = serve(Gateway::with_cache(endpoint.clone(), None, cache.clone())).await;
    let started = Instant::now();
    let res = client
        .get(format!("{base}/pkarr/{name}/"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.bytes().await.unwrap().as_ref(), b"<p>named</p>");
    // The kept record is used after a short wait, not the full lookup timeout.
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
    // Further requests reuse it at once.
    let started = Instant::now();
    let res = client
        .get(format!("{base}/pkarr/{name}/index.html"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    cache.shutdown().await.unwrap();
}
