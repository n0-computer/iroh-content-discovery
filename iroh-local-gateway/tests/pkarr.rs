//! Pkarr redirects over a local DHT and a real HTTP listener.

use std::time::Duration;

use iroh::{Endpoint, endpoint::presets};
use iroh_local_gateway::Gateway;
use iroh_mainline_endpoint_discovery::{Directory, Resolver};
use n0_mainline::{Dht, MutableItem};
use pkarr::{Keypair, SignedPacket, dns::rdata::SVCB};
use reqwest::{Client, StatusCode};

async fn publish(dht: &Dht, key: &Keypair, target: &str) {
    let packet = SignedPacket::builder()
        .https(
            ".".try_into().unwrap(),
            SVCB::new(0, target.try_into().unwrap()),
            60,
        )
        .sign(key)
        .unwrap();
    let item = MutableItem::new_signed_unchecked(
        *key.public_key().as_bytes(),
        packet.signature().to_bytes(),
        &packet.encoded_packet(),
        packet.timestamp().as_u64() as i64,
        None,
    );
    dht.put_mutable(item, None).await.unwrap();
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
    // Pkarr only uses the DHT; no content tracker or provider is needed.
    let directory = Directory::udp(gateway_dht.clone(), "127.0.0.1:9".parse().unwrap())
        .await
        .unwrap();
    let resolver = Resolver::bind(gateway_dht, directory).await.unwrap();
    let endpoint = Endpoint::bind(presets::Minimal).await.unwrap();
    let gateway = Gateway::new(endpoint.clone(), resolver);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/pkarr", listener.local_addr().unwrap());
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
    let key = Keypair::from_secret_key(&[42; 32]);
    let url = format!("{base}/{}", key.public_key().to_z32());
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
    // A content-addressed hostname is returned unchanged, just like any domain.
    let target = format!("{}.blake3.link", z32::encode(&[1; 32]));
    publish(&publisher, &key, &target).await;
    let response = client.head(format!("{url}/file")).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        response.headers()["location"],
        format!("https://{target}/file")
    );
    assert!(response.bytes().await.unwrap().is_empty());

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
    let missing = Keypair::from_secret_key(&[43; 32]);
    assert_eq!(
        client
            .get(format!("{base}/{}", missing.public_key().to_z32()))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );

    shutdown.send(()).unwrap();
    task.await.unwrap();
    endpoint.close().await;
}
