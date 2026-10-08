//! Providers named with `?provider=`, asked before Mainline.

use std::{
    net::{Ipv4Addr, SocketAddrV4},
    time::Duration,
};

use iroh::{Endpoint, address_lookup::memory::MemoryLookup, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, format::collection::Collection, store::mem::MemStore};
use iroh_link_gateway::Gateway;
use iroh_mainline_endpoint_discovery::{AddrIndex, Resolver, infohash_from_blake3};
use n0_mainline::Dht;
use reqwest::{Client, StatusCode};
use udp_addr_index::{Limits, Server};

/// reqwest 0.13 builds rustls without a crypto provider here, so install one
/// before building a client.
fn client_builder() -> reqwest::ClientBuilder {
    let _ = rustls::crypto::ring::default_provider().install_default();
    Client::builder()
}

#[tokio::test]
async fn named_providers_serve_unannounced_content() {
    tokio::time::timeout(Duration::from_secs(90), run())
        .await
        .expect("hints integration timed out");
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
    let server = Server::new(Limits::for_tests());
    let server_handle = server.attach(node()).await.unwrap();
    let server_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, server_handle.local_addr().port());

    let store = MemStore::new();
    let page = store
        .blobs()
        .add_bytes(b"<p>page</p>".to_vec())
        .await
        .unwrap();
    let style = store.blobs().add_bytes(b"p {}".to_vec()).await.unwrap();
    let website = Collection::from_iter([("index.html", page.hash), ("style.css", style.hash)]);
    let website = website.store(&store).await.unwrap();
    let announced = Collection::from_iter([("index.html", style.hash)]);
    let announced = announced.store(&store).await.unwrap();
    let provider = Endpoint::builder(presets::Minimal)
        .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
        .unwrap()
        .bind()
        .await
        .unwrap();
    let provider_router = Router::builder(provider.clone())
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&store, None))
        .spawn();
    // Only `announced` is announced; `website` is reachable through links alone.
    let publisher_dht = node();
    let index = AddrIndex::udp(publisher_dht.clone(), server_addr)
        .await
        .unwrap();
    index.publish(provider.secret_key()).await.unwrap();
    let infohash = infohash_from_blake3(&blake3::Hash::from_bytes(*announced.hash().as_bytes()));
    publisher_dht
        .announce_peer(infohash.into(), None)
        .await
        .unwrap();

    let client_endpoint = Endpoint::builder(presets::Minimal)
        .address_lookup(MemoryLookup::from_endpoint_info([provider.addr()]))
        .bind()
        .await
        .unwrap();
    let gateway_dht = node();
    let index = AddrIndex::udp(gateway_dht.clone(), server_addr)
        .await
        .unwrap();
    let gateway = Gateway::new(client_endpoint.clone(), Resolver::new(gateway_dht, index));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen_addr = listener.local_addr().unwrap();
    let base = format!("http://{listen_addr}");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        gateway
            .serve(listener, async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let hash = z32::encode(website.hash().as_bytes());
    let id = z32::encode(provider.id().as_bytes());
    let host = format!("{hash}.blake3.localhost");
    let client = client_builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .resolve(&host, listen_addr)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let get = |url: String| {
        let client = client.clone();
        async move { client.get(url).send().await.unwrap() }
    };

    // Nobody announced it, so without a hint it is not found.
    let res = get(format!("{base}/blake3/{hash}/")).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    // A malformed provider is rejected rather than ignored.
    let res = get(format!("{base}/blake3/{hash}/?provider=nope")).await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // A hint serves it on the same URL, despite the lookup that just failed.
    let res = get(format!("{base}/blake3/{hash}/?provider={id}")).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.bytes().await.unwrap().as_ref(), b"<p>page</p>");
    // Subresources carry no query, and the remembered hint serves them.
    let res = get(format!("{base}/blake3/{hash}/style.css")).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.bytes().await.unwrap().as_ref(), b"p {}");
    // The subdomain origin is the same with or without a hint, here in hex.
    let port = listen_addr.port();
    let res = get(format!("http://{host}:{port}/?provider={}", provider.id())).await;
    assert_eq!(res.status(), StatusCode::OK);
    let res = get(format!("http://{host}:{port}/style.css")).await;
    assert_eq!(res.status(), StatusCode::OK);

    // The debug page lists the named providers, with their probes.
    let res = get(format!("{base}/blake3/{hash}/?debug")).await;
    assert_eq!(res.status(), StatusCode::OK);
    let page = res.text().await.unwrap();
    assert!(page.contains("named by links"));
    assert!(page.contains(&provider.id().to_string()));

    // A hint that does not answer falls back to Mainline after its head start.
    let dead = z32::encode(iroh::SecretKey::from_bytes(&[7; 32]).public().as_bytes());
    let announced = z32::encode(announced.hash().as_bytes());
    let res = get(format!("{base}/blake3/{announced}/?provider={dead}")).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.bytes().await.unwrap().as_ref(), b"p {}");

    shutdown_tx.send(()).unwrap();
    task.await.unwrap();
    client_endpoint.close().await;
    provider_router.shutdown().await.unwrap();
}
