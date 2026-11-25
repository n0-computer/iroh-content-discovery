//! A gateway with a store serves content it has seen without a provider.

use std::{
    net::{Ipv4Addr, SocketAddrV4},
    time::Duration,
};

use iroh::{Endpoint, address_lookup::memory::MemoryLookup, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, format::collection::Collection, store::mem::MemStore};
use iroh_blobs_cache::{BlobCache, CacheConfig};
use iroh_link_gateway::Gateway;
use iroh_mainline_endpoint_discovery::{AddrIndex, Resolver};
use n0_mainline::Dht;
use reqwest::{Client, StatusCode, header};
use udp_addr_index::{Limits, Server};

/// Larger than the store's streaming window, so it streams in several steps.
const BIG: usize = 6 * 1024 * 1024 + 123;

#[tokio::test]
async fn stored_content_is_served_without_a_provider() {
    tokio::time::timeout(Duration::from_secs(120), run())
        .await
        .expect("store integration timed out");
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

    let big: Vec<u8> = (0..BIG).map(|index| (index % 251) as u8).collect();
    let provider_store = MemStore::new();
    let page = provider_store
        .blobs()
        .add_bytes(b"<p>page</p>".to_vec())
        .await
        .unwrap();
    let wasm = provider_store.blobs().add_bytes(big.clone()).await.unwrap();
    let website = Collection::from_iter([("index.html", page.hash), ("app.wasm", wasm.hash)])
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

    let dir = tempfile::tempdir().unwrap();
    let cache = BlobCache::load(dir.path(), CacheConfig::new(1 << 30))
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
    let resolver = Resolver::new(gateway_dht, index);
    let serve = |gateway: Gateway| async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { gateway.serve(listener, std::future::pending()).await });
        base
    };
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let hash = z32::encode(website.hash().as_bytes());
    let id = z32::encode(provider.id().as_bytes());

    // Online: a range first, then the whole file, which only needs the rest.
    let base = serve(Gateway::with_cache(
        client_endpoint.clone(),
        resolver.clone(),
        cache.clone(),
    ))
    .await;
    let res = client
        .get(format!("{base}/blake3/{hash}/?provider={id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.bytes().await.unwrap().as_ref(), b"<p>page</p>");
    let res = client
        .get(format!("{base}/blake3/{hash}/app.wasm"))
        .header(header::RANGE, "bytes=5000000-5000099")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        res.bytes().await.unwrap().as_ref(),
        &big[5_000_000..5_000_100]
    );
    let res = client
        .get(format!("{base}/blake3/{hash}/app.wasm"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.bytes().await.unwrap().as_ref(), big.as_slice());

    // Served blobs are cached: the collection root, its names, both files.
    let usage = cache.evict().await.unwrap();
    assert_eq!(usage.blobs, 4, "{usage:?}");

    // Offline: the provider is gone, and a new gateway has no memory of it.
    provider_router.shutdown().await.unwrap();
    provider.close().await;
    let base = serve(Gateway::with_cache(
        client_endpoint.clone(),
        resolver,
        cache.clone(),
    ))
    .await;
    let res = client
        .get(format!("{base}/blake3/{hash}/"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.bytes().await.unwrap().as_ref(), b"<p>page</p>");
    let res = client
        .get(format!("{base}/blake3/{hash}/app.wasm"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()[header::CONTENT_TYPE], "application/wasm");
    assert_eq!(res.bytes().await.unwrap().as_ref(), big.as_slice());
    let res = client
        .get(format!("{base}/blake3/{hash}/?listing&sizes"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    cache.shutdown().await.unwrap();
}
