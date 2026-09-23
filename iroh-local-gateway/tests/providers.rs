//! Provider filtering against real local iroh endpoints.

use std::time::Duration;

use iroh::{Endpoint, address_lookup::memory::MemoryLookup, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, Hash, store::mem::MemStore};
use iroh_local_gateway::filter_verified_providers;
use n0_future::{StreamExt, stream};

async fn endpoint() -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
        .unwrap()
        .alpns(vec![iroh_blobs::ALPN.to_vec()])
        .bind()
        .await
        .unwrap()
}

#[tokio::test]
async fn skips_missing_content_and_deduplicates_providers() {
    let good = endpoint().await;
    let missing = endpoint().await;
    let store = MemStore::new();
    let tag = store.blobs().add_bytes(vec![42; 100_000]).await.unwrap();
    let good_router = Router::builder(good.clone())
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&store, None))
        .spawn();
    let missing_router = Router::builder(missing.clone())
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&MemStore::new(), None))
        .spawn();
    let client = Endpoint::builder(presets::Minimal)
        .address_lookup(MemoryLookup::from_endpoint_info([
            good.addr(),
            missing.addr(),
        ]))
        .bind()
        .await
        .unwrap();
    let peers = filter_verified_providers(
        client.clone(),
        tag.hash,
        stream::iter([missing.id(), good.id(), good.id()]),
    );
    let result = tokio::time::timeout(Duration::from_secs(5), peers.collect::<Vec<_>>())
        .await
        .unwrap();
    assert_eq!(result, [good.id()]);
    client.close().await;
    good_router.shutdown().await.unwrap();
    missing_router.shutdown().await.unwrap();
}

#[tokio::test]
async fn stalled_provider_does_not_block_healthy_provider() {
    let stalled = endpoint().await; // No accept loop: handshake cannot complete.
    let good = endpoint().await;
    let store = MemStore::new();
    let tag = store.blobs().add_bytes(Vec::new()).await.unwrap();
    let router = Router::builder(good.clone())
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&store, None))
        .spawn();
    let client = Endpoint::builder(presets::Minimal)
        .address_lookup(MemoryLookup::from_endpoint_info([
            stalled.addr(),
            good.addr(),
        ]))
        .bind()
        .await
        .unwrap();
    let mut peers = filter_verified_providers(
        client.clone(),
        tag.hash,
        stream::iter([stalled.id(), good.id()]),
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), peers.next())
            .await
            .unwrap(),
        Some(good.id())
    );
    drop(peers);
    client.close().await;
    stalled.close().await;
    router.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejects_size_header_with_invalid_content() {
    let liar = endpoint().await;
    let client = Endpoint::builder(presets::Minimal)
        .address_lookup(MemoryLookup::from_endpoint_info([liar.addr()]))
        .bind()
        .await
        .unwrap();
    let server = liar.clone();
    let task = tokio::spawn(async move {
        let connection = server.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        recv.read_to_end(1024).await.unwrap();
        send.write_all(&3u64.to_le_bytes()).await.unwrap();
        send.write_all(b"bad").await.unwrap();
        send.finish().unwrap();
        let _ = send.stopped().await;
    });
    let mut peers =
        filter_verified_providers(client.clone(), Hash::new(b"yes"), stream::iter([liar.id()]));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), peers.next())
            .await
            .unwrap(),
        None
    );
    task.await.unwrap();
    client.close().await;
    liar.close().await;
}
