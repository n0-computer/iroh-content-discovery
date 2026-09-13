//! Discover a real iroh-blobs provider through Mainline and the endpoint
//! address index, then download its blob.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use iroh::{endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, store::mem::MemStore};
use iroh_mainline_endpoint_discovery::{Directory, Publisher, Resolver, infohash_from_blake3};
use n0_mainline::{Dht, Id};
use tokio::io::AsyncReadExt;

const DATA: &[u8] = b"hello from iroh-mainline-endpoint-discovery\n";
const RESOLVE_BUDGET: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("info,iroh_addr_index=trace")
            }),
        )
        .init();

    let replica = std::env::var("IROH_ADDR_INDEX")
        .context("set IROH_ADDR_INDEX to a public address-index UDP socket")?
        .parse()
        .context("invalid IROH_ADDR_INDEX socket")?;

    let provider_store = MemStore::new();
    let tag = provider_store.blobs().add_bytes(DATA.to_vec()).await?;
    let blob_hash = tag.hash;
    let infohash = Id::from(infohash_from_blake3(&blake3::Hash::from_bytes(
        *blob_hash.as_bytes(),
    )));
    let provider_ep = iroh::Endpoint::bind(presets::N0)
        .await
        .context("provider endpoint")?;
    let provider_router = Router::builder(provider_ep.clone())
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&provider_store, None))
        .spawn();

    let dht = Dht::client().context("DHT")?;
    if !dht.bootstrapped().await? {
        bail!("DHT bootstrap failed");
    }
    let index = Directory::udp(dht.clone(), replica).await?;
    let publisher = Publisher::new(provider_ep.secret_key().clone(), dht.clone(), index.clone());
    publisher.add_infohash(infohash);
    let resolver = Resolver::bind(dht, index).await?;

    println!("blob {blob_hash}");
    println!("provider {}", provider_ep.id());
    let download = async {
        publisher.wait_published().await;
        let eid = resolve_provider(&resolver, infohash).await?;
        let client_store = MemStore::new();
        let client_ep = iroh::Endpoint::bind(presets::N0).await?;
        client_store
            .downloader(&client_ep)
            .download(blob_hash, Some(eid))
            .await?;
        let mut reader = client_store.blobs().reader(blob_hash);
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await?;
        println!("downloaded from {eid}: {}", String::from_utf8_lossy(&data));
        client_ep.close().await;
        client_store.shutdown().await?;
        anyhow::Ok(())
    };

    tokio::select! {
        result = publisher.run() => result?,
        result = download => result?,
        _ = tokio::signal::ctrl_c() => bail!("interrupted"),
    }
    provider_router.shutdown().await?;
    Ok(())
}

async fn resolve_provider(resolver: &Resolver, infohash: Id) -> Result<iroh::EndpointId> {
    let deadline = Instant::now() + RESOLVE_BUDGET;
    loop {
        if let Some(eid) = resolver.resolve(infohash).await?.into_iter().next() {
            return Ok(eid);
        }
        if Instant::now() >= deadline {
            bail!("timed out resolving {infohash}");
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
