//! Discover a real iroh-blobs provider through Mainline and the endpoint
//! address index, then download its blob.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use iroh::{endpoint::presets, protocol::Router};
use iroh_blobs::{
    BlobsProtocol, HashAndFormat, api::downloader::ContentDiscovery, store::mem::MemStore,
};
use iroh_mainline_endpoint_discovery::{Directory, Publisher, Resolver, infohash_from_blake3};
use n0_future::stream;
use n0_mainline::{Dht, Id};
use tokio::io::AsyncReadExt;

const DATA: &[u8] = b"hello from iroh-mainline-endpoint-discovery\n";
const DOWNLOAD_BUDGET: Duration = Duration::from_secs(180);

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("info,udp_address_records=trace")
            }),
        )
        .init();

    let replica = std::env::var("IROH_ADDR_INDEX")
        .ok()
        .map(|value| value.parse())
        .transpose()
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
    let index = match replica {
        Some(replica) => Directory::udp(dht.clone(), replica).await?,
        None => Directory::discover(dht.clone()).await?,
    };
    let publisher = Publisher::new(provider_ep.secret_key().clone(), dht.clone(), index.clone());
    publisher.add_infohash(infohash);
    let resolver = Resolver::bind(dht, index).await?;

    println!("blob {blob_hash}");
    println!("provider {}", provider_ep.id());
    let download = async {
        publisher.wait_published().await;
        let client_store = MemStore::new();
        let client_ep = iroh::Endpoint::bind(presets::N0).await?;
        tokio::time::timeout(
            DOWNLOAD_BUDGET,
            client_store
                .downloader(&client_ep)
                .download(blob_hash, MainlineProviders(resolver)),
        )
        .await??;
        let mut reader = client_store.blobs().reader(blob_hash);
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await?;
        println!("downloaded: {}", String::from_utf8_lossy(&data));
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

#[derive(Debug)]
struct MainlineProviders(Resolver);

impl ContentDiscovery for MainlineProviders {
    fn find_providers(&self, hash: HashAndFormat) -> stream::Boxed<iroh::EndpointId> {
        let infohash = Id::from(infohash_from_blake3(&blake3::Hash::from_bytes(
            *hash.hash.as_bytes(),
        )));
        self.0.resolve_continuously(infohash)
    }
}
