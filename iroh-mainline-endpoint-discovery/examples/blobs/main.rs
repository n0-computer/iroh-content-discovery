//! Discovers a real iroh-blobs provider and downloads its blob.
//!
//! The provider is found through Mainline and the endpoint address index.

use std::{collections::HashSet, time::Duration};

use iroh::{endpoint::presets, protocol::Router};
use iroh_blobs::{
    BlobsProtocol, HashAndFormat, api::downloader::ContentDiscovery, store::mem::MemStore,
};
use iroh_mainline_endpoint_discovery::{AddrIndex, Publisher, infohash_from_blake3};
use n0_error::{Result, StackResultExt, StdResultExt, bail_any};
use n0_future::{StreamExt, stream};
use n0_mainline::{Dht, Id};
use tokio::io::AsyncReadExt;

const DATA: &[u8] = b"hello from iroh-mainline-endpoint-discovery\n";
const DOWNLOAD_BUDGET: Duration = Duration::from_secs(180);

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let server = std::env::var("IROH_ADDR_INDEX")
        .ok()
        .map(|value| value.parse())
        .transpose()
        .std_context("invalid IROH_ADDR_INDEX socket")?;

    let provider_store = MemStore::new();
    let tag = provider_store.blobs().add_bytes(DATA.to_vec()).await?;
    let blob_hash = tag.hash;
    let infohash = Id::from(infohash_from_blake3(&blake3::Hash::from_bytes(
        *blob_hash.as_bytes(),
    )));
    println!("Blob hash: {blob_hash}");
    let provider_ep = iroh::Endpoint::bind(presets::N0)
        .await
        .context("provider endpoint")?;
    let provider_router = Router::builder(provider_ep.clone())
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&provider_store, None))
        .spawn();
    println!("Provider endpoint: {}", provider_ep.id());

    println!("Bootstrapping public Mainline...");
    let dht = Dht::client().std_context("DHT")?;
    if !dht.bootstrapped().await? {
        bail_any!("DHT bootstrap failed");
    }
    let index = match server {
        Some(server) => {
            println!("Using address index server: {server}");
            AddrIndex::udp(dht.clone(), server).await?
        }
        None => {
            println!("Discovering address index servers through Mainline...");
            AddrIndex::discover(dht.clone()).await?
        }
    };
    println!("Publishing the endpoint record and announcing the blob...");
    let publisher = Publisher::new(provider_ep.secret_key().clone(), dht.clone(), index.clone());
    publisher.add_infohash(infohash);
    let download = async {
        publisher.wait_published().await;
        println!("Published. Starting content discovery and download...");
        let client_store = MemStore::new();
        let client_ep = iroh::Endpoint::bind(presets::N0).await?;
        tokio::time::timeout(
            DOWNLOAD_BUDGET,
            client_store
                .downloader(&client_ep)
                .download(blob_hash, MainlineProviders { dht, index }),
        )
        .await
        .anyerr()??;
        let mut reader = client_store.blobs().reader(blob_hash);
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await?;
        println!("Downloaded and verified {} bytes:", data.len());
        print!("{}", String::from_utf8_lossy(&data));
        client_ep.close().await;
        client_store.shutdown().await?;
        n0_error::Ok(())
    };

    // The publisher keeps announcing in its own task until it is dropped.
    tokio::select! {
        result = download => result?,
        _ = tokio::signal::ctrl_c() => bail_any!("interrupted"),
    }
    provider_router.shutdown().await.anyerr()?;
    Ok(())
}

#[derive(Debug)]
struct MainlineProviders {
    dht: Dht,
    index: AddrIndex,
}

impl ContentDiscovery for MainlineProviders {
    fn find_providers(&self, hash: HashAndFormat) -> stream::Boxed<iroh::EndpointId> {
        let infohash = Id::from(infohash_from_blake3(&blake3::Hash::from_bytes(
            *hash.hash.as_bytes(),
        )));
        let dht = self.dht.clone();
        let index = self.index.clone();
        // Spell out the two lookup stages here so this example can print the
        // address-to-endpoint translation without changing library logging.
        async_stream::stream! {
            println!("Looking up Mainline infohash: {infohash}");
            loop {
                let mut peers = match dht.get_peers(infohash).await {
                    Ok(peers) => peers,
                    Err(error) => {
                        eprintln!("Mainline lookup failed: {error}");
                        break;
                    }
                };
                let mut seen = HashSet::new();
                while let Some(batch) = peers.next().await {
                    for peer in batch {
                        if !seen.insert(peer) { continue; }
                        println!("Mainline returned provider address: {peer}");
                        match index.lookup(peer).await {
                            Ok(records) => {
                                if records.is_empty() {
                                    println!("  No valid endpoint record; skipping.");
                                }
                                for record in records {
                                    println!("Address index: {peer} -> {}", record.endpoint_id);
                                    println!("Trying this endpoint with the iroh-blobs downloader...");
                                    yield record.endpoint_id;
                                }
                            }
                            Err(error) => eprintln!("Address index lookup for {peer} failed: {error}"),
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }.boxed()
    }
}
