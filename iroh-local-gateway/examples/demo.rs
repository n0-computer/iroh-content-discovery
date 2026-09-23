//! One-command local Pkarr, tracker, blob provider, and HTTP gateway demo.

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use clap::Parser;
use iroh::{Endpoint, address_lookup::memory::MemoryLookup, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, store::fs::FsStore};
use iroh_local_gateway::Gateway;
use iroh_mainline_endpoint_discovery::{Directory, Publisher, Resolver, infohash_from_blake3};
use n0_mainline::{Dht, MutableItem, SigningKey, Testnet};
use simple_dns::{
    CLASS, Packet, ResourceRecord,
    rdata::{HTTPS, RData, SVCB},
};
use udp_address_records::{Limits, Server};

#[derive(Parser)]
#[command(
    about = "Serve a file via Pkarr -> blake3.link -> local gateway, with a local DHT and peer"
)]
struct Args {
    /// File to serve, for example an MP4. Without a file, serve a text greeting.
    file: Option<PathBuf>,
    /// Local HTTP port; configure the same port in the browser extension.
    #[arg(long, default_value_t = 8080)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    // Bind first so an occupied port fails before importing a potentially large file.
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, args.port)).await?;
    let http_addr = listener.local_addr()?;
    let temporary = tempfile::tempdir()?;
    let store = FsStore::load(temporary.path()).await?;
    let tag = match args.file {
        Some(path) => {
            println!("Importing {}...", path.display());
            store.blobs().add_path(path.canonicalize()?).await?
        }
        None => {
            store
                .blobs()
                .add_bytes(b"Hello through pkarr.link, blake3.link, and an iroh peer!\n".to_vec())
                .await?
        }
    };
    let hash = tag.hash;
    let encoded = z32::encode(hash.as_bytes());
    let infohash = infohash_from_blake3(&blake3::Hash::from_bytes(*hash.as_bytes())).into();
    let network = Testnet::new(3).await?;
    let node = || Dht::builder().bootstrap(&network.bootstrap).port(0).build();
    let tracker = Server::new(Limits::for_tests());
    let tracker_handle = tracker.attach(node()?).await?;
    let tracker_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, tracker_handle.local_addr().port());
    let provider = Endpoint::builder(presets::Minimal)
        .bind_addr("127.0.0.1:0".parse::<SocketAddr>()?)?
        .bind()
        .await?;
    let router = Router::builder(provider.clone())
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&store, None))
        .spawn();
    let provider_dht = node()?;
    let directory = Directory::udp(provider_dht.clone(), tracker_addr).await?;
    let publisher = Publisher::new(
        provider.secret_key().clone(),
        provider_dht.clone(),
        directory,
    );
    publisher.add_infohash(infohash);
    let key = SigningKey::from_bytes(&rand::random());
    let public_key = z32::encode(key.verifying_key().as_bytes());
    let target = format!("{encoded}.blake3.link");
    let mut packet = Packet::new_reply(0);
    packet.answers.push(ResourceRecord::new(
        public_key.as_str().try_into()?,
        CLASS::IN,
        300,
        RData::HTTPS(HTTPS(SVCB::new(0, target.as_str().try_into()?))),
    ));
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_micros()
        .try_into()?;
    let item = MutableItem::new(&key, &packet.build_bytes_vec_compressed()?, timestamp, None);
    drop(key);
    let client = Endpoint::builder(presets::Minimal)
        .address_lookup(MemoryLookup::from_endpoint_info([provider.addr()]))
        .bind()
        .await?;
    let gateway_dht = node()?;
    let directory = Directory::udp(gateway_dht.clone(), tracker_addr).await?;
    let gateway = Gateway::new(
        client.clone(),
        Resolver::bind(gateway_dht, directory).await?,
    );
    let serve = async {
        publisher.wait_published().await;
        tokio::time::timeout(
            Duration::from_secs(30),
            provider_dht.put_mutable(item.clone(), None),
        )
        .await??;
        println!("\nExtension port: {}", http_addr.port());
        println!("Open:         https://{public_key}.pkarr.link/");
        println!("Redirects to: https://{encoded}.blake3.link/");
        println!("Pkarr route:  http://{http_addr}/pkarr/{public_key}/");
        println!("Blob route:   http://{http_addr}/blake3/{encoded}");
        println!("\nLeave this running. Press Ctrl-C to stop.");
        tokio::select! {
            result = gateway.serve(listener, async { let _ = tokio::signal::ctrl_c().await; }) => result,
            result = renew_pkarr(&provider_dht, &item) => result,
        }
    };
    let result = tokio::select! {
        result = publisher.run() => result.map_err(anyhow::Error::from),
        result = serve => result,
    };
    client.close().await;
    router.shutdown().await?;
    // Router shutdown also shuts down the blob store before TempDir cleanup.
    result
}

async fn renew_pkarr(dht: &Dht, item: &MutableItem) -> Result<()> {
    loop {
        tokio::time::sleep(Duration::from_secs(600)).await;
        tokio::time::timeout(Duration::from_secs(30), dht.put_mutable(item.clone(), None))
            .await??;
    }
}
