//! Publish a blob, name it with Pkarr, then resolve, discover and download it.
//! No HTTP gateway is involved. Requires public Mainline access and a reachable
//! address index (discovered by default, or supplied with --index-server).

use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    net::SocketAddrV4,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use clap::Parser;
use iroh::{Endpoint, endpoint::presets, protocol::Router};
use iroh_blobs::{
    BlobsProtocol, Hash, HashAndFormat, api::downloader::ContentDiscovery, store::mem::MemStore,
};
use iroh_mainline_endpoint_discovery::{
    AddrIndex, BLAKE3_DOMAIN, PKARR_DOMAIN, PkarrPublisher, Publisher, Resolver,
    infohash_from_blake3, pkarr_name,
};
use n0_error::{Result, StdResultExt};
use n0_future::{StreamExt, stream};
use n0_mainline::{Dht, Id, SigningKey};
use simple_dns::{Packet, rdata::RData};
use tokio::io::AsyncReadExt;

const BUDGET: Duration = Duration::from_secs(180);

#[derive(Parser)]
#[command(about = "Publish content and a Pkarr name, then resolve, discover and download it")]
struct Args {
    /// Text to publish as a blob; change it while reusing the key to update the name.
    #[arg(long, default_value = "hello from a Pkarr-named iroh blob\n")]
    data: String,
    /// Load or create a 32-byte secret-key file; otherwise generate a temporary keypair.
    #[arg(long)]
    key_file: Option<PathBuf>,
    /// Use this address index for both sides instead of discovering index servers.
    #[arg(long, env = "IROH_ADDR_INDEX")]
    index_server: Option<SocketAddrV4>,
    /// Exit after downloading; otherwise keep serving and republishing until Ctrl-C.
    #[arg(long)]
    once: bool,
}

fn load_key(path: Option<&Path>) -> std::io::Result<SigningKey> {
    let Some(path) = path else {
        return Ok(SigningKey::from_bytes(&rand::random()));
    };
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(mut file) => {
            let secret: [u8; 32] = rand::random();
            file.write_all(&secret)?;
            file.sync_all()?;
            Ok(SigningKey::from_bytes(&secret))
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let mut secret = Vec::new();
            File::open(path)?.take(33).read_to_end(&mut secret)?;
            let secret: [u8; 32] = secret.try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "key file must contain exactly 32 bytes",
                )
            })?;
            Ok(SigningKey::from_bytes(&secret))
        }
        Err(error) => Err(error),
    }
}

/// Resolve the newest signed packet and read its apex HTTPS target.
async fn resolve_target(dht: &Dht, public: &[u8; 32]) -> Result<String> {
    // n0-mainline verifies the BEP44 signature against the requested public key.
    let item = dht
        .get_mutable_most_recent(public, None)
        .await?
        .std_context("no Pkarr record found")?;
    let packet = Packet::parse(item.value()).std_context("decode Pkarr DNS packet")?;
    let zone = pkarr_name(public);
    for record in &packet.answers {
        if record.name.to_string().trim_end_matches('.') != zone {
            continue;
        }
        if let RData::HTTPS(https) = &record.rdata {
            return Ok(https.0.target.to_string());
        }
    }
    n0_error::bail_any!("Pkarr packet has no apex HTTPS record")
}

fn content_hash(target: &str) -> Result<Hash> {
    let target = target.trim_end_matches('.').to_ascii_lowercase();
    let label = target
        .strip_suffix(&format!(".{BLAKE3_DOMAIN}"))
        .std_context("Pkarr target is not a blake3.net content name")?;
    let bytes = z32::decode(label.as_bytes()).std_context("invalid z-base-32 content hash")?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .ok()
        .std_context("content hash must be 32 bytes")?;
    Ok(Hash::from_bytes(bytes))
}

fn infohash(hash: Hash) -> Id {
    Id::from(infohash_from_blake3(&blake3::Hash::from_bytes(
        *hash.as_bytes(),
    )))
}

async fn address_index(dht: &Dht, server: Option<SocketAddrV4>) -> Result<AddrIndex> {
    Ok(match server {
        Some(server) => AddrIndex::udp(dht.clone(), server).await?,
        None => AddrIndex::discover(dht.clone()).await?,
    })
}

#[derive(Debug)]
struct MainlineProviders(Resolver);

impl ContentDiscovery for MainlineProviders {
    fn find_providers(&self, hash: HashAndFormat) -> stream::Boxed<iroh::EndpointId> {
        println!("  Mainline infohash: {}", infohash(hash.hash));
        self.0
            .resolve_continuously(infohash(hash.hash))
            .map(|id| {
                println!("  Provider: {id}");
                id
            })
            .boxed()
    }
}

/// The receiving side knows the name, not the hash or provider endpoint.
async fn receive(public: &[u8; 32], server: Option<SocketAddrV4>) -> Result<Vec<u8>> {
    let dht = Dht::client().std_context("start independent resolving DHT client")?;
    println!("\n[4/5] Resolve the name from an independent DHT client");
    let target = resolve_target(&dht, public).await?;
    let hash = content_hash(&target)?;
    println!("  Verified signed record. Target:");
    println!("  {target}");
    println!("\n[5/5] Discover a provider and download the content");
    let index = address_index(&dht, server).await?;
    let resolver = Resolver::new(dht, index);
    let store = MemStore::new();
    let endpoint = Endpoint::bind(presets::N0).await?;
    let result = async {
        store
            .downloader(&endpoint)
            .download(hash, MainlineProviders(resolver))
            .await?;
        let mut data = Vec::new();
        store.blobs().reader(hash).read_to_end(&mut data).await?;
        n0_error::Ok(data)
    }
    .await;
    endpoint.close().await;
    store.shutdown().await?;
    result
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();
    let key = load_key(args.key_file.as_deref()).std_context("load or create Pkarr key file")?;
    let public = key.verifying_key().to_bytes();
    let name = pkarr_name(&public);
    println!("[1/5] Prepare content and identity");
    if args.key_file.is_none() {
        println!("  Generated a keypair. Use --key-file to keep this name.");
    }
    println!("  Name: https://{name}.{PKARR_DOMAIN}/");
    let store = MemStore::new();
    let tag = store
        .blobs()
        .add_bytes(args.data.as_bytes().to_vec())
        .await?;
    let hash = tag.hash;
    println!(
        "  Blob: https://{}.{BLAKE3_DOMAIN}/",
        z32::encode(hash.as_bytes())
    );
    let endpoint = Endpoint::bind(presets::N0).await?;
    let router = Router::builder(endpoint.clone())
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&store, None))
        .spawn();
    println!("  Serving {}", endpoint.id());
    let workflow = async {
        println!("\n[2/5] Publish the endpoint mapping and announce the blob");
        let dht = Dht::client().std_context("start publishing DHT client")?;
        let index = address_index(&dht, args.index_server).await?;
        let publisher = Publisher::new(endpoint.secret_key().clone(), dht.clone(), index);
        publisher.add_infohash(infohash(hash));
        tokio::time::timeout(BUDGET, publisher.wait_published())
            .await
            .std_context("content announcement timed out")?;
        println!("  Endpoint mapping and provider announcement published.");
        let pkarr = PkarrPublisher::new(dht);
        pkarr.set_blake3(&key, hash.as_bytes())?;
        println!("\n[3/5] Publish the signed Pkarr name");
        tokio::time::timeout(BUDGET, pkarr.publish_all())
            .await
            .std_context("Pkarr publication timed out")??;
        println!("  Name now points to the blob hash.");
        let start = Instant::now();
        let received = tokio::time::timeout(BUDGET, receive(&public, args.index_server))
            .await
            .std_context("name resolution, discovery or download timed out")??;
        // This is a demo assertion only: the receiving side never sees the source bytes.
        n0_error::ensure_any!(
            received == args.data.as_bytes(),
            "downloaded content differs"
        );
        println!(
            "\nDownloaded and BLAKE3-verified {} bytes in {:.2}s.",
            received.len(),
            start.elapsed().as_secs_f64()
        );
        println!("Content:");
        for line in String::from_utf8_lossy(&received).lines() {
            println!("  {line}");
        }
        if args.once {
            return Ok(());
        }
        println!("Keeping the content and name available. Press Ctrl-C to stop.");
        std::future::pending::<Result<()>>().await
    };
    let result = tokio::select! {
        result = workflow => result,
        result = tokio::signal::ctrl_c() => result.anyerr(),
    };
    router.shutdown().await.anyerr()?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publish_then_resolve() -> Result<()> {
        let network = n0_mainline::Testnet::new(3).await.anyerr()?;
        let node = || Dht::builder().bootstrap(&network.bootstrap).port(0).build();
        let key = load_key(None).anyerr()?;
        let publisher = PkarrPublisher::new(node().anyerr()?);
        let hash = blake3::hash(b"named content");
        publisher.set_blake3(&key, hash.as_bytes())?;
        publisher.publish_all().await?;
        let reader = node().anyerr()?;
        let target = tokio::time::timeout(
            Duration::from_secs(10),
            resolve_target(&reader, &key.verifying_key().to_bytes()),
        )
        .await
        .anyerr()??;
        assert_eq!(content_hash(&target)?, Hash::from_bytes(*hash.as_bytes()));
        Ok(())
    }

    #[test]
    fn rejects_non_content_names() {
        assert!(content_hash("example.com").is_err());
        assert!(content_hash("short.blake3.net").is_err());
        assert!(content_hash("invalid!.blake3.net").is_err());
        assert!(content_hash("foo.bar.blake3.net").is_err());
    }

    #[test]
    fn reuses_a_key_file() -> std::io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("key");
        let key = load_key(Some(&path))?;
        assert_eq!(key.verifying_key(), load_key(Some(&path))?.verifying_key());
        // Without a file each run is a new identity.
        assert_ne!(
            load_key(None)?.verifying_key(),
            load_key(None)?.verifying_key()
        );
        Ok(())
    }
}
