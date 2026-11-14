//! Publishes a directory as a website on the public Mainline DHT.
//!
//! Imports the directory as an iroh-blobs collection, provides it over iroh,
//! announces it on Mainline, and points a Pkarr name at it. A collection with a
//! top-level `index.html` opens as a page in the gateway.

use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use clap::Parser;
use iroh::{Endpoint, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, format::collection::Collection, store::mem::MemStore};
use iroh_mainline_endpoint_discovery::{
    AddrIndex, BLAKE3_DOMAIN, PKARR_DOMAIN, PkarrPublisher, Publisher, infohash_from_blake3,
    pkarr_name,
};
use n0_error::{Result, StackResultExt, StdResultExt, anyerr};
use n0_mainline::{Dht, SigningKey};

#[derive(Parser)]
#[command(about = "Publish a directory as a blake3.net website with a pkarr.net name")]
struct Args {
    /// Directory to publish; its `index.html` becomes the start page.
    dir: PathBuf,
    /// Secret-key file to read or create, to keep the same pkarr.net name.
    ///
    /// Without this option, a temporary name is generated for this run.
    #[arg(long)]
    key_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let key = load_key(args.key_file.as_deref())?;
    let root = args
        .dir
        .canonicalize()
        .with_std_context(|_| format!("cannot read {}", args.dir.display()))?;
    let store = MemStore::new();
    let mut files = Vec::new();
    collect_files(&root, &root, &mut files)?;
    let mut collection = Collection::default();
    for (name, path) in files {
        let tag = store.blobs().add_path(&path).await?;
        println!("  {name} ({})", tag.hash);
        collection.push(name, tag.hash);
    }
    let tag = collection.store(&store).await?;
    let hash = tag.hash();
    let encoded = z32::encode(hash.as_bytes());

    println!("Discovering index servers on the public Mainline DHT...");
    let dht = Dht::builder()
        .port(0)
        .build()
        .std_context("failed to bind the Mainline socket")?;
    let index = AddrIndex::builder(dht.clone())
        .n0_defaults()
        .build()
        .await
        .context("index server discovery failed")?;
    let endpoint = Endpoint::bind(presets::N0).await?;
    let router = Router::builder(endpoint.clone())
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&store, None))
        .spawn();
    let publisher = Publisher::new(endpoint.secret_key().clone(), dht.clone(), index);
    publisher
        .add_infohash(infohash_from_blake3(&blake3::Hash::from_bytes(*hash.as_bytes())).into());
    let pkarr = PkarrPublisher::new(dht);
    pkarr.set_blake3(&key, hash.as_bytes())?;
    let public_key = pkarr_name(&key.verifying_key().to_bytes());

    let publish = async {
        tokio::time::timeout(Duration::from_secs(120), publisher.wait_published())
            .await
            .std_context("content publication timed out")?;
        pkarr.publish_all().await?;
        println!("\nEndpoint: {}", endpoint.id());
        println!("Content:  https://{encoded}.{BLAKE3_DOMAIN}/");
        println!("Name:     https://{public_key}.{PKARR_DOMAIN}/");
        println!("\nLeave this running. Press Ctrl-C to stop.");
        // Both publishers keep republishing in their own tasks.
        std::future::pending::<Result<()>>().await
    };
    let result = tokio::select! {
        result = publish => result,
        result = tokio::signal::ctrl_c() => { result.anyerr()?; Ok(()) },
    };
    router.shutdown().await.anyerr()?;
    drop(tag);
    result
}

/// Lists the files below `dir` with `/`-separated names relative to `root`.
fn collect_files(root: &Path, dir: &Path, files: &mut Vec<(String, PathBuf)>) -> Result<()> {
    let mut entries = std::fs::read_dir(dir)
        .with_std_context(|_| format!("cannot read {}", dir.display()))?
        .collect::<std::io::Result<Vec<_>>>()
        .anyerr()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_files(root, &path, files)?;
        } else if path.is_file() {
            let name = path
                .strip_prefix(root)
                .anyerr()?
                .components()
                .map(|part| part.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            files.push((name, path));
        }
    }
    Ok(())
}

/// Reads a 32-byte secret key from `path`, creating it if missing.
fn load_key(path: Option<&Path>) -> Result<SigningKey> {
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
            file.write_all(&secret).anyerr()?;
            file.sync_all().anyerr()?;
            Ok(SigningKey::from_bytes(&secret))
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let mut secret = Vec::new();
            File::open(path)
                .anyerr()?
                .take(33)
                .read_to_end(&mut secret)
                .anyerr()?;
            let secret: [u8; 32] = secret
                .try_into()
                .map_err(|_| anyerr!("key file must contain exactly 32 bytes"))?;
            Ok(SigningKey::from_bytes(&secret))
        }
        Err(error) => Err(anyerr!(error)),
    }
    .with_context(|_| format!("key file {}", path.display()))
}
