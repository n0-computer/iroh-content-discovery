//! Provide a file or directory through iroh-blobs and announce every blob
//! and the collection on Mainline.

use std::{
    net::SocketAddrV4,
    path::{Path, PathBuf},
};

use clap::Parser;
use data_encoding::{HEXLOWER, HEXLOWER_PERMISSIVE};
use iroh::{Endpoint, endpoint::presets, protocol::Router};
use iroh_blobs::{
    BlobFormat, BlobsProtocol, Hash,
    api::blobs::{AddPathOptions, ImportMode},
    format::collection::Collection,
    store::fs::FsStore,
};
use iroh_mainline_endpoint_discovery::{
    Directory, PkarrPublisher, Publisher, infohash_from_blake3, pkarr_name,
};
use n0_error::{Result, StackResultExt, StdResultExt, bail_any};
use n0_mainline::{Dht, Id, SigningKey};

/// Provide files and announce their hashes on Mainline.
#[derive(Debug, Parser)]
struct Cli {
    /// File or directory to provide.
    path: PathBuf,
    /// Address-index replica to use instead of discovering one.
    #[arg(long, env = "IROH_ADDR_INDEX")]
    addr_index: Option<SocketAddrV4>,
    /// Do not publish a Pkarr name for the collection.
    #[arg(long)]
    no_pkarr: bool,
}

/// Secret key for the Pkarr name, as 64 hex digits.
const PKARR_SECRET: &str = "PKARR_SECRET";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    // Read or make the Pkarr key first, so a generated secret is the first
    // thing printed and cannot scroll away behind the hashes.
    let pkarr_key = (!cli.no_pkarr).then(pkarr_key).transpose()?;

    let root = std::fs::canonicalize(&cli.path).std_context("invalid path")?;
    let files = collect_files(&root)?;
    if files.is_empty() {
        bail_any!("no files found at {}", root.display());
    }

    let store_dir = tempfile::tempdir().std_context("store directory")?;
    let (store, entries, collection_hash) = import_path(store_dir.path(), files).await?;

    let endpoint = Endpoint::bind(presets::N0).await?;
    let router = Router::builder(endpoint.clone())
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&store, None))
        .spawn();

    let dht = Dht::client().std_context("DHT")?;
    if !dht.bootstrapped().await? {
        bail_any!("DHT bootstrap failed");
    }
    let index = match cli.addr_index {
        Some(replica) => Directory::udp(dht.clone(), replica).await?,
        None => Directory::discover(dht.clone()).await?,
    };
    let publisher = Publisher::new(endpoint.secret_key().clone(), dht.clone(), index);
    for (_, hash) in &entries {
        publisher.add_infohash(infohash(hash));
    }
    publisher.add_infohash(infohash(&collection_hash));

    // Hashes are printed in z-base-32, the encoding used by the gateway.
    println!("provider {}", endpoint.id());
    for (name, hash) in &entries {
        println!("{}  {name}", z32::encode(hash.as_bytes()));
    }
    let collection = z32::encode(collection_hash.as_bytes());
    println!("{collection}  (collection)");
    // With the browser extension, the link URLs reach the local gateway.
    println!("https://{collection}.blake3.link/");
    println!("http://{collection}.blake3.localhost:8080/");

    // The name outlives this run; the hash it points at does not. The
    // publisher keeps republishing in its own task until it is dropped.
    let _pkarr = pkarr_key
        .map(|key| {
            let publisher = PkarrPublisher::new(dht.clone());
            publisher.set_blake3(&key, collection_hash.as_bytes())?;
            let name = pkarr_name(&key.verifying_key().to_bytes());
            println!("https://{name}.pkarr.link/");
            println!("http://{name}.pkarr.localhost:8080/");
            n0_error::Ok(publisher)
        })
        .transpose()?;

    let announced = async {
        publisher.wait_published().await;
        tracing::info!("published, press Ctrl-C to stop");
        std::future::pending::<()>().await;
    };
    let result = tokio::select! {
        result = publisher.run() => result,
        _ = announced => unreachable!(),
        _ = tokio::signal::ctrl_c() => Ok(()),
    };
    // Router shutdown also shuts down the store before the directory is removed.
    router.shutdown().await.anyerr()?;
    drop(store_dir);
    result
}

/// Adds every file as a blob plus a collection, and returns their hashes.
///
/// Files are referenced in place rather than copied into the store, which
/// only holds the outboards and files small enough to be inlined. The files
/// must not change while they are provided.
async fn import_path(
    store_dir: &Path,
    files: Vec<(String, PathBuf)>,
) -> Result<(FsStore, Vec<(String, Hash)>, Hash)> {
    let store = FsStore::load(store_dir).await?;
    let mut entries = Vec::with_capacity(files.len());
    for (name, path) in files {
        let tag = store
            .blobs()
            .add_path_with_opts(AddPathOptions {
                path,
                mode: ImportMode::TryReference,
                format: BlobFormat::Raw,
            })
            .with_tag()
            .await?;
        entries.push((name, tag.hash));
    }
    let collection: Collection = entries.iter().cloned().collect();
    let collection_tag = collection.store(&store).await?;
    store
        .tags()
        .create(collection_tag.hash_and_format())
        .await?;
    Ok((store, entries, collection_tag.hash()))
}

/// Reads the Pkarr signing key from the environment, or makes a new one.
///
/// A generated key is printed so the same name can be reused on the next run.
fn pkarr_key() -> Result<SigningKey> {
    let Some(hex) = std::env::var_os(PKARR_SECRET) else {
        let secret: [u8; 32] = rand::random();
        let printable = HEXLOWER.encode(&secret);
        println!("{PKARR_SECRET}={printable}  (generated, export it to reuse this name)");
        return Ok(SigningKey::from_bytes(&secret));
    };
    let hex = hex.to_str().std_context("secret must be hex digits")?;
    let secret: [u8; 32] = HEXLOWER_PERMISSIVE
        .decode(hex.as_bytes())
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .std_context("secret must contain 64 hex digits")?;
    Ok(SigningKey::from_bytes(&secret))
}

fn infohash(hash: &Hash) -> Id {
    Id::from(infohash_from_blake3(&blake3::Hash::from_bytes(
        *hash.as_bytes(),
    )))
}

/// Returns all files below `root` as `(name, path)` pairs, sorted by name.
///
/// Names are relative to `root` and use `/` as separator. A file `root`
/// yields a single entry named after the file.
fn collect_files(root: &Path) -> Result<Vec<(String, PathBuf)>> {
    if root.is_file() {
        let name = root
            .file_name()
            .std_context("path has no file name")?
            .to_string_lossy()
            .into_owned();
        return Ok(vec![(name, root.to_owned())]);
    }
    let mut files = Vec::new();
    let mut dirs = vec![root.to_owned()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).std_context("read directory")? {
            let path = entry.std_context("read directory entry")?.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.is_file() {
                let name = path
                    .strip_prefix(root)
                    .expect("walked path is below root")
                    .components()
                    .map(|part| part.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                files.push((name, path));
            }
        }
    }
    files.sort();
    Ok(files)
}
