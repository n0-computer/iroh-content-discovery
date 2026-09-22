//! Provide a file or directory through iroh-blobs and announce every blob
//! and the collection on Mainline.

use std::{
    net::SocketAddrV4,
    path::{Path, PathBuf},
};

use clap::Parser;
use iroh::{endpoint::presets, protocol::Router};
use iroh_blobs::{
    BlobFormat, BlobsProtocol, Hash,
    api::blobs::{AddPathOptions, ImportMode},
    format::collection::Collection,
    store::fs::FsStore,
};
use iroh_mainline_endpoint_discovery::{Directory, Publisher, infohash_from_blake3};
use n0_error::{Result, StackResultExt, StdResultExt, bail_any};
use n0_mainline::{Dht, Id};

/// Provide files and announce their hashes on Mainline.
#[derive(Debug, Parser)]
struct Cli {
    /// File or directory to provide.
    path: PathBuf,
    /// Address-index replica to use instead of discovering one.
    #[arg(long, env = "IROH_ADDR_INDEX")]
    addr_index: Option<SocketAddrV4>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();

    let root = std::fs::canonicalize(&cli.path).std_context("invalid path")?;
    let files = collect_files(&root)?;
    if files.is_empty() {
        bail_any!("no files found at {}", root.display());
    }

    // Files are referenced in place rather than copied into the store, which
    // only holds the outboards and files small enough to be inlined. The
    // files must not change while they are provided.
    let store_dir = tempfile::tempdir().std_context("store directory")?;
    let store = FsStore::load(store_dir.path()).await?;
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
    let collection_hash = collection_tag.hash();

    let endpoint = iroh::Endpoint::bind(presets::N0)
        .await
        .context("provider endpoint")?;
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
    let publisher = Publisher::new(endpoint.secret_key().clone(), dht, index);
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
    println!("http://127.0.0.1:8080/tree/{collection}");

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
