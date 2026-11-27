//! Local HTTP gateway command line interface.

#[allow(dead_code)]
mod background;

use std::{
    net::{SocketAddr, SocketAddrV4},
    path::{Path, PathBuf},
    time::Duration,
};

use clap::Parser;
use data_encoding::HEXLOWER_PERMISSIVE;
use iroh::endpoint::presets;
use iroh_blobs_cache::{BlobCache, CacheConfig};
use iroh_link_gateway::{Gateway, validate_listen_addr};
use iroh_mainline_endpoint_discovery::{
    AddrIndex, AddrIndexBuilder, Resolver, decode_signed_packet, encode_signed_packet,
};
use n0_error::{Result, StdResultExt};
use n0_mainline::{Dht, MutableItem};
use tracing::{info, warn};

#[derive(Parser)]
#[command(about = "Serve local content at /blake3/<z32> and Pkarr redirects at /pkarr/<key>")]
struct Args {
    /// Lifecycle directory used by the per-user background launcher.
    #[arg(long)]
    state_dir: Option<PathBuf>,
    /// Loopback HTTP listen address (plaintext).
    #[arg(long, default_value = "127.0.0.1:45475")]
    listen: SocketAddr,
    /// Address index server to use instead of discovering one.
    #[arg(long, env = "IROH_ADDR_INDEX")]
    index_server: Option<SocketAddrV4>,
    /// Pkarr public key of the trusted server list, as z-base-32 or 64 hex digits;
    /// defaults to the list maintained by n0.
    #[arg(long, env = "IROH_ADDR_INDEX_LIST_KEY", value_parser = parse_list_key)]
    index_list_key: Option<[u8; 32]>,
    /// Untrusted rendezvous hash as 40 hex digits, tried when the signed list yields nothing.
    #[arg(long, env = "IROH_ADDR_INDEX_RENDEZVOUS", value_parser = parse_hex::<20>)]
    rendezvous_hash: Option<[u8; 20]>,
    /// Local Mainline UDP port; zero selects an available port.
    #[arg(long, default_value_t = 0)]
    dht_port: u16,
    /// Directory of an iroh-blobs store that keeps verified content and Pkarr records.
    ///
    /// The gateway serves them from the store, also offline.
    #[arg(long)]
    store: Option<PathBuf>,
    /// Upper bound for the store on disk, in MiB.
    ///
    /// The least recently used content goes first.
    #[arg(long, default_value_t = 1024)]
    store_max_mib: u64,
}

impl Args {
    /// Configures index discovery from the command line.
    fn index(&self, dht: Dht) -> AddrIndexBuilder {
        let mut builder = AddrIndex::builder(dht).n0_defaults();
        if let Some(server) = self.index_server {
            builder = builder.server(server);
        }
        if let Some(key) = self.index_list_key {
            builder = builder.list_key(key);
        }
        if let Some(hash) = self.rendezvous_hash {
            builder = builder.rendezvous_hash(hash);
        }
        builder
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    validate_listen_addr(args.listen)?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let runtime = args
        .state_dir
        .as_deref()
        .map(background::Runtime::acquire)
        .transpose()?;
    // Bind before discovery so an occupied port fails promptly, even offline.
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    let endpoint = iroh::Endpoint::bind(presets::N0).await?;
    let dht = Dht::builder().port(args.dht_port).build()?;
    let cache = match &args.store {
        Some(dir) => {
            let max_bytes = args.store_max_mib.saturating_mul(1024 * 1024);
            Some(BlobCache::load(dir, CacheConfig::new(max_bytes)).await?)
        }
        None => None,
    };
    if let Some(runtime) = &runtime {
        runtime.ready()?;
    }
    let result = tokio::select! {
        result = serve(args, listener, endpoint.clone(), dht, cache.clone()) => result,
        _ = shutdown(runtime.as_ref()) => Ok(()),
    };
    endpoint.close().await;
    if let Some(cache) = cache
        && let Err(error) = cache.shutdown().await
    {
        warn!(%error, "cannot shut the store down");
    }
    result
}

async fn shutdown(runtime: Option<&background::Runtime>) {
    let stop_file = async {
        match runtime {
            Some(runtime) => runtime.stopped().await,
            None => std::future::pending().await,
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = terminate => {},
        _ = stop_file => {},
    }
}

async fn serve(
    args: Args,
    listener: tokio::net::TcpListener,
    endpoint: iroh::Endpoint,
    dht: Dht,
    cache: Option<BlobCache>,
) -> Result<()> {
    let Some(cache) = cache else {
        let gateway = Gateway::new(endpoint, resolver(&args, dht).await?);
        info!(listen = %listener.local_addr()?, "gateway ready");
        return Ok(gateway.serve(listener, std::future::pending()).await?);
    };
    // The store needs no network, so it is served while discovery runs.
    let gateway = Gateway::with_cache(endpoint, None, cache);
    info!(listen = %listener.local_addr()?, "gateway ready, serving the store");
    let discovery = async {
        gateway.set_resolver(resolver(&args, dht).await?);
        info!("index servers found; looking up content and names now");
        std::future::pending::<Result<()>>().await
    };
    tokio::select! {
        result = gateway.serve(listener, std::future::pending()) => Ok(result?),
        result = discovery => result,
    }
}

/// Finds index servers and returns a resolver that uses them.
///
/// A background gateway, or one that serves a store meanwhile, keeps trying.
/// Otherwise the first failure is returned.
async fn resolver(args: &Args, dht: Dht) -> Result<Resolver> {
    let mut builder = args.index(dht.clone());
    if let Some(item) = args.state_dir.as_deref().and_then(load_index_list) {
        builder = builder.fallback_list(item);
    }
    let retry = args.state_dir.is_some() || args.store.is_some();
    info!("finding index servers");
    let index = loop {
        match builder.clone().build().await {
            Ok(index) => break index,
            Err(error) if retry => {
                warn!(%error, "index discovery failed; retrying in 20 seconds");
                tokio::time::sleep(Duration::from_secs(20)).await;
            }
            Err(error) => return Err(error.into()),
        }
    };
    if let Some(state) = &args.state_dir
        && let Some(item) = index.signed_list().await
        && let Err(error) = store_index_list(state, &item)
    {
        warn!(%error, "cannot store the signed index list");
    }
    Ok(Resolver::new(dht, index))
}

/// Signed index list from the last successful discovery, used when the
/// Pkarr record does not resolve.
///
/// Stored as a Pkarr signed packet and verified again when read.
const INDEX_LIST_FILE: &str = "index-list.pkarr";

fn load_index_list(state: &Path) -> Option<MutableItem> {
    let bytes = std::fs::read(state.join(INDEX_LIST_FILE)).ok()?;
    let item = decode_signed_packet(&bytes);
    if item.is_none() {
        warn!("ignoring stored index list that does not verify");
    }
    item
}

fn store_index_list(state: &Path, item: &MutableItem) -> Result<()> {
    // Write beside the file and rename, so a crash never leaves half a list.
    let path = state.join(INDEX_LIST_FILE);
    let temporary = path.with_extension("tmp");
    let bytes = encode_signed_packet(item).std_context("index list is not a Pkarr packet")?;
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

fn parse_hex<const N: usize>(value: &str) -> Result<[u8; N], String> {
    let invalid = || format!("expected {} hex digits", N * 2);
    HEXLOWER_PERMISSIVE
        .decode(value.as_bytes())
        .map_err(|_| invalid())?
        .try_into()
        .map_err(|_| invalid())
}

fn parse_list_key(value: &str) -> Result<[u8; 32], String> {
    if value.len() == 64 {
        return parse_hex(value);
    }
    let invalid = || "expected a z-base-32 Pkarr public key or 64 hex digits".to_owned();
    let bytes: [u8; 32] = z32::decode(value.as_bytes())
        .map_err(|_| invalid())?
        .try_into()
        .map_err(|_| invalid())?;
    if z32::encode(&bytes) != value {
        return Err(invalid());
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_the_signed_list_and_preserves_explicit_sources() {
        // Environment-backed options are covered by Clap; test discovery defaults
        // with those bindings removed so the caller's environment cannot affect it.
        use clap::{CommandFactory, FromArgMatches};
        let parse = |args: Vec<&str>| {
            let cmd = Args::command()
                .mut_arg("index_server", |arg| arg.env(None::<&str>))
                .mut_arg("index_list_key", |arg| arg.env(None::<&str>))
                .mut_arg("rendezvous_hash", |arg| arg.env(None::<&str>));
            cmd.try_get_matches_from(args)
                .and_then(|matches| Args::from_arg_matches(&matches))
        };
        let key = z32::encode(&[42; 32]);
        let hash = "01".repeat(20);
        let default = parse(vec!["gateway"]).unwrap();
        assert!(default.index_list_key.is_none());
        assert!(default.rendezvous_hash.is_none());
        let curated = parse(vec!["gateway", "--index-list-key", &key]).unwrap();
        assert_eq!(curated.index_list_key, Some([42; 32]));
        let custom = parse(vec!["gateway", "--rendezvous-hash", &hash]).unwrap();
        assert!(custom.index_list_key.is_none());
        assert_eq!(custom.rendezvous_hash, Some([1; 20]));
        let both = parse(vec![
            "gateway",
            "--index-list-key",
            &key,
            "--rendezvous-hash",
            &hash,
        ])
        .unwrap();
        assert_eq!(both.index_list_key, Some([42; 32]));
        assert_eq!(both.rendezvous_hash, Some([1; 20]));
        let direct = parse(vec!["gateway", "--index-server", "127.0.0.1:60125"]).unwrap();
        assert_eq!(
            direct.index_server,
            Some("127.0.0.1:60125".parse().unwrap())
        );
        assert!(direct.rendezvous_hash.is_none());
    }

    #[test]
    fn stored_index_list_roundtrips() {
        let state = tempfile::tempdir().unwrap();
        assert!(load_index_list(state.path()).is_none());
        let key = n0_mainline::SigningKey::from_bytes(&[42; 32]);
        let item = MutableItem::new(&key, b"list", 7, None);
        store_index_list(state.path(), &item).unwrap();
        assert_eq!(load_index_list(state.path()), Some(item));
        let path = state.path().join(INDEX_LIST_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        std::fs::write(&path, bytes).unwrap();
        assert!(load_index_list(state.path()).is_none());
    }

    #[test]
    fn accepts_pkarr_and_hex_list_keys() {
        let key = [42; 32];
        assert_eq!(parse_list_key(&z32::encode(&key)).unwrap(), key);
        assert_eq!(parse_list_key(&"2a".repeat(32)).unwrap(), key);
        assert!(parse_list_key("invalid").is_err());
        assert!(parse_list_key(&"0".repeat(63)).is_err());
    }
}
