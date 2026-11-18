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
use iroh_link_gateway::{Gateway, validate_listen_addr};
use iroh_mainline_endpoint_discovery::{
    AddrIndex, AddrIndexBuilder, Resolver, decode_signed_packet, encode_signed_packet,
};
use n0_error::{Result, StdResultExt};
use n0_mainline::{Dht, MutableItem};
use tracing::{debug, info, warn};

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
    /// HTTP Pkarr relay asked when Mainline has no packet for a key, such as
    /// https://relay.pkarr.org; may repeat. The relay learns which keys are
    /// resolved. Packets from relays are verified like those from Mainline.
    #[arg(long = "pkarr-relay", env = "IROH_PKARR_RELAYS", value_delimiter = ',')]
    pkarr_relays: Vec<String>,
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
    if let Some(runtime) = &runtime {
        runtime.ready()?;
    }
    let result = tokio::select! {
        result = serve(args, listener, endpoint.clone()) => result,
        _ = shutdown(runtime.as_ref()) => Ok(()),
    };
    endpoint.close().await;
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
) -> Result<()> {
    let mut dht = start_mainline(args.dht_port).await;
    let index = discover_index(&args, &dht, args.state_dir.is_some()).await?;
    let gateway = Gateway::new(endpoint, Resolver::new(dht.clone(), index));
    gateway.set_pkarr_relays(args.pkarr_relays.clone())?;
    info!(listen = %listener.local_addr()?, "gateway ready");
    // A node can lose all its peers, for example after a sleep or a network
    // change, and not find new ones. Then start over with a new one.
    let watchdog = async {
        loop {
            watch_mainline(&dht).await;
            warn!("Mainline node has no peers; starting a new one");
            dht = start_mainline(args.dht_port).await;
            let index = discover_index(&args, &dht, true)
                .await
                .expect("retries until it succeeds");
            gateway.set_resolver(Resolver::new(dht.clone(), index));
            info!("gateway uses the new Mainline node");
        }
    };
    tokio::select! {
        result = gateway.serve(listener, std::future::pending()) => Ok(result?),
        () = watchdog => unreachable!("the watchdog runs until the gateway stops"),
    }
}

/// Finds the address index servers, retrying until it succeeds if `retry`.
async fn discover_index(args: &Args, dht: &Dht, retry: bool) -> Result<AddrIndex> {
    let mut builder = args.index(dht.clone());
    if let Some(item) = args.state_dir.as_deref().and_then(load_index_list) {
        builder = builder.fallback_list(item);
    }
    info!("finding index servers");
    let index = loop {
        match builder.clone().build().await {
            Ok(index) => break index,
            Err(error) if retry => {
                warn!(%error, "index discovery failed; retrying in 20 seconds");
                tokio::time::sleep(RETRY_DELAY).await;
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
    Ok(index)
}

/// Pause between attempts to start a Mainline node or find index servers.
const RETRY_DELAY: Duration = Duration::from_secs(20);
/// How often [`watch_mainline`] checks the routing table.
const WATCH_INTERVAL: Duration = Duration::from_secs(30);
/// Empty checks in a row before the Mainline node is replaced.
const EMPTY_TABLE_CHECKS: u32 = 3;
/// The default bootstrap nodes of n0-mainline, which it does not export.
const BOOTSTRAP_NODES: [&str; 4] = [
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "dht.libtorrent.org:25401",
    "relay.pkarr.org:6881",
];

/// Starts a Mainline node with bootstrap nodes given by address.
///
/// n0-mainline resolves bootstrap host names once, when the node starts. If
/// that fails, as it does offline, the node never finds a peer. So resolve
/// them here, and wait until at least one resolves. A port in use, as it is
/// while the node this one replaces is still running, falls back to any port.
async fn start_mainline(port: u16) -> Dht {
    loop {
        let mut addresses = Vec::new();
        for host in BOOTSTRAP_NODES {
            match tokio::net::lookup_host(host).await {
                // n0-mainline speaks IPv4 only.
                Ok(found) => addresses.extend(
                    found
                        .filter(|address| address.is_ipv4())
                        .map(|address| address.to_string()),
                ),
                Err(error) => warn!(host, %error, "cannot resolve Mainline bootstrap node"),
            }
        }
        if addresses.is_empty() {
            warn!("no Mainline bootstrap node resolves; the network may be down, retrying");
            tokio::time::sleep(RETRY_DELAY).await;
            continue;
        }
        debug!(?addresses, "starting Mainline node");
        let build = |port| Dht::builder().port(port).bootstrap(&addresses).build();
        match build(port).or_else(|error| match port {
            0 => Err(error),
            _ => {
                warn!(port, %error, "cannot bind the Mainline port; using any port");
                build(0)
            }
        }) {
            Ok(dht) => return dht,
            Err(error) => {
                warn!(%error, "cannot start Mainline node; retrying");
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
}

/// Returns once the Mainline node has had no peers for [`EMPTY_TABLE_CHECKS`] checks in a row.
///
/// The node refills its routing table from the bootstrap nodes when the
/// network comes back. This catches the cases where it does not.
async fn watch_mainline(dht: &Dht) {
    let mut empty_checks = 0;
    loop {
        tokio::time::sleep(WATCH_INTERVAL).await;
        let peers = match dht.info().await {
            Ok(info) => info.routing_table_size(),
            Err(_) => return,
        };
        if peers > 0 {
            empty_checks = 0;
            continue;
        }
        empty_checks += 1;
        warn!(checks = empty_checks, "Mainline routing table is empty");
        // Ask the bootstrap nodes again before giving up on this node.
        if empty_checks >= EMPTY_TABLE_CHECKS && !dht.bootstrapped().await.unwrap_or(false) {
            return;
        }
    }
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
