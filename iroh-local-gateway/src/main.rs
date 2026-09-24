//! Local HTTP gateway command line interface.

#[allow(dead_code)]
mod background;

use std::{
    net::{SocketAddr, SocketAddrV4},
    path::PathBuf,
    time::Duration,
};

use anyhow::Result;
use clap::Parser;
use data_encoding::HEXLOWER_PERMISSIVE;
use iroh::endpoint::presets;
use iroh_local_gateway::{Gateway, validate_listen_addr};
use iroh_mainline_endpoint_discovery::{AddrIndex, DiscoveryConfig, Resolver};
use n0_mainline::Dht;
use tracing::info;
use udp_addr_index_proto::RENDEZVOUS_INFOHASH;

#[derive(Parser)]
#[command(about = "Serve local content at /blake3/<z32> and Pkarr redirects at /pkarr/<key>")]
struct Args {
    /// Lifecycle directory used by the per-user background launcher.
    #[arg(long)]
    state_dir: Option<PathBuf>,
    /// Loopback HTTP listen address (plaintext).
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
    /// Address index server to use instead of discovering one.
    #[arg(long, env = "IROH_ADDR_INDEX")]
    index_server: Option<SocketAddrV4>,
    /// Public key of the trusted server list, as 64 hex digits.
    #[arg(long, env = "IROH_ADDR_INDEX_LIST_KEY", value_parser = parse_hex::<32>)]
    index_list_key: Option<[u8; 32]>,
    /// Rendezvous infohash for server discovery, as 40 hex digits (default: protocol hash).
    #[arg(long, env = "IROH_ADDR_INDEX_RENDEZVOUS", value_parser = parse_hex::<20>, conflicts_with = "no_rendezvous")]
    rendezvous_hash: Option<[u8; 20]>,
    /// Disable the rendezvous fallback; use only an explicit server or the trusted list.
    #[arg(long)]
    no_rendezvous: bool,
    /// Local Mainline UDP port; zero selects an available port.
    #[arg(long, default_value_t = 0)]
    dht_port: u16,
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
    if let Some(runtime) = &runtime {
        runtime.ready()?;
    }
    let result = tokio::select! {
        result = serve(args, listener, endpoint.clone(), dht) => result,
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
    dht: Dht,
) -> Result<()> {
    let config = DiscoveryConfig {
        server: args.index_server,
        public_key: args.index_list_key,
        rendezvous_hash: if args.no_rendezvous {
            None
        } else {
            Some(args.rendezvous_hash.unwrap_or(RENDEZVOUS_INFOHASH))
        },
    };
    info!("finding index servers");
    let index = loop {
        match AddrIndex::discover_with_config(dht.clone(), config.clone()).await {
            Ok(index) => break index,
            Err(error) if args.state_dir.is_some() => {
                tracing::warn!(%error, "index discovery failed; retrying in 20 seconds");
                tokio::time::sleep(Duration::from_secs(20)).await;
            }
            Err(error) => return Err(error.into()),
        }
    };
    let resolver = Resolver::new(dht, index);
    let gateway = Gateway::new(endpoint, resolver);
    info!(listen = %listener.local_addr()?, "gateway ready");
    gateway.serve(listener, std::future::pending()).await
}

fn parse_hex<const N: usize>(value: &str) -> Result<[u8; N], String> {
    let invalid = || format!("expected {} hex digits", N * 2);
    HEXLOWER_PERMISSIVE
        .decode(value.as_bytes())
        .map_err(|_| invalid())?
        .try_into()
        .map_err(|_| invalid())
}
