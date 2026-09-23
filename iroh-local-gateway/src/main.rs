//! Local HTTP gateway command line interface.

use std::net::{SocketAddr, SocketAddrV4};

use anyhow::Result;
use clap::Parser;
use data_encoding::HEXLOWER_PERMISSIVE;
use iroh::endpoint::presets;
use iroh_local_gateway::{Gateway, validate_listen_addr};
use iroh_mainline_endpoint_discovery::{AddrIndex, DiscoveryConfig, Resolver};
use n0_mainline::Dht;
use udp_addr_index_proto::RENDEZVOUS_INFOHASH;

#[derive(Parser)]
#[command(about = "Serve local content at /blake3/<z32> and Pkarr redirects at /pkarr/<key>")]
struct Args {
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
    let dht = Dht::builder().port(args.dht_port).build()?;
    let config = DiscoveryConfig {
        server: args.index_server,
        public_key: args.index_list_key,
        rendezvous_hash: if args.no_rendezvous {
            None
        } else {
            Some(args.rendezvous_hash.unwrap_or(RENDEZVOUS_INFOHASH))
        },
    };
    tracing::info!("finding index servers");
    let index = AddrIndex::discover_with_config(dht.clone(), config).await?;
    let resolver = Resolver::bind(dht, index).await?;
    let endpoint = iroh::Endpoint::bind(presets::N0).await?;
    let gateway = Gateway::new(endpoint.clone(), resolver);
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    let result = gateway
        .serve(listener, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;
    endpoint.close().await;
    result
}

fn parse_hex<const N: usize>(value: &str) -> Result<[u8; N], String> {
    let invalid = || format!("expected {} hex digits", N * 2);
    HEXLOWER_PERMISSIVE
        .decode(value.as_bytes())
        .map_err(|_| invalid())?
        .try_into()
        .map_err(|_| invalid())
}
