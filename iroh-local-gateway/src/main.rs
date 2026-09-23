//! Local HTTP gateway command line interface.

use std::net::{SocketAddr, SocketAddrV4};

use anyhow::Result;
use clap::Parser;
use data_encoding::HEXLOWER_PERMISSIVE;
use iroh::endpoint::presets;
use iroh_local_gateway::{Gateway, validate_listen_addr};
use iroh_mainline_endpoint_discovery::{Directory, DiscoveryConfig, Resolver};
use n0_mainline::Dht;
use udp_address_records_proto::RENDEZVOUS_INFOHASH;

#[derive(Parser)]
#[command(about = "Serve local content at /blake3/<z32> and Pkarr redirects at /pkarr/<key>")]
struct Args {
    /// Loopback HTTP listen address (plaintext).
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
    /// Explicit tracker IPv4:port; bypasses tracker discovery.
    #[arg(long, env = "IROH_ADDR_INDEX")]
    tracker: Option<SocketAddrV4>,
    /// Trusted BEP44 tracker-list public key, as 64 hex digits.
    #[arg(long, env = "IROH_TRACKER_PUBKEY", value_parser = parse_hex::<32>)]
    tracker_pubkey: Option<[u8; 32]>,
    /// Tracker rendezvous infohash, as 40 hex digits (default: protocol hash).
    #[arg(long, env = "IROH_TRACKER_INFOHASH", value_parser = parse_hex::<20>, conflicts_with = "no_rendezvous")]
    rendezvous_hash: Option<[u8; 20]>,
    /// Disable rendezvous fallback; use only the explicit tracker or trusted key.
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
        tracker: args.tracker,
        public_key: args.tracker_pubkey,
        rendezvous_hash: if args.no_rendezvous {
            None
        } else {
            Some(args.rendezvous_hash.unwrap_or(RENDEZVOUS_INFOHASH))
        },
    };
    tracing::info!("finding trackers");
    let directory = Directory::discover_with_config(dht.clone(), config).await?;
    let resolver = Resolver::bind(dht, directory).await?;
    let endpoint = iroh::Endpoint::bind(presets::N0).await?;
    let gateway = Gateway::new(endpoint.clone(), resolver);
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    println!("http://{}/blake3/<z32>", listener.local_addr()?);
    println!("http://{}/pkarr/<key>", listener.local_addr()?);
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
