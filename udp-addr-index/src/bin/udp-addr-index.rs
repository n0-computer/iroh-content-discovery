//! Address-index server command line interface.

use n0_mainline::Dht;
use std::time::Instant;
use std::{net::SocketAddr, sync::Arc};

use clap::Parser;
use data_encoding::HEXLOWER_PERMISSIVE;
use iroh_metrics::{Registry, service::MetricsServer};
use n0_error::{Result, StdResultExt};
use tokio::signal;
use tracing::info;
use udp_addr_index::{Limits, Server};

#[derive(Debug, Parser)]
#[command(
    name = "udp-addr-index",
    about = "Opaque return-routability-gated UDP address index"
)]
struct Cli {
    /// Local Mainline UDP port, shared with the address index (binds all IPv4 interfaces).
    #[arg(long, default_value_t = 11223)]
    dht_port: u16,
    /// Maximum opaque value length.
    #[arg(long, default_value_t = udp_addr_index_proto::MAX_VALUE_LEN)]
    max_value_len: usize,
    /// Maximum number of live entries.
    #[arg(long, default_value_t = 2_000_000)]
    max_entries: usize,
    /// How long an accepted value remains live, in seconds.
    #[arg(long, default_value_t = 3600)]
    value_ttl_secs: u64,
    /// Length of one token validity bucket, in seconds.
    #[arg(long, default_value_t = 30)]
    token_bucket_secs: u64,
    /// Mainline rendezvous infohash (40 hex digits); omit to serve without announcing.
    #[arg(long, value_parser = parse_infohash)]
    rendezvous_hash: Option<[u8; 20]>,
    /// Bind address for the Prometheus /metrics endpoint (for example 127.0.0.1:9090).
    #[arg(long)]
    metrics_listen: Option<SocketAddr>,
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
    let server = Server::new(Limits {
        max_value_len: cli.max_value_len,
        max_entries: cli.max_entries,
        value_ttl_secs: cli.value_ttl_secs,
        token_bucket_secs: cli.token_bucket_secs,
        ..Limits::default()
    });
    let _metrics_server = if let Some(addr) = cli.metrics_listen {
        let mut registry = Registry::default();
        registry.register(server.metrics());
        let metrics_server = MetricsServer::spawn(addr, Arc::new(registry))
            .await
            .std_context("failed to start the metrics server")?;
        info!(addr = %metrics_server.local_addr(), "metrics listening");
        Some(metrics_server)
    } else {
        None
    };
    let dht = Dht::builder()
        .server_mode()
        .port(cli.dht_port)
        .build()
        .std_context("failed to bind the Mainline socket")?;
    let mut udp = server
        .attach_with_rendezvous(dht, cli.rendezvous_hash)
        .await?;
    println!("udp: {}", udp.local_addr());
    let start = Instant::now();
    tokio::select! {
        result = shutdown_signal() => result?,
        result = udp.terminated() => result?,
    }
    info!(elapsed = ?start.elapsed(), "shutting down");
    Ok(())
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate = signal::unix::signal(signal::unix::SignalKind::terminate()).anyerr()?;
        tokio::select! {
            result = signal::ctrl_c() => result.anyerr()?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    signal::ctrl_c().await.anyerr()?;
    Ok(())
}

fn parse_infohash(value: &str) -> std::result::Result<[u8; 20], String> {
    let invalid = || "expected 40 hexadecimal digits".to_owned();
    HEXLOWER_PERMISSIVE
        .decode(value.as_bytes())
        .map_err(|_| invalid())?
        .try_into()
        .map_err(|_| invalid())
}
