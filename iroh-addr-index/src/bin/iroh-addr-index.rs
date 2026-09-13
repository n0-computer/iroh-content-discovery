//! Address-index replica command line interface.

use std::{net::SocketAddr, time::Instant};

use anyhow::Result;
use clap::Parser;
use iroh_addr_index::{Limits, Server};
use tokio::signal;
use tracing::info;

#[derive(Debug, Parser)]
#[command(
    name = "iroh-addr-index",
    about = "Opaque return-routability-gated UDP address index"
)]
struct Cli {
    /// Serve on this UDP address.
    #[arg(long, default_value = "0.0.0.0:11223")]
    udp_bind: SocketAddr,
    /// Maximum opaque value length.
    #[arg(long, default_value_t = iroh_addr_index_proto::MAX_VALUE_LEN)]
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
    let udp = server.bind_udp(cli.udp_bind).await?;
    println!("udp: {}", udp.local_addr());
    let start = Instant::now();
    signal::ctrl_c().await?;
    info!(elapsed = ?start.elapsed(), "shutting down");
    Ok(())
}
