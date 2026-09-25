//! Publishes an endpoint mapping and reads it through a second UDP socket.

use std::{net::SocketAddrV4, time::Duration};

use clap::Parser;
use iroh_base::SecretKey;
use iroh_mainline_endpoint_discovery::AddrIndex;
use n0_error::{Result, StackResultExt, StdResultExt, ensure_any};
use n0_mainline::Dht;

#[derive(Parser)]
struct Args {
    /// Index server to test directly, without rendezvous discovery.
    #[arg(long, env = "IROH_ADDR_INDEX")]
    index_server: SocketAddrV4,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,iroh_mainline_endpoint_discovery=debug".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();
    tokio::time::timeout(Duration::from_secs(20), async {
        let publisher = Dht::builder()
            .no_bootstrap()
            .port(0)
            .build()
            .std_context("publisher UDP socket")?;
        let reader = Dht::builder()
            .no_bootstrap()
            .port(0)
            .build()
            .std_context("reader UDP socket")?;
        let writer = AddrIndex::udp(publisher, args.index_server).await?;
        let reader = AddrIndex::udp(reader, args.index_server).await?;
        let key = SecretKey::from_bytes(&rand::random());
        println!("Index server: {}", args.index_server);
        println!("Publishing endpoint: {}", key.public());
        let mappings = writer
            .publish(&key)
            .await
            .context("publish mapping (prepare + put)")?;
        ensure_any!(!mappings.is_empty(), "no mapping was published");
        for mapping in mappings {
            println!("Stored mapping: {mapping}; reading from a separate UDP socket...");
            let records = reader.lookup(mapping).await.context("read mapping (get)")?;
            ensure_any!(
                records
                    .iter()
                    .any(|record| record.endpoint_id == key.public()),
                "published endpoint missing from verified records"
            );
            println!("Verified mapping: {mapping} -> {}", key.public());
        }
        println!("PASS: publish and verified read");
        Ok(())
    })
    .await
    .std_context("index round trip exceeded 20 seconds")?
}
