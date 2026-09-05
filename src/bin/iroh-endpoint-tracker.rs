//! Directory replica plus publisher / finder CLIs.

use std::{
    net::{SocketAddr, SocketAddrV4},
    path::PathBuf,
    time::Instant,
};

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use iroh::{SecretKey, endpoint::presets};
use iroh_endpoint_tracker::{
    Directory, Limits, MAX_ALPN_LEN, Server, SignedRecord, UdpClient, infohash_hex, parse_infohash,
};
use tokio::signal;
use tracing::info;

#[derive(Parser, Debug)]
#[command(
    name = "iroh-endpoint-tracker",
    about = "Addr → EndpointId directory: join Mainline get_peers contacts to iroh identities"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run a directory replica.
    Serve(ServeArgs),
    /// Sign the DHT-seen mapping and publish it to the directory.
    Publish(PublishArgs),
    /// Directory lookup for one compact ip:port.
    Lookup(LookupArgs),
    /// Directory lookup for compact peers from get_peers.
    Find(FindArgs),
}

#[derive(Parser, Debug)]
struct ServeArgs {
    /// Maximum addresses per published record.
    #[arg(long, default_value_t = 16)]
    max_addrs: usize,
    /// Maximum ALPNs per published record.
    #[arg(long, default_value_t = 8)]
    max_alpns: usize,
    /// How long accepted records are retained, in seconds.
    #[arg(long, default_value_t = 3600)]
    record_ttl_secs: u64,
    /// Serve publish/resolve on this UDP address.
    #[arg(long, default_value = "0.0.0.0:11223")]
    udp_bind: SocketAddr,
    /// Disable checking that a publish datagram came from a claimed IP.
    #[arg(long)]
    no_verify_udp_source_ip: bool,
}

#[derive(Parser, Debug)]
struct PublishArgs {
    /// Directory UDP socket.
    #[arg(long)]
    udp: SocketAddr,
    /// Compact ip:port the DHT will return for this publisher (BEP 42 + announce port).
    #[arg(long)]
    dht_addr: SocketAddrV4,
    /// ALPNs this endpoint currently accepts. At least one is required; repeatable.
    #[arg(long = "alpn", required = true)]
    alpns: Vec<String>,
    /// 40-char infohash hex or 64-char BLAKE3 hex (printed so you can announce_peer).
    hash: String,
    #[arg(long, env = "IROH_SECRET_KEY")]
    secret_key: Option<String>,
    #[arg(long, env = "IROH_SECRET_KEY_FILE")]
    secret_key_file: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct LookupArgs {
    /// Directory UDP socket.
    #[arg(long)]
    udp: SocketAddr,
    /// Compact ip:port as returned by get_peers.
    addr: SocketAddrV4,
}

#[derive(Parser, Debug)]
struct FindArgs {
    /// Directory UDP socket.
    #[arg(long)]
    udp: SocketAddr,
    /// Compact peers from get_peers (repeatable).
    #[arg(long = "peer", required = true)]
    peers: Vec<SocketAddrV4>,
}

fn load_secret(hex: Option<&str>, file: Option<&PathBuf>) -> Result<SecretKey> {
    if let Some(hex) = hex {
        return Ok(hex.parse()?);
    }
    if let Some(path) = file {
        if path.exists() {
            let hex = std::fs::read_to_string(path)?;
            return Ok(hex.trim().parse()?);
        }
        let key = SecretKey::generate();
        let encoded: String = key.to_bytes().iter().map(|b| format!("{b:02x}")).collect();
        std::fs::write(path, format!("{encoded}\n"))?;
        info!("wrote new secret key to {}", path.display());
        return Ok(key);
    }
    Ok(SecretKey::generate())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    match Cli::parse().command {
        Command::Serve(args) => serve(args).await,
        Command::Publish(args) => publish(args).await,
        Command::Lookup(args) => lookup(args).await,
        Command::Find(args) => find(args).await,
    }
}

async fn serve(args: ServeArgs) -> Result<()> {
    let limits = Limits {
        verify_udp_source_ip: !args.no_verify_udp_source_ip,
        max_addrs: args.max_addrs,
        max_alpns: args.max_alpns,
        record_ttl_secs: args.record_ttl_secs,
        ..Limits::default()
    };

    let endpoint = iroh::Endpoint::bind(presets::Minimal).await?;
    let server = Server::new(limits);
    server.set_probe(endpoint.clone());
    let udp = server.bind_udp(args.udp_bind).await?;
    println!("udp: {}", udp.local_addr());

    let start = Instant::now();
    signal::ctrl_c().await?;
    info!("shutting down after {:?}", start.elapsed());
    drop(udp);
    endpoint.close().await;
    Ok(())
}

fn publish_alpns(raw: &[String]) -> Result<Vec<Vec<u8>>> {
    let alpns: Vec<Vec<u8>> = raw.iter().map(|s| s.as_bytes().to_vec()).collect();
    if alpns.is_empty() {
        bail!("at least one --alpn is required");
    }
    if alpns
        .iter()
        .any(|alpn| alpn.is_empty() || alpn.len() > MAX_ALPN_LEN)
    {
        bail!("ALPNs must contain 1..={MAX_ALPN_LEN} bytes");
    }
    Ok(alpns)
}

async fn publish(args: PublishArgs) -> Result<()> {
    let secret = load_secret(args.secret_key.as_deref(), args.secret_key_file.as_ref())?;
    let infohash = parse_infohash(&args.hash)?;
    let alpns = publish_alpns(&args.alpns)?;
    let rec = SignedRecord::sign(&secret, vec![args.dht_addr], alpns);
    let dir = Directory::udp(args.udp).await?;
    dir.publish(rec.clone()).await?;
    println!("published eid {}", rec.eid);
    println!("addrs: {:?}", rec.addrs);
    println!(
        "alpns: {:?}",
        rec.alpns
            .iter()
            .map(|a| String::from_utf8_lossy(a))
            .collect::<Vec<_>>()
    );
    println!(
        "announce_peer infohash {} port {}",
        infohash_hex(&infohash),
        args.dht_addr.port()
    );
    Ok(())
}

async fn lookup(args: LookupArgs) -> Result<()> {
    let res = UdpClient::bind()
        .await?
        .resolve_from(args.udp, args.addr)
        .await?;
    if res.truncated {
        eprintln!("warning: UDP resolve truncated to one MTU");
    }
    let hits = res.records;
    if hits.is_empty() {
        bail!("no live eids for {}", args.addr);
    }
    print_hits(&hits);
    Ok(())
}

async fn find(args: FindArgs) -> Result<()> {
    let client = UdpClient::bind().await?;
    let mut hits = Vec::new();
    for peer in args.peers {
        let res = client.resolve_from(args.udp, peer).await?;
        if res.truncated {
            eprintln!("warning: UDP resolve truncated for {peer}");
        }
        hits.extend(res.records);
    }
    if hits.is_empty() {
        bail!("no directory hits");
    }
    print_hits(&hits);
    Ok(())
}

fn print_hits(hits: &[iroh_endpoint_tracker::SignedRecord]) {
    for h in hits {
        println!("{}  ts={}  index={:?}", h.eid, h.ts, h.index);
        println!("  addrs: {:?}", h.addrs);
        println!(
            "  alpns: {:?}",
            h.alpns
                .iter()
                .map(|a| String::from_utf8_lossy(a))
                .collect::<Vec<_>>()
        );
        println!("  (probe a BLAKE3 chunk before caching hash → eid)");
    }
}
