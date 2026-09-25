//! Resolves a BLAKE3 hash to endpoint IDs with discovery diagnostics enabled.
//!
//! Run `cargo run -p iroh-mainline-endpoint-discovery --example resolve -- <URL-or-hash>`.
//! Uses public rendezvous discovery by default; `--index-server IP:PORT` overrides it.
//! Endpoint IDs go to stdout, diagnostics to stderr. `RUST_LOG` overrides the filter.

use std::{collections::BTreeSet, net::SocketAddrV4, time::Duration};

use clap::Parser;
use data_encoding::HEXLOWER_PERMISSIVE;
use iroh_mainline_endpoint_discovery::{
    AddrIndex, Hash, Resolver, infohash_from_blake3, infohash_hex,
};
use n0_error::{Result, StdResultExt, bail_any};
use n0_future::StreamExt;
use n0_mainline::Dht;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(about = "Resolve a blake3.net URL or raw BLAKE3 hash with discovery logs")]
struct Args {
    /// HTTP(S) blake3.net URL, z-base-32 hash, or 64-digit hexadecimal hash.
    #[arg(value_parser = parse_hash)]
    hash: Hash,
    /// Address index server; otherwise use the public rendezvous hash.
    #[arg(long, env = "IROH_ADDR_INDEX")]
    index_server: Option<SocketAddrV4>,
    /// Overall timeout in seconds, including index-server discovery.
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
    timeout: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            "info,iroh_mainline_endpoint_discovery=debug,n0_mainline=debug".into()
        }))
        .with_writer(std::io::stderr)
        .init();
    tokio::time::timeout(Duration::from_secs(args.timeout), resolve(args))
        .await
        .std_context(
            "resolution timed out; any endpoint IDs already printed remain valid discovery results",
        )?
}

async fn resolve(args: Args) -> Result<()> {
    let infohash = infohash_from_blake3(&args.hash);
    info!(hash = %args.hash, infohash = %infohash_hex(&infohash), "resolving content providers");
    let dht = Dht::client()?;
    info!(server = ?args.index_server, "finding index servers (public rendezvous if no explicit server)");
    let index = match args.index_server {
        Some(server) => AddrIndex::udp(dht.clone(), server).await?,
        None => AddrIndex::discover(dht.clone()).await?,
    };
    let resolver = Resolver::new(dht, index);
    let mut providers = resolver.resolve_stream(infohash.into()).await?;
    let mut unique = BTreeSet::new();
    while let Some(endpoint) = providers.next().await {
        if unique.insert(endpoint) {
            println!("{endpoint}");
        }
    }
    info!(
        providers = unique.len(),
        "provider discovery complete (content availability has not been probed)"
    );
    if unique.is_empty() {
        bail_any!("no endpoint IDs found; see discovery logs above");
    }
    Ok(())
}

fn parse_hash(input: &str) -> std::result::Result<Hash, String> {
    let value = input.trim();
    let value = if let Some((scheme, rest)) = value.split_once("://") {
        if !matches!(scheme, "http" | "https") {
            return Err("expected an HTTP(S) blake3.net URL".into());
        }
        rest.split(['/', '?', '#'])
            .next()
            .and_then(|host| host.strip_suffix(".blake3.net"))
            .ok_or("expected https://<hash>.blake3.net/")?
    } else {
        value
    };
    let bytes = if value.len() == 64 {
        HEXLOWER_PERMISSIVE.decode(value.as_bytes()).ok()
    } else {
        z32::decode(value.as_bytes()).ok()
    };
    let bytes: [u8; 32] = bytes
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or("expected a z-base-32 or 64-digit hexadecimal BLAKE3 hash")?;
    Ok(Hash::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_urls_and_both_hash_encodings() {
        let hash = Hash::from_bytes([42; 32]);
        let z32 = z32::encode(hash.as_bytes());
        for input in [
            hash.to_hex().to_string(),
            z32.clone(),
            format!("https://{z32}.blake3.net/path/file?query#fragment"),
        ] {
            assert_eq!(parse_hash(&input).unwrap(), hash);
        }
        for input in [
            "invalid",
            "https://example.com/",
            "ftp://example.com/",
            "https://blake3.net/",
            "https://not-a-hash.blake3.net/",
        ] {
            assert!(parse_hash(input).is_err());
        }
    }
}
