//! Local HTTP gateway command line interface.

use std::net::{SocketAddr, SocketAddrV4};

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
    /// Loopback HTTP listen address (plaintext).
    #[arg(long, default_value = "127.0.0.1:45475")]
    listen: SocketAddr,
    /// Address index server to use instead of discovering one.
    #[arg(long, env = "IROH_ADDR_INDEX")]
    index_server: Option<SocketAddrV4>,
    /// Pkarr public key of the trusted server list, as z-base-32 or 64 hex digits.
    #[arg(long, env = "IROH_ADDR_INDEX_LIST_KEY", value_parser = parse_list_key)]
    index_list_key: Option<[u8; 32]>,
    /// Rendezvous hash as 40 hex digits; defaults to the protocol hash if no discovery source is set.
    #[arg(long, env = "IROH_ADDR_INDEX_RENDEZVOUS", value_parser = parse_hex::<20>)]
    rendezvous_hash: Option<[u8; 20]>,
    /// Local Mainline UDP port; zero selects an available port.
    #[arg(long, default_value_t = 0)]
    dht_port: u16,
}

impl Args {
    fn discovery_config(&self) -> DiscoveryConfig {
        DiscoveryConfig {
            server: self.index_server,
            public_key: self.index_list_key,
            rendezvous_hash: self.rendezvous_hash.or_else(|| {
                (self.index_server.is_none() && self.index_list_key.is_none())
                    .then_some(RENDEZVOUS_INFOHASH)
            }),
        }
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
    let dht = Dht::builder().port(args.dht_port).build()?;
    let config = args.discovery_config();
    info!("finding index servers");
    let index = AddrIndex::discover_with_config(dht.clone(), config).await?;
    let resolver = Resolver::new(dht, index);
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
    fn defaults_to_rendezvous_and_preserves_explicit_discovery_sources() {
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
        let default = parse(vec!["gateway"]).unwrap().discovery_config();
        assert_eq!(default.rendezvous_hash, Some(RENDEZVOUS_INFOHASH));
        assert!(default.server.is_none());
        assert!(default.public_key.is_none());
        let curated = parse(vec!["gateway", "--index-list-key", &key]).unwrap();
        assert!(curated.discovery_config().rendezvous_hash.is_none());
        let custom = parse(vec!["gateway", "--rendezvous-hash", &hash]).unwrap();
        assert_eq!(custom.discovery_config().rendezvous_hash, Some([1; 20]));
        let both = parse(vec![
            "gateway",
            "--index-list-key",
            &key,
            "--rendezvous-hash",
            &hash,
        ])
        .unwrap()
        .discovery_config();
        assert_eq!(both.public_key, Some([42; 32]));
        assert_eq!(both.rendezvous_hash, Some([1; 20]));
        let direct = parse(vec!["gateway", "--index-server", "127.0.0.1:11223"])
            .unwrap()
            .discovery_config();
        assert_eq!(direct.server, Some("127.0.0.1:11223".parse().unwrap()));
        assert!(direct.rendezvous_hash.is_none());
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
