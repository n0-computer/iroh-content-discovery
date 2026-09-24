//! Standalone signed index-list publisher.

use clap::Parser;
use data_encoding::HEXLOWER_PERMISSIVE;
use iroh_mainline_endpoint_discovery::{ServerList, republish_server_list};
use n0_error::{Result, StdResultExt};
use n0_mainline::{Dht, MutableItem, SigningKey};
use std::net::SocketAddrV4;
use tracing::info;
use zeroize::Zeroizing;

const SECRET_ENV: &str = "IROH_INDEX_LIST_SECRET";

#[derive(Parser)]
#[command(
    about = "Publish and renew a BEP44 server list; reads IROH_INDEX_LIST_SECRET (64 hex digits)"
)]
struct Cli {
    /// Public index server sockets (at most two). Omit to publish an empty list.
    #[arg(long, num_args = 1..=2)]
    server: Vec<SocketAddrV4>,
    /// Nonnegative BEP44 sequence; increase whenever the list changes.
    #[arg(long)]
    sequence: i64,
    /// Local Mainline UDP port; zero selects an available port.
    #[arg(long, default_value_t = 0)]
    dht_port: u16,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    // Read and remove the environment secret before any runtime or logging
    // threads start. Do not retain it in the long-lived async task.
    let secret = std::env::var_os(SECRET_ENV).std_context("IROH_INDEX_LIST_SECRET is required")?;
    // SAFETY: this standalone binary is still single-threaded, before creating
    // the runtime or initializing tracing; no concurrent environment access.
    unsafe { std::env::remove_var(SECRET_ENV) };
    let secret = Zeroizing::new(secret.into_encoded_bytes());
    let item = sign(&cli, &secret)?;
    drop(secret);
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let public_key = item
        .key()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    info!(%public_key, sequence = item.seq(), "starting index-list republisher");
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .anyerr()?
        .block_on(async {
            let dht = Dht::builder().port(cli.dht_port).build()?;
            tokio::select! {
                result = republish_server_list(dht, item) => result,
                result = tokio::signal::ctrl_c() => result.map_err(Into::into),
            }
        })
}

fn sign(cli: &Cli, secret: &[u8]) -> Result<MutableItem> {
    n0_error::ensure_any!(
        secret.len() == 64,
        "secret must contain exactly 64 hex digits"
    );
    let decoded = Zeroizing::new(
        HEXLOWER_PERMISSIVE
            .decode(secret)
            .map_err(|_| n0_error::anyerr!("secret must contain only hex digits"))?,
    );
    let bytes = Zeroizing::new(<[u8; 32]>::try_from(decoded.as_slice()).anyerr()?);
    let key = SigningKey::from_bytes(&bytes);
    // SigningKey zeroizes its secret on drop; the decoded buffer is Zeroizing.
    ServerList::new(cli.server.clone())?.sign(&key, cli.sequence)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_is_hex_and_signs_expected_list() {
        let cli = Cli {
            server: vec!["203.0.113.1:1234".parse().unwrap()],
            sequence: 7,
            dht_port: 0,
        };
        let item = sign(&cli, &[b'0'; 64]).unwrap();
        assert_eq!(
            item.key(),
            SigningKey::from_bytes(&[0; 32]).verifying_key().as_bytes()
        );
        assert_eq!(item.seq(), 7);
        assert_eq!(
            ServerList::decode(item.value()).unwrap().addresses(),
            cli.server
        );
        assert!(sign(&cli, &[b'x'; 64]).is_err());
        assert!(sign(&cli, &[b'0'; 63]).is_err());
    }
}
