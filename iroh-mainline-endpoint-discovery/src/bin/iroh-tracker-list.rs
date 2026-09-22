//! Standalone signed tracker-list publisher.

use clap::Parser;
use iroh_mainline_endpoint_discovery::{TrackerList, republish_tracker_list};
use n0_error::{Result, StdResultExt};
use n0_mainline::{Dht, MutableItem, SigningKey};
use std::net::SocketAddrV4;
use zeroize::Zeroizing;

const SECRET_ENV: &str = "IROH_TRACKER_LIST_SECRET";

#[derive(Parser)]
#[command(
    about = "Publish and renew a BEP44 tracker list; reads IROH_TRACKER_LIST_SECRET (64 hex digits)"
)]
struct Cli {
    /// Public tracker sockets (at most two). Omit to publish an empty list.
    #[arg(long, num_args = 1..=2)]
    tracker: Vec<SocketAddrV4>,
    /// Nonnegative BEP44 sequence; increase whenever the list changes.
    #[arg(long)]
    sequence: i64,
    /// Local Mainline UDP port; zero selects an available port.
    #[arg(long, default_value_t = 0)]
    udp_port: u16,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    // Read and remove the environment secret before any runtime or logging
    // threads start. Do not retain it in the long-lived async task.
    let secret =
        std::env::var_os(SECRET_ENV).std_context("IROH_TRACKER_LIST_SECRET is required")?;
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
    tracing::info!(%public_key, sequence = item.seq(), "starting tracker-list republisher");
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .anyerr()?
        .block_on(async {
            let dht = Dht::builder().port(cli.udp_port).build()?;
            tokio::select! {
                result = republish_tracker_list(dht, item) => result,
                result = tokio::signal::ctrl_c() => result.map_err(Into::into),
            }
        })
}

fn sign(cli: &Cli, secret: &[u8]) -> Result<MutableItem> {
    n0_error::ensure_any!(
        secret.len() == 64,
        "secret must contain exactly 64 hex digits"
    );
    let mut bytes = Zeroizing::new([0u8; 32]);
    for (out, pair) in bytes.iter_mut().zip(secret.chunks_exact(2)) {
        let digit = |b: u8| {
            (b as char)
                .to_digit(16)
                .std_context("secret must contain only hex digits")
        };
        *out = ((digit(pair[0])? << 4) | digit(pair[1])?) as u8;
    }
    let key = SigningKey::from_bytes(&bytes);
    // SigningKey zeroizes its secret on drop; the decoded buffer is Zeroizing.
    TrackerList::new(cli.tracker.clone())?.sign(&key, cli.sequence)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_is_hex_and_signs_expected_list() {
        let cli = Cli {
            tracker: vec!["203.0.113.1:1234".parse().unwrap()],
            sequence: 7,
            udp_port: 0,
        };
        let item = sign(&cli, &[b'0'; 64]).unwrap();
        assert_eq!(
            item.key(),
            SigningKey::from_bytes(&[0; 32]).verifying_key().as_bytes()
        );
        assert_eq!(item.seq(), 7);
        assert_eq!(
            TrackerList::decode(item.value()).unwrap().addresses(),
            cli.tracker
        );
        assert!(sign(&cli, &[b'x'; 64]).is_err());
        assert!(sign(&cli, &[b'0'; 63]).is_err());
    }
}
