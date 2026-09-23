//! Publish an apex HTTPS record and keep its signed packet alive on Mainline.

use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, ensure};
use clap::Parser;
use n0_mainline::{
    Dht, MutableItem, SigningKey,
    errors::{PutMutableError, PutQueryError},
};
use simple_dns::{
    CLASS, Packet, ResourceRecord,
    rdata::{HTTPS, RData, SVCB},
};

#[derive(Parser)]
#[command(about = "Publish a Pkarr HTTPS target and republish every ten minutes until Ctrl-C")]
struct Args {
    /// Target hostname, e.g. example.com or <hash>.blake3.link (no scheme or path).
    target: String,
    /// Read or create a 32-byte secret-key file to retain the same public-key URL.
    /// Without this option, generate a temporary identity for this run.
    #[arg(long)]
    key_file: Option<PathBuf>,
}

fn load_key(path: Option<&Path>) -> Result<SigningKey> {
    let Some(path) = path else {
        return Ok(SigningKey::from_bytes(&rand::random()));
    };
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(mut file) => {
            let secret: [u8; 32] = rand::random();
            file.write_all(&secret)?;
            file.sync_all()?;
            Ok(SigningKey::from_bytes(&secret))
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let mut secret = Vec::new();
            File::open(path)?.take(33).read_to_end(&mut secret)?;
            let secret: [u8; 32] = secret
                .try_into()
                .map_err(|_| anyhow::anyhow!("key file must contain exactly 32 bytes"))?;
            Ok(SigningKey::from_bytes(&secret))
        }
        Err(error) => Err(anyhow::Error::from(error)),
    }
    .with_context(|| format!("key file {}", path.display()))
}

fn record(key: &SigningKey, target: &str) -> Result<MutableItem> {
    let target = target.strip_suffix('.').unwrap_or(target);
    ensure!(
        target.len() <= 253
            && target.contains('.')
            && target.split('.').all(|label| {
                !label.is_empty()
                    && label.len() <= 63
                    && !label.starts_with('-')
                    && !label.ends_with('-')
                    && label
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            }),
        "target must be a DNS hostname, without a scheme, port, or path"
    );
    let public_key = z32::encode(key.verifying_key().as_bytes());
    let mut packet = Packet::new_reply(0);
    packet.answers.push(ResourceRecord::new(
        public_key.as_str().try_into()?,
        CLASS::IN,
        300,
        RData::HTTPS(HTTPS(SVCB::new(0, target.try_into()?))),
    ));
    let bytes = packet.build_bytes_vec_compressed()?;
    ensure!(
        bytes.len() <= 1000,
        "DNS packet exceeds the BEP44 size limit"
    );
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_micros()
        .try_into()?;
    Ok(MutableItem::new(key, &bytes, timestamp, None))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let key = load_key(args.key_file.as_deref())?;
    let item = record(&key, &args.target)?;
    drop(key);
    let public_key = z32::encode(item.key());
    let dht = Dht::builder().port(0).build()?;
    println!(
        "Publishing HTTPS target {} on the public Mainline DHT...",
        args.target
    );
    println!("Public key: {public_key}");
    if args.key_file.is_none() {
        println!("Temporary identity; use --key-file to reuse a key across runs.");
    }
    let publish = async {
        loop {
            let delay = match tokio::time::timeout(
                Duration::from_secs(60),
                dht.put_mutable(item.clone(), None),
            )
            .await
            {
                Ok(Ok(_)) => {
                    println!("Published: https://{public_key}.pkarr.link/");
                    println!("Direct gateway: http://127.0.0.1:8080/pkarr/{public_key}/");
                    println!("Republishing in ten minutes. Press Ctrl-C to stop.");
                    Duration::from_secs(600)
                }
                Ok(Err(error @ PutMutableError::Concurrency(_)))
                | Ok(Err(error @ PutMutableError::Query(PutQueryError::Shutdown))) => {
                    return Err(anyhow::Error::from(error));
                }
                Ok(Err(error)) => {
                    eprintln!("Publish failed: {error}; retrying in 30 seconds.");
                    Duration::from_secs(30)
                }
                Err(_) => {
                    eprintln!("Publish timed out; retrying in 30 seconds.");
                    Duration::from_secs(30)
                }
            };
            tokio::time::sleep(delay).await;
        }
    };
    tokio::select! {
        result = publish => result,
        result = tokio::signal::ctrl_c() => { result?; Ok(()) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use n0_future::StreamExt;

    #[tokio::test]
    async fn publishes_resolvable_https_record_and_reuses_key() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("key");
        let key = load_key(Some(&path))?;
        assert_eq!(key.verifying_key(), load_key(Some(&path))?.verifying_key());
        assert!(record(&key, "https://example.com/path").is_err());
        let item = record(&key, "example.com")?;
        let network = n0_mainline::Testnet::new(3).await?;
        let node = || Dht::builder().bootstrap(&network.bootstrap).port(0).build();
        let publisher = node()?;
        let resolver = node()?;
        tokio::time::timeout(Duration::from_secs(20), async {
            publisher.put_mutable(item.clone(), None).await?;
            let mut records = resolver.get_mutable(item.key(), None, None).await?;
            let found = records.next().await.context("record not found")?;
            assert_eq!(found.value(), item.value());
            let packet = Packet::parse(found.value())?;
            assert_eq!(packet.answers[0].name.to_string(), z32::encode(item.key()));
            let RData::HTTPS(https) = &packet.answers[0].rdata else {
                panic!("expected HTTPS")
            };
            assert_eq!(https.0.target.to_string(), "example.com");
            anyhow::Ok(())
        })
        .await??;
        Ok(())
    }
}
