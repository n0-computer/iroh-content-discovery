//! Publish an apex HTTPS record and keep its signed packet alive on Mainline.

use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::Parser;
use iroh_mainline_endpoint_discovery::{PkarrPublisher, pkarr_name};
use n0_mainline::{Dht, SigningKey};

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

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let key = load_key(args.key_file.as_deref())?;
    let dht = Dht::builder().port(0).build()?;
    let publisher = PkarrPublisher::new(dht);
    publisher.set_https(&key, "", &args.target)?;
    let public_key = pkarr_name(&key.verifying_key().to_bytes());
    println!(
        "Publishing HTTPS target {} on the public Mainline DHT...",
        args.target
    );
    println!("Public key: {public_key}");
    if args.key_file.is_none() {
        println!("Temporary identity; use --key-file to reuse a key across runs.");
    }
    let publish = async {
        // The first publish reports failures; the publisher's own task then
        // keeps the name alive.
        publisher.publish_all().await.map_err(anyhow::Error::from)?;
        println!("Published: https://{public_key}.pkarr.link/");
        println!("Local origin: http://{public_key}.pkarr.localhost:8080/");
        println!("Republishing every ten minutes. Press Ctrl-C to stop.");
        std::future::pending().await
    };
    tokio::select! {
        result = publish => result,
        result = tokio::signal::ctrl_c() => { result?; Ok(()) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuses_a_key_file() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("key");
        let key = load_key(Some(&path))?;
        assert_eq!(key.verifying_key(), load_key(Some(&path))?.verifying_key());
        // Without a file each run is a new identity.
        assert_ne!(
            load_key(None)?.verifying_key(),
            load_key(None)?.verifying_key()
        );
        Ok(())
    }
}
