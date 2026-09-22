//! Show that another UDP socket cannot replace a publisher's address-index value.
//!
//! A public replica is required so it and Mainline observe the same public UDP
//! mapping for the shared DHT socket.

use std::time::{Duration, Instant};

use iroh::{SecretKey, endpoint::presets};
use iroh_mainline_endpoint_discovery::{
    Directory, Publisher, Resolver, SignedRecord, infohash_from_blake3, parse_infohash,
};
use n0_error::{Result, StackResultExt, StdResultExt, bail_any};
use n0_mainline::{Dht, Id};

const RESOLVE_DELAY: Duration = Duration::from_secs(5);
const RESOLVE_BUDGET: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("info,iroh_mainline_endpoint_discovery=trace")
            }),
        )
        .init();

    let replica = std::env::var("IROH_ADDR_INDEX")
        .ok()
        .map(|value| value.parse())
        .transpose()
        .std_context("invalid IROH_ADDR_INDEX socket")?;
    let mut infohashes = parse_infohashes(std::env::args().skip(1))?;

    let secret = SecretKey::generate();
    let publisher_ep = iroh::Endpoint::builder(presets::N0)
        .secret_key(secret.clone())
        .bind()
        .await
        .context("publisher endpoint")?;
    let demo_hash = blake3::hash(b"iroh-mainline-endpoint-discovery demo\n");
    let demo_infohash = Id::from(infohash_from_blake3(&demo_hash));
    infohashes.push(demo_infohash);

    let dht = Dht::client().std_context("DHT")?;
    if !dht.bootstrapped().await? {
        bail_any!("DHT bootstrap failed");
    }
    let index = match replica {
        Some(replica) => Directory::udp(dht.clone(), replica).await?,
        None => Directory::discover(dht.clone()).await?,
    };
    let publisher = Publisher::new(secret, dht.clone(), index.clone());
    let resolver = Resolver::bind(dht, index).await?;
    for infohash in infohashes {
        publisher.add_infohash(infohash);
    }

    println!("endpoint {}", publisher.id());
    println!("demo infohash {demo_infohash}");

    let workflow = async {
        publisher.wait_published().await;
        let mapping = publisher
            .public_v4()
            .std_context("publisher has no mapping")?;
        println!("published {mapping}");

        let attacker_dht = Dht::client().std_context("attacker DHT socket")?;
        let attacker = match replica {
            Some(replica) => Directory::udp(attacker_dht, replica).await?,
            None => Directory::discover(attacker_dht).await?,
        };
        let spoof = SignedRecord::sign(&SecretKey::generate());
        let attacker_addrs = attacker.publish(&spoof).await?;
        println!("attacker could only publish at {attacker_addrs:?}");

        let records = publisher.directory().lookup(mapping).await?;
        if !records.iter().any(|record| record.eid == publisher.id()) {
            bail_any!("publisher value was replaced at {mapping}");
        }

        tokio::time::sleep(RESOLVE_DELAY).await;
        resolve_publisher(&resolver, demo_infohash, publisher.id()).await
    };

    tokio::select! {
        result = publisher.run() => result?,
        result = workflow => result?,
        _ = tokio::signal::ctrl_c() => bail_any!("interrupted"),
    }

    publisher_ep.close().await;
    Ok(())
}

async fn resolve_publisher(
    resolver: &Resolver,
    infohash: Id,
    expected: iroh::EndpointId,
) -> Result<()> {
    let deadline = Instant::now() + RESOLVE_BUDGET;
    loop {
        match resolver.resolve(infohash).await {
            Ok(ids) if ids.contains(&expected) => {
                println!("resolved {infohash} to {expected}");
                return Ok(());
            }
            Ok(ids) => tracing::debug!(?ids, "publisher not returned yet"),
            Err(err) => tracing::warn!(%err, "resolve"),
        }
        if Instant::now() >= deadline {
            bail_any!("timed out resolving {infohash} to {expected}");
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

fn parse_infohashes(args: impl IntoIterator<Item = String>) -> Result<Vec<Id>> {
    args.into_iter()
        .map(|value| {
            parse_infohash(&value)
                .map(Id::from)
                .with_std_context(|_| format!("hash {value}"))
        })
        .collect()
}
