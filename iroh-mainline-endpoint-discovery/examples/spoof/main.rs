//! Directory replica plus a [`publisher::Publisher`] on the public Mainline DHT
//! and a [`resolver::Resolver`] that maps infohash → [`iroh::EndpointId`].
//!
//! One peer announces `SHA-1` of a tiny demo blob's BLAKE3. A spoofer tries
//! to steal that mapping and is rejected. A bit later a second peer resolves
//! the infohash via Mainline `get_peers` and the directory. Extra 64-char
//! BLAKE3 hex or 40-char infohash hex can be passed on the CLI.
//!
//! ```sh
//! cargo run --example spoof -- <blake3-hex|infohash-hex>…
//! ```

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use iroh::{SecretKey, endpoint::presets, protocol::Router};
use iroh_addr_index::{DEFAULT_PROBE_TIMEOUT, Limits, PROBE_ALPN, ProbeAccept, Server};
use iroh_addr_index_proto::SignedRecord;
use iroh_mainline_endpoint_discovery::{
    Directory, Publisher, Resolver, infohash_from_blake3, parse_infohash,
};
use n0_mainline::{Dht, Id};

/// Pause after the publisher is live before the resolver looks the hash up.
const RESOLVE_DELAY: Duration = Duration::from_secs(5);
/// Give public Mainline this long after that pause to return the publisher.
const RESOLVE_BUDGET: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("info,iroh_addr_index=trace")
            }),
        )
        .init();

    let mut infohashes = parse_infohashes(std::env::args().skip(1))?;

    let probe_ep = iroh::Endpoint::bind(iroh::endpoint::presets::Minimal)
        .await
        .context("probe endpoint")?;
    let directory = Server::new(Limits {
        verify_udp_source_ip: false,
        ..Limits::default()
    });
    directory.set_probe(probe_ep.clone());
    let directory_udp = directory.bind_udp("127.0.0.1:0".parse()?).await?;

    let secret = SecretKey::generate();
    let publisher_ep = iroh::Endpoint::builder(presets::N0)
        .secret_key(secret)
        .bind()
        .await
        .context("publisher endpoint")?;
    let publisher_router = Router::builder(publisher_ep.clone())
        .accept(PROBE_ALPN, ProbeAccept)
        .spawn();
    let blob_hash = blake3::hash(b"iroh-mainline-endpoint-discovery demo blob\n");
    let blob_infohash = Id::from(infohash_from_blake3(&blob_hash));
    infohashes.push(blob_infohash);
    println!("blob {blob_hash} infohash {blob_infohash}");
    for id in infohashes.iter().filter(|id| **id != blob_infohash) {
        println!("infohash {id}");
    }

    // Publisher and resolver are two roles of this node, not two DHT nodes.
    let dht = Dht::client().context("DHT")?;
    if !dht.bootstrapped().await? {
        bail!("DHT bootstrap failed");
    }
    let index = Directory::udp(directory_udp.local_addr()).await?;
    let (publisher, resolver) = tokio::try_join!(
        Publisher::bind(publisher_ep.clone(), dht.clone(), index.clone()),
        Resolver::bind(dht, index),
    )?;
    for infohash in infohashes {
        publisher.add_infohash(infohash, PROBE_ALPN)?;
    }
    println!("publisher {}", publisher.id());

    let workflow = async {
        publisher.wait_published().await;
        println!("published {:?}", publisher.public_v4().await);

        let resolve = async {
            tokio::time::sleep(RESOLVE_DELAY).await;
            resolve_publisher(&resolver, blob_infohash, publisher.id()).await
        };
        tokio::select! {
            r = resolve => r,
            r = spoof_loop(&publisher) => r,
        }
    };

    tokio::select! {
        r = publisher.run() => r?,
        r = workflow => r?,
        _ = tokio::signal::ctrl_c() => bail!("interrupted"),
    }

    publisher_router.shutdown().await.ok();
    publisher_ep.close().await;
    probe_ep.close().await;
    Ok(())
}

async fn resolve_publisher(
    resolver: &Resolver,
    infohash: Id,
    expect: iroh::EndpointId,
) -> Result<()> {
    let deadline = Instant::now() + RESOLVE_BUDGET;
    loop {
        match resolver.resolve(infohash).await {
            Ok(eids) if eids.contains(&expect) => {
                println!("resolve {infohash}:");
                for eid in &eids {
                    println!("  {eid}");
                }
                return Ok(());
            }
            Ok(eids) => tracing::debug!(?eids, "resolve: publisher not yet"),
            Err(err) => tracing::warn!(%err, "resolve"),
        }
        if Instant::now() >= deadline {
            bail!("timed out resolving {infohash} (wanted {expect})");
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn spoof_loop(publisher: &Publisher) -> Result<()> {
    let dir = publisher.directory();
    loop {
        let Some(compact) = publisher.public_v4().await.into_iter().next() else {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        };
        let spoof = SignedRecord::sign(&SecretKey::generate(), vec![compact], [PROBE_ALPN]);
        let spoof_eid = spoof.eid;
        if let Err(err) = dir.publish(spoof).await {
            println!("spoof {spoof_eid} rejected: {err}");
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }
        println!("spoof offered {compact} → {spoof_eid}");
        // UDP publish is fire-and-forget; wait out the mapping probe.
        tokio::time::sleep(DEFAULT_PROBE_TIMEOUT + Duration::from_millis(200)).await;
        let records = dir.lookup(compact).await.context("spoof check")?;
        if records.iter().any(|r| r.eid == spoof_eid) {
            bail!("spoof {spoof_eid} was stored at {compact}");
        }
        println!("spoof {spoof_eid} rejected");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn parse_infohashes(args: impl IntoIterator<Item = String>) -> Result<Vec<Id>> {
    let mut out = Vec::new();
    for s in args {
        let raw = parse_infohash(&s).with_context(|| format!("hash {s}"))?;
        out.push(Id::from(raw));
    }
    Ok(out)
}
