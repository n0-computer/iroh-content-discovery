//! Joins an iroh-gossip swarm whose members find each other through Mainline.
//!
//! The topic string is hashed into both a gossip topic id and a Mainline
//! infohash. Every member announces itself under that infohash and resolves
//! the others, so no ticket has to be exchanged: two processes started with
//! the same topic string meet on their own.

use std::{collections::HashSet, net::SocketAddrV4, time::Duration};

use bytes::Bytes;
use clap::Parser;
use iroh::{Endpoint, EndpointId, endpoint::presets, protocol::Router};
use iroh_gossip::{
    api::{Event, GossipSender},
    net::{GOSSIP_ALPN, Gossip},
    proto::TopicId,
};
use iroh_mainline_endpoint_discovery::{AddrIndex, Publisher, Resolver, infohash_from_blake3};
use n0_error::{Result, StdResultExt, bail_any};
use n0_future::{
    StreamExt,
    task::{self, AbortOnDropHandle},
};
use n0_mainline::{Dht, Id};

/// Chat with everyone who knows the same topic string.
#[derive(Debug, Parser)]
struct Cli {
    /// Topic string, hashed into a gossip topic and a Mainline infohash.
    topic: String,
    /// Address index server to use instead of discovering one.
    #[arg(long, env = "IROH_ADDR_INDEX")]
    index_server: Option<SocketAddrV4>,
}

/// Separates gossip topics from other uses of the same string.
const TOPIC_SALT: &str = "iroh-mainline-endpoint-discovery gossip topic v1";

/// How often each member broadcasts.
const BROADCAST_INTERVAL: Duration = Duration::from_secs(1);

/// Neighbours that make a swarm worth staying in without looking for more.
const ENOUGH_NEIGHBORS: usize = 2;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    let topic_key = blake3::derive_key(TOPIC_SALT, cli.topic.as_bytes());
    let topic = TopicId::from_bytes(topic_key);
    let infohash = Id::from(infohash_from_blake3(&blake3::Hash::from_bytes(topic_key)));

    let endpoint = Endpoint::bind(presets::N0).await?;
    let me = endpoint.id();
    let gossip = Gossip::builder().spawn(endpoint.clone());
    let router = Router::builder(endpoint.clone())
        .accept(GOSSIP_ALPN, gossip.clone())
        .spawn();

    let dht = Dht::client().std_context("DHT")?;
    if !dht.bootstrapped().await? {
        bail_any!("DHT bootstrap failed");
    }
    let index = match cli.index_server {
        Some(server) => AddrIndex::udp(dht.clone(), server).await?,
        None => AddrIndex::discover(dht.clone()).await?,
    };
    // Announcing and resolving are independent, so a member that has not been
    // published yet already sees whoever came before it.
    let publisher = Publisher::new(endpoint.secret_key().clone(), dht.clone(), index.clone());
    publisher.add_infohash(infohash);
    let resolver = Resolver::new(dht, index);

    println!("topic {}", cli.topic);
    println!("infohash {infohash}");
    println!("me {}", me.fmt_short());

    let (sender, mut receiver) = gossip.subscribe(topic, Vec::new()).await?.split();

    // Look for members until gossip has enough neighbours, and only look again
    // if we lose all of them. Gossip maintains the swarm itself once it is in
    // it, so a lookup per member would be work nobody reads.
    let mut discovery = Some(discover(resolver.clone(), sender.clone(), me, infohash));

    let broadcast = async {
        let mut ticker = tokio::time::interval(BROADCAST_INTERVAL);
        for count in 0.. {
            ticker.tick().await;
            let message = format!("hi from {} (no. {count})", me.fmt_short());
            if let Err(err) = sender.broadcast(Bytes::from(message)).await {
                println!("stopping send loop: {err}");
                break;
            }
        }
    };

    let receive = async {
        while let Some(event) = receiver.try_next().await? {
            match event {
                Event::Received(message) => {
                    println!("message: {}", String::from_utf8_lossy(&message.content))
                }
                Event::NeighborUp(id) => {
                    println!("neighbour up: {}", id.fmt_short());
                    if receiver.neighbors().count() >= ENOUGH_NEIGHBORS {
                        println!("enough neighbors, stopping discovery");
                        // Dropping the handle aborts the lookup in flight.
                        discovery = None;
                    }
                }
                Event::NeighborDown(id) => {
                    println!("neighbour down: {}", id.fmt_short());
                    if !receiver.is_joined() {
                        println!("out of neighbors, restarting discovery");
                        discovery = Some(discover(resolver.clone(), sender.clone(), me, infohash));
                    }
                }
                Event::Lagged => println!("lagged, some messages were dropped"),
            }
        }
        n0_error::Ok(())
    };

    // The publisher keeps announcing in its own task until it is dropped.
    tokio::select! {
        result = broadcast => result,
        result = receive => result?,
        _ = tokio::signal::ctrl_c() => println!("interrupted"),
    }

    router.shutdown().await.anyerr()?;
    Ok(())
}

/// Hands every newly discovered member to gossip until the handle is dropped.
fn discover(
    resolver: Resolver,
    sender: GossipSender,
    me: EndpointId,
    infohash: Id,
) -> AbortOnDropHandle<()> {
    AbortOnDropHandle::new(task::spawn(async move {
        // Peers we saw before are offered again on a restart, since losing
        // every neighbour is exactly when a stale peer might be back.
        let mut known = HashSet::from([me]);
        let mut found = resolver.resolve_continuously(infohash);
        println!("discovering peers via mainline DHT...");
        while let Some(id) = found.next().await {
            if !known.insert(id) {
                continue;
            }
            println!("discovered {}", id.fmt_short());
            if sender.join_peers(vec![id]).await.is_err() {
                break;
            }
        }
    }))
}
