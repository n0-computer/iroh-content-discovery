//! Resolution of a Mainline infohash to [`EndpointId`]s.
//!
//! Peers come from `get_peers`, and each peer's address is looked up in the
//! address index to recover the endpoint identity that published it. The
//! infohash is opaque to this crate: BLAKE3 content is hashed to `SHA-1` at
//! the call site.

use std::collections::{HashSet, VecDeque};

use iroh_base::EndpointId;
use n0_error::{Result, StackResultExt};
use n0_future::{FuturesUnordered, StreamExt, stream};
use n0_mainline::{Dht, Id};
use tokio::task::JoinSet;

use crate::AddrIndex;
use tracing::debug;

const MAX_INDEX_LOOKUPS: usize = 16;
const MAX_QUEUED_PEERS: usize = 64;

/// Looks up who announced a Mainline infohash.
#[derive(Debug, Clone)]
pub struct Resolver {
    dht: Dht,
    index: AddrIndex,
}

impl Resolver {
    /// Returns the shared Mainline node used for lookups.
    pub fn dht(&self) -> &Dht {
        &self.dht
    }

    /// Creates a resolver from a shared Mainline node and an address index.
    pub fn new(dht: Dht, index: AddrIndex) -> Self {
        Self { dht, index }
    }

    /// Yields endpoint IDs as Mainline peers and index records arrive.
    ///
    /// Up to 16 index lookups run concurrently, so a failed or slow peer does
    /// not delay other results. Lookup failures are logged and skipped.
    /// Dropping the stream cancels its pending lookups. The caller should
    /// impose a deadline.
    pub async fn resolve_stream(&self, infohash: Id) -> Result<stream::Boxed<EndpointId>> {
        debug!(%infohash, "starting Mainline provider stream");
        let mut peers = self.dht.get_peers(infohash).await.context("get_peers")?;
        let index = self.index.clone();
        let stream = async_stream::stream! {
            let mut pending_peers = VecDeque::new();
            let mut lookups = FuturesUnordered::new();
            let mut peers_done = false;
            loop {
                while lookups.len() < MAX_INDEX_LOOKUPS {
                    let Some(peer) = pending_peers.pop_front() else { break };
                    let index = index.clone();
                    lookups.push(async move { (peer, index.lookup(peer).await) });
                }
                if peers_done && pending_peers.is_empty() && lookups.is_empty() {
                    break;
                }
                let poll_peers = !peers_done && pending_peers.len() < MAX_QUEUED_PEERS;
                let poll_lookups = !lookups.is_empty();
                tokio::select! {
                    batch = peers.next(), if poll_peers => match batch {
                        Some(batch) => {
                            debug!(%infohash, count = batch.len(), "Mainline peers received");
                            pending_peers.extend(batch);
                        },
                        None => peers_done = true,
                    },
                    result = lookups.next(), if poll_lookups => {
                        if let Some((peer, result)) = result {
                            match result {
                                Ok(records) => for record in records {
                                    debug!(%infohash, %peer, endpoint = %record.endpoint_id, "discovered content provider");
                                    yield record.endpoint_id;
                                },
                                Err(err) => debug!(%peer, %err, "index resolve"),
                            }
                        }
                    }
                }
            }
        };
        Ok(stream.boxed())
    }

    /// Keeps looking for providers until the consumer drops the stream.
    ///
    /// A new Mainline lookup starts when the consumer asks for another item
    /// after the previous lookup ends. Results may include duplicate IDs.
    /// The stream ends when the Mainline node is gone, which is the only
    /// reason a lookup cannot start. Consumers should set their own deadline
    /// for a bounded operation.
    pub fn resolve_continuously(&self, infohash: Id) -> stream::Boxed<EndpointId> {
        let resolver = self.clone();
        let stream = async_stream::stream! {
            loop {
                // A lookup only fails to start once the Mainline node is gone,
                // and it does not come back, so retrying would spin this loop
                // at full speed forever.
                let Ok(mut found) = resolver.resolve_stream(infohash).await.inspect_err(|err| {
                    debug!(%infohash, %err, "Mainline node is gone, ending provider stream");
                }) else {
                    break;
                };
                while let Some(id) = found.next().await {
                    yield id;
                }
                tokio::task::yield_now().await;
            }
        };
        stream.boxed()
    }

    /// Runs `get_peers` for `infohash`, then index-resolves each compact peer.
    ///
    /// Returns unique endpoint IDs, sorted, and nothing at all if the DHT has
    /// no peers or none of them are in the index. Returned endpoint IDs are
    /// dialed through normal iroh discovery, not through the DHT address.
    pub async fn resolve(&self, infohash: Id) -> Result<Vec<EndpointId>> {
        let mut stream = self.dht.get_peers(infohash).await.context("get_peers")?;
        let mut peers = HashSet::new();
        while let Some(batch) = stream.next().await {
            peers.extend(batch);
        }
        debug!(n_peers = peers.len(), "get_peers");

        let mut set = JoinSet::new();
        for peer in peers {
            let index = self.index.clone();
            set.spawn(async move { (peer, index.lookup(peer).await) });
        }

        let mut endpoint_ids = HashSet::new();
        let mut any_ok = false;
        let mut last_err = None;
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((_, Ok(records))) => {
                    any_ok = true;
                    endpoint_ids.extend(records.into_iter().map(|r| r.endpoint_id));
                }
                Ok((peer, Err(err))) => {
                    debug!(%peer, %err, "index resolve");
                    last_err = Some(err);
                }
                Err(err) => debug!(%err, "index resolve join"),
            }
        }
        if !any_ok && let Some(err) = last_err {
            return Err(err.into());
        }
        let mut out: Vec<_> = endpoint_ids.into_iter().collect();
        out.sort();
        Ok(out)
    }
}
