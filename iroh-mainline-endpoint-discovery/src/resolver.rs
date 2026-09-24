//! Mainline infohash → [`EndpointId`]s via `get_peers` and an addr → endpoint_id index.
//!
//! The infohash is opaque (`SHA-1`, 20 bytes). BLAKE3 content is hashed at
//! the call site (`SHA-1(blake3)`). Each compact `ip:port` is then resolved
//! through the index.

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
    /// The shared Mainline node used for lookups.
    pub fn dht(&self) -> &Dht {
        &self.dht
    }

    /// Use the supplied shared Mainline node and address index.
    pub async fn bind(dht: Dht, index: AddrIndex) -> Result<Self> {
        Ok(Self { dht, index })
    }

    /// Yield endpoint IDs as Mainline peers and index records arrive.
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

    /// Keep looking for providers until the consumer drops the stream.
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

    /// Return the first signed endpoint found, without resolving every peer.
    ///
    /// AddrIndex lookups are sequential and duplicate sockets are skipped.
    /// The caller should impose a deadline on this network operation.
    #[tracing::instrument(level = "debug", skip(self), fields(infohash = %infohash))]
    pub async fn resolve_one(&self, infohash: Id) -> Result<Option<EndpointId>> {
        let started = std::time::Instant::now();
        debug!("starting Mainline get_peers");
        let mut stream = self.dht.get_peers(infohash).await.context("get_peers")?;
        let mut peers = HashSet::new();
        let mut last_error = None;
        let mut any_ok = false;
        while let Some(batch) = stream.next().await {
            debug!(
                count = batch.len(),
                elapsed_ms = started.elapsed().as_millis(),
                "Mainline peers received"
            );
            for peer in batch {
                if !peers.insert(peer) {
                    continue;
                }
                debug!(%peer, "looking up signed endpoint in index server");
                match self.index.lookup(peer).await {
                    Ok(records) => {
                        debug!(%peer, count = records.len(), "index server endpoint records received");
                        any_ok = true;
                        if let Some(record) = records.into_iter().next() {
                            debug!(%peer, endpoint = %record.endpoint_id, elapsed_ms = started.elapsed().as_millis(), "selected content provider");
                            return Ok(Some(record.endpoint_id));
                        }
                    }
                    Err(error) => {
                        debug!(%peer, ?error, "index server endpoint lookup failed");
                        last_error = Some(error);
                    }
                }
            }
        }
        debug!(
            peers = peers.len(),
            elapsed_ms = started.elapsed().as_millis(),
            "Mainline lookup exhausted without a provider"
        );
        if !any_ok && let Some(error) = last_error {
            return Err(error.into());
        }
        Ok(None)
    }

    /// `get_peers` for `infohash`, then index-resolve each compact peer.
    ///
    /// Unique endpoint_ids, sorted. Empty if the DHT has no peers or none of them are
    /// in the index. Returned endpoint IDs are dialed through normal iroh
    /// discovery, not through the DHT address.
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
