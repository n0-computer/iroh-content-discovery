//! Mainline infohash → [`EndpointId`]s via `get_peers` and an addr → eid directory.
//!
//! The infohash is opaque (`SHA-1`, 20 bytes). BLAKE3 content is hashed at
//! the call site (`SHA-1(blake3)`). Each compact `ip:port` is then resolved
//! through the directory.

use std::collections::{HashSet, VecDeque};

use anyhow::{Context, Result};
use iroh_base::EndpointId;
use n0_future::{FuturesUnordered, StreamExt, stream};
use n0_mainline::{Dht, Id};
use tokio::task::JoinSet;

use crate::Directory;

const MAX_INDEX_LOOKUPS: usize = 16;
const MAX_QUEUED_PEERS: usize = 64;

/// Looks up who announced a Mainline infohash.
#[derive(Debug, Clone)]
pub struct Resolver {
    dht: Dht,
    dir: Directory,
}

impl Resolver {
    /// Use the supplied shared Mainline node and address-index directory.
    pub async fn bind(dht: Dht, dir: Directory) -> Result<Self> {
        Ok(Self { dht, dir })
    }

    /// Yield endpoint IDs as Mainline peers and index records arrive.
    ///
    /// Up to 16 index lookups run concurrently, so a failed or slow peer does
    /// not delay other results. Lookup failures are logged and skipped.
    /// Dropping the stream cancels its pending lookups. The caller should
    /// impose a deadline.
    pub async fn resolve_stream(&self, infohash: Id) -> Result<stream::Boxed<EndpointId>> {
        let mut peers = self.dht.get_peers(infohash).await.context("get_peers")?;
        let directory = self.dir.clone();
        let stream = async_stream::stream! {
            let mut pending_peers = VecDeque::new();
            let mut lookups = FuturesUnordered::new();
            let mut peers_done = false;
            loop {
                while lookups.len() < MAX_INDEX_LOOKUPS {
                    let Some(peer) = pending_peers.pop_front() else { break };
                    let directory = directory.clone();
                    lookups.push(async move { (peer, directory.lookup(peer).await) });
                }
                if peers_done && pending_peers.is_empty() && lookups.is_empty() {
                    break;
                }
                let poll_peers = !peers_done && pending_peers.len() < MAX_QUEUED_PEERS;
                let poll_lookups = !lookups.is_empty();
                tokio::select! {
                    batch = peers.next(), if poll_peers => match batch {
                        Some(batch) => pending_peers.extend(batch),
                        None => peers_done = true,
                    },
                    result = lookups.next(), if poll_lookups => {
                        if let Some((peer, result)) = result {
                            match result {
                                Ok(records) => for record in records {
                                    yield record.eid;
                                },
                                Err(err) => tracing::debug!(%peer, %err, "directory resolve"),
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
    /// Consumers should set their own deadline for a bounded operation.
    pub fn resolve_continuously(&self, infohash: Id) -> stream::Boxed<EndpointId> {
        let resolver = self.clone();
        let stream = async_stream::stream! {
            loop {
                match resolver.resolve_stream(infohash).await {
                    Ok(mut found) => while let Some(id) = found.next().await {
                        yield id;
                    },
                    Err(err) => tracing::warn!(%infohash, %err, "Mainline provider lookup failed"),
                }
                tokio::task::yield_now().await;
            }
        };
        stream.boxed()
    }

    /// Return the first signed endpoint found, without resolving every peer.
    ///
    /// Directory lookups are sequential and duplicate sockets are skipped.
    /// The caller should impose a deadline on this network operation.
    pub async fn resolve_one(&self, infohash: Id) -> Result<Option<EndpointId>> {
        let mut stream = self.dht.get_peers(infohash).await.context("get_peers")?;
        let mut peers = HashSet::new();
        let mut last_error = None;
        let mut any_ok = false;
        while let Some(batch) = stream.next().await {
            for peer in batch {
                if !peers.insert(peer) {
                    continue;
                }
                match self.dir.lookup(peer).await {
                    Ok(records) => {
                        any_ok = true;
                        if let Some(record) = records.into_iter().next() {
                            return Ok(Some(record.eid));
                        }
                    }
                    Err(error) => last_error = Some(error),
                }
            }
        }
        if !any_ok && let Some(error) = last_error {
            return Err(error.into());
        }
        Ok(None)
    }

    /// `get_peers` for `infohash`, then directory-resolve each compact peer.
    ///
    /// Unique eids, sorted. Empty if the DHT has no peers or none of them are
    /// in the directory. Returned endpoint IDs are dialed through normal iroh
    /// discovery, not through the DHT address.
    pub async fn resolve(&self, infohash: Id) -> Result<Vec<EndpointId>> {
        let mut stream = self.dht.get_peers(infohash).await.context("get_peers")?;
        let mut peers = HashSet::new();
        while let Some(batch) = stream.next().await {
            peers.extend(batch);
        }
        tracing::debug!(n_peers = peers.len(), "get_peers");

        let mut set = JoinSet::new();
        for peer in peers {
            let dir = self.dir.clone();
            set.spawn(async move { (peer, dir.lookup(peer).await) });
        }

        let mut eids = HashSet::new();
        let mut any_ok = false;
        let mut last_err = None;
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((_, Ok(records))) => {
                    any_ok = true;
                    eids.extend(records.into_iter().map(|r| r.eid));
                }
                Ok((peer, Err(err))) => {
                    tracing::debug!(%peer, %err, "directory resolve");
                    last_err = Some(err);
                }
                Err(err) => tracing::debug!(%err, "directory resolve join"),
            }
        }
        if !any_ok && let Some(err) = last_err {
            return Err(err.into());
        }
        let mut out: Vec<_> = eids.into_iter().collect();
        out.sort();
        Ok(out)
    }
}
