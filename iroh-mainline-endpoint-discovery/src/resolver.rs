//! Mainline infohash → [`EndpointId`]s via `get_peers` and an addr → eid directory.
//!
//! The infohash is opaque (`SHA-1`, 20 bytes). BLAKE3 content is hashed at
//! the call site (`SHA-1(blake3)`). Each compact `ip:port` is then resolved
//! through the directory.

use std::collections::HashSet;

use anyhow::{Context, Result};
use iroh::EndpointId;
use n0_future::StreamExt;
use n0_mainline::{Dht, Id};
use tokio::task::JoinSet;

use crate::Directory;

/// Looks up who announced a Mainline infohash.
pub struct Resolver {
    dht: Dht,
    dir: Directory,
}

impl Resolver {
    /// Use the supplied shared Mainline node and address-index directory.
    pub async fn bind(dht: Dht, dir: Directory) -> Result<Self> {
        Ok(Self { dht, dir })
    }

    /// `get_peers` for `infohash`, then directory-resolve each compact peer.
    ///
    /// Unique eids, sorted. Empty if the DHT has no peers or none of them are
    /// in the directory. Does not probe sockets.
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
