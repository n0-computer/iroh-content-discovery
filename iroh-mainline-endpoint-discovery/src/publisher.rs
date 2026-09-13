//! Periodically publish an endpoint identity and announce Mainline infohashes.

use std::{
    collections::HashSet,
    net::SocketAddrV4,
    sync::{Mutex, PoisonError},
    time::Duration,
};

use anyhow::{Context, Result};
use iroh_base::{EndpointId, SecretKey};
use n0_mainline::{Dht, Id};
use tokio::sync::{Notify, watch};

use crate::{Directory, SignedRecord};

/// How often to renew Mainline announcements and address-index values.
pub const REFRESH: Duration = Duration::from_secs(10 * 60);
/// Delay between announcements after the public mapping changes.
pub const ANNOUNCE_SPACING: Duration = Duration::from_millis(250);

/// Keeps Mainline announcements and one signed endpoint value current.
pub struct Publisher {
    secret: SecretKey,
    dht: Dht,
    directory: Directory,
    entries: Mutex<HashSet<Id>>,
    notify: Notify,
    published: watch::Sender<Option<SocketAddrV4>>,
}

impl Publisher {
    /// Use an endpoint secret key, shared Mainline node, and address-index directory.
    pub fn new(secret: SecretKey, dht: Dht, directory: Directory) -> Self {
        Self {
            secret,
            dht,
            directory,
            entries: Mutex::new(HashSet::new()),
            notify: Notify::new(),
            published: watch::channel(None).0,
        }
    }

    /// Endpoint identity published by this instance.
    pub fn id(&self) -> EndpointId {
        self.secret.public()
    }

    /// Address-index directory used by this publisher.
    pub fn directory(&self) -> &Directory {
        &self.directory
    }

    /// Most recently announced Mainline lookup key.
    pub fn public_v4(&self) -> Option<SocketAddrV4> {
        *self.published.borrow()
    }

    /// Infohashes currently registered for announcement.
    pub fn infohashes(&self) -> Vec<Id> {
        let mut entries: Vec<_> = self
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .copied()
            .collect();
        entries.sort();
        entries
    }

    /// Register an infohash. Returns whether it was newly inserted.
    pub fn add_infohash(&self, infohash: Id) -> bool {
        let inserted = self
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(infohash);
        if inserted {
            self.notify.notify_one();
        }
        inserted
    }

    /// Stop renewing an infohash.
    pub fn remove_infohash(&self, infohash: &Id) -> bool {
        let removed = self
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(infohash);
        if removed {
            self.notify.notify_one();
        }
        removed
    }

    /// Wait until a Mainline announce and address-index put have succeeded.
    pub async fn wait_published(&self) {
        let mut receiver = self.published.subscribe();
        while receiver.borrow().is_none() && receiver.changed().await.is_ok() {}
    }

    /// Reconcile when hashes are added and periodically thereafter.
    pub async fn run(&self) -> Result<()> {
        let mut refresh = tokio::time::interval_at(tokio::time::Instant::now() + REFRESH, REFRESH);
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut mapping = None;
        loop {
            tokio::select! {
                _ = self.notify.notified() => self.reconcile(&mut mapping).await?,
                _ = refresh.tick() => self.reconcile(&mut mapping).await?,
            }
        }
    }

    async fn reconcile(&self, mapping: &mut Option<SocketAddrV4>) -> Result<()> {
        let entries = self.infohashes();
        if entries.is_empty() {
            return Ok(());
        }

        let record = SignedRecord::sign(&self.secret);
        let value_addrs = self.directory.publish(&record).await?;
        let next_mapping = value_addrs
            .first()
            .copied()
            .context("address-index publish returned no public mapping")?;
        let accelerated = *mapping != Some(next_mapping);

        if accelerated {
            *mapping = Some(next_mapping);
            self.published.send_replace(Some(next_mapping));
            tracing::info!(mapping = %next_mapping, "public UDP mapping changed");
        }

        let regular_spacing = REFRESH / entries.len() as u32;

        for (index, infohash) in entries.iter().copied().enumerate() {
            // Refresh public-address votes and prime token-bearing closest nodes
            // for the immediately following announce PUT.
            self.dht
                .get_closest_nodes(infohash)
                .await
                .with_context(|| format!("get_closest_nodes for {infohash}"))?;
            self.dht
                .announce_peer(infohash, None)
                .await
                .with_context(|| format!("announce_peer infohash {infohash}"))?;

            if index + 1 != entries.len() {
                tokio::time::sleep(if accelerated {
                    ANNOUNCE_SPACING
                } else {
                    regular_spacing
                })
                .await;
            }
        }
        tracing::info!(
            n_infohashes = entries.len(),
            "renewed Mainline announcements"
        );
        Ok(())
    }
}
