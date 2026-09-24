//! Periodically publish an endpoint identity and announce Mainline infohashes.

use std::{
    collections::HashSet,
    net::SocketAddrV4,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use iroh_base::{EndpointId, SecretKey};
use n0_error::{Result, StackResultExt, StdResultExt};
use n0_future::task::{self, AbortOnDropHandle};
use n0_mainline::{Dht, Id};
use tokio::sync::{Notify, watch};

use crate::AddrIndex;

/// How often to renew Mainline announcements and address-index values.
pub const REFRESH: Duration = Duration::from_secs(10 * 60);
/// Delay between announcements after the public mapping changes.
pub const ANNOUNCE_SPACING: Duration = Duration::from_millis(250);
/// Delay before retrying after a failed reconcile.
const RETRY: Duration = Duration::from_secs(30);

/// Keeps Mainline announcements and one signed endpoint value current.
///
/// Publishing runs in a background task owned by this handle. Dropping the
/// handle stops it, and the announcements expire from the DHT soon after.
#[derive(Debug, Clone)]
pub struct Publisher {
    state: Arc<State>,
    _task: Arc<AbortOnDropHandle<()>>,
}

#[derive(Debug)]
struct State {
    secret: SecretKey,
    dht: Dht,
    index: AddrIndex,
    entries: Mutex<HashSet<Id>>,
    notify: Notify,
    published: watch::Sender<Option<SocketAddrV4>>,
}

impl Publisher {
    /// Use an endpoint secret key, a shared Mainline node, and an address index.
    ///
    /// Publishing starts immediately, and does nothing until the first
    /// infohash is added.
    pub fn new(secret: SecretKey, dht: Dht, index: AddrIndex) -> Self {
        let state = Arc::new(State {
            secret,
            dht,
            index,
            entries: Mutex::new(HashSet::new()),
            notify: Notify::new(),
            published: watch::channel(None).0,
        });
        let task = task::spawn({
            let state = state.clone();
            async move { state.run().await }
        });
        Self {
            state,
            _task: Arc::new(AbortOnDropHandle::new(task)),
        }
    }

    /// Endpoint identity published by this instance.
    pub fn id(&self) -> EndpointId {
        self.state.secret.public()
    }

    /// Address-index index used by this publisher.
    pub fn index(&self) -> &AddrIndex {
        &self.state.index
    }

    /// Most recently announced Mainline lookup key.
    pub fn public_v4(&self) -> Option<SocketAddrV4> {
        *self.state.published.borrow()
    }

    /// Infohashes currently registered for announcement.
    pub fn infohashes(&self) -> Vec<Id> {
        self.state.infohashes()
    }

    /// Register an infohash. Returns whether it was newly inserted.
    pub fn add_infohash(&self, infohash: Id) -> bool {
        let inserted = self
            .state
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(infohash);
        if inserted {
            self.state.notify.notify_one();
        }
        inserted
    }

    /// Stop renewing an infohash.
    pub fn remove_infohash(&self, infohash: &Id) -> bool {
        let removed = self
            .state
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(infohash);
        if removed {
            self.state.notify.notify_one();
        }
        removed
    }

    /// Wait until the first successful publication round completes.
    ///
    /// A round stores the address-index record and announces every infohash
    /// captured at its start. Once a round succeeds, subsequent calls return
    /// immediately, including after new infohashes are added.
    pub async fn wait_published(&self) {
        let mut receiver = self.state.published.subscribe();
        while receiver.borrow().is_none() && receiver.changed().await.is_ok() {}
    }
}

impl State {
    /// Reconcile when hashes are added and periodically thereafter.
    ///
    /// A failed reconcile is retried after thirty seconds; the publisher is
    /// meant to keep the record alive without supervision.
    async fn run(&self) {
        let mut refresh = tokio::time::interval_at(tokio::time::Instant::now() + REFRESH, REFRESH);
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut mapping = None;
        loop {
            if let Err(err) = self.reconcile(&mut mapping).await {
                tracing::warn!(%err, "publishing failed");
                tokio::time::sleep(RETRY).await;
                continue;
            }
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = refresh.tick() => {}
            }
        }
    }

    /// Infohashes currently registered, sorted.
    fn infohashes(&self) -> Vec<Id> {
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

    async fn reconcile(&self, mapping: &mut Option<SocketAddrV4>) -> Result<()> {
        let entries = self.infohashes();
        if entries.is_empty() {
            return Ok(());
        }

        let value_addrs = self.index.publish(&self.secret).await?;
        let next_mapping = value_addrs
            .first()
            .copied()
            .std_context("address-index publish returned no public mapping")?;
        let accelerated = *mapping != Some(next_mapping);

        if accelerated {
            *mapping = Some(next_mapping);
            tracing::info!(mapping = %next_mapping, "public UDP mapping changed");
        }

        let regular_spacing = REFRESH / entries.len() as u32;

        for (index, infohash) in entries.iter().copied().enumerate() {
            // Refresh public-address votes and prime token-bearing closest nodes
            // for the immediately following announce PUT.
            self.dht
                .get_closest_nodes(infohash)
                .await
                .with_context(|_| format!("get_closest_nodes for {infohash}"))?;
            self.dht
                .announce_peer(infohash, None)
                .await
                .with_context(|_| format!("announce_peer infohash {infohash}"))?;

            if index + 1 != entries.len() {
                tokio::time::sleep(if accelerated {
                    ANNOUNCE_SPACING
                } else {
                    regular_spacing
                })
                .await;
            }
        }
        // Report the mapping only once every infohash is announced, so a
        // waiter that starts resolving does not race the announcements.
        self.published.send_replace(Some(next_mapping));
        tracing::info!(
            n_infohashes = entries.len(),
            "renewed Mainline announcements"
        );
        Ok(())
    }
}
