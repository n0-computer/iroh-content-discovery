//! Periodically publish an endpoint identity and announce Mainline infohashes.

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddrV4,
    sync::{Arc, Mutex},
    time::Duration,
};

use iroh_base::{EndpointId, SecretKey};
use n0_error::{Result, StackResultExt, StdResultExt};
use n0_future::task::{self, AbortOnDropHandle};
use n0_mainline::{Dht, Id};
use tokio::{
    sync::{Notify, Semaphore, mpsc, watch},
    time::Instant,
};

use crate::AddrIndex;
use tracing::{info, warn};

/// How often to renew Mainline announcements and address-index values.
pub const REFRESH: Duration = Duration::from_secs(10 * 60);
/// Minimum spacing between announcement starts.
pub const ANNOUNCE_SPACING: Duration = Duration::from_millis(250);
/// Delay before retrying a failed publication or announcement.
pub const RETRY: Duration = Duration::from_secs(30);

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
    /// Creates a publisher from a secret key, a Mainline node, and an address index.
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

    /// Returns the endpoint identity published by this instance.
    pub fn id(&self) -> EndpointId {
        self.state.secret.public()
    }

    /// Returns the address index used by this publisher.
    pub fn index(&self) -> &AddrIndex {
        &self.state.index
    }

    /// Returns the most recently announced Mainline lookup key.
    pub fn public_v4(&self) -> Option<SocketAddrV4> {
        *self.state.published.borrow()
    }

    /// Returns the infohashes currently registered for announcement.
    pub fn infohashes(&self) -> Vec<Id> {
        self.state.infohashes()
    }

    /// Registers an infohash for announcement.
    ///
    /// Returns whether it was newly inserted.
    pub fn add_infohash(&self, infohash: Id) -> bool {
        let inserted = self
            .state
            .entries
            .lock()
            .expect("poisoned")
            .insert(infohash);
        if inserted {
            self.state.notify.notify_one();
        }
        inserted
    }

    /// Stops renewing an infohash.
    ///
    /// Returns whether it was registered.
    pub fn remove_infohash(&self, infohash: &Id) -> bool {
        let removed = self
            .state
            .entries
            .lock()
            .expect("poisoned")
            .remove(infohash);
        if removed {
            self.state.notify.notify_one();
        }
        removed
    }

    /// Waits until the index record and all currently registered hashes are published.
    ///
    /// After the first successful publication, subsequent calls return immediately,
    /// including after new infohashes are added.
    pub async fn wait_published(&self) {
        let mut receiver = self.state.published.subscribe();
        while receiver.borrow().is_none() && receiver.changed().await.is_ok() {}
    }
}

impl State {
    /// Runs independent announcement workers and an address-index refresh worker.
    async fn run(&self) {
        let (mapping_tx, mut mapping_rx) = watch::channel(None);
        let publish = async {
            loop {
                // Avoid publishing an unused endpoint before any hashes are added.
                if self.infohashes().is_empty() {
                    tokio::time::sleep(RETRY).await;
                    continue;
                }
                let delay = match self.publish_index().await {
                    Ok(mapping) => {
                        mapping_tx.send_replace(Some(mapping));
                        REFRESH
                    }
                    Err(err) => {
                        warn!(%err, "address-index publication failed");
                        RETRY
                    }
                };
                tokio::time::sleep(delay).await;
            }
        };
        // Start the index worker only once there is work, without polling latency.
        while self.infohashes().is_empty() {
            self.notify.notified().await;
        }
        tokio::pin!(publish);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut workers = HashMap::new();
        let mut announced = HashSet::new();
        let pacing = Arc::new(tokio::sync::Mutex::new(Instant::now()));
        let permits = Arc::new(Semaphore::new(3));
        loop {
            let entries: HashSet<_> = self.infohashes().into_iter().collect();
            workers.retain(|hash, _| entries.contains(hash));
            announced.retain(|hash| entries.contains(hash));
            for hash in entries.iter().copied() {
                workers.entry(hash).or_insert_with(|| {
                    let dht = self.dht.clone();
                    let mut mapping = mapping_rx.clone();
                    let tx = tx.clone();
                    let pacing = pacing.clone();
                    let permits = permits.clone();
                    AbortOnDropHandle::new(task::spawn(async move {
                        // The address must be indexed before advertising it in the DHT.
                        while mapping.borrow().is_none() {
                            if mapping.changed().await.is_err() {
                                return;
                            }
                        }
                        repeat_announcement(|| async {
                            let _permit = permits.acquire().await.expect("semaphore closed");
                            {
                                let mut next = pacing.lock().await;
                                tokio::time::sleep_until(*next).await;
                                *next = Instant::now() + ANNOUNCE_SPACING;
                            }
                            let result = announce(&dht, hash).await;
                            match &result {
                                Ok(()) => {
                                    let _ = tx.send(hash);
                                }
                                Err(err) => warn!(%hash, %err, "Mainline announcement failed"),
                            }
                            result.is_ok()
                        })
                        .await;
                    }))
                });
            }
            if !entries.is_empty() && entries.is_subset(&announced) {
                self.published.send_replace(*mapping_rx.borrow());
            }
            tokio::select! {
                _ = &mut publish => unreachable!("index publisher runs until cancelled"),
                _ = self.notify.notified() => {},
                _ = mapping_rx.changed() => {},
                Some(hash) = rx.recv() => { announced.insert(hash); },
            }
        }
    }

    /// Returns the registered infohashes, sorted.
    fn infohashes(&self) -> Vec<Id> {
        let mut entries: Vec<_> = self
            .entries
            .lock()
            .expect("poisoned")
            .iter()
            .copied()
            .collect();
        entries.sort();
        entries
    }

    async fn publish_index(&self) -> Result<SocketAddrV4> {
        self.index
            .publish(&self.secret)
            .await?
            .first()
            .copied()
            .std_context("address-index publish returned no public mapping")
    }
}

async fn announce(dht: &Dht, infohash: Id) -> Result<()> {
    // Prime token-bearing closest nodes before the announce PUT.
    dht.get_closest_nodes(infohash)
        .await
        .with_context(|_| format!("get_closest_nodes for {infohash}"))?;
    dht.announce_peer(infohash, None)
        .await
        .with_context(|_| format!("announce_peer infohash {infohash}"))?;
    info!(%infohash, "renewed Mainline announcement");
    Ok(())
}

/// Each worker owns its timer; neither another hash nor a mapping change resets it.
async fn repeat_announcement<F, Fut>(mut announce: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    loop {
        let delay = if announce().await {
            // Refresh slightly early with jitter to avoid synchronized renewals.
            REFRESH - Duration::from_secs(rand::random_range(0..=60))
        } else {
            RETRY
        };
        tokio::time::sleep(delay).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn new_hash_does_not_wait_for_existing_refresh_and_removal_stops_it() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let first_tx = tx.clone();
        let first = tokio::spawn(repeat_announcement(move || {
            first_tx.send(1).expect("receiver alive");
            async { true }
        }));
        assert_eq!(rx.recv().await, Some(1));
        tokio::time::advance(Duration::from_secs(100)).await;
        let second = tokio::spawn(repeat_announcement(move || {
            tx.send(2).expect("receiver alive");
            async { true }
        }));
        assert_eq!(rx.recv().await, Some(2));
        let started = Instant::now();
        assert_eq!(rx.recv().await, Some(1));
        assert!(started.elapsed() >= Duration::from_secs(440));
        assert!(started.elapsed() <= Duration::from_secs(500));
        first.abort();
        first.await.expect_err("worker cancelled");
        assert_eq!(rx.recv().await, Some(2));
        second.abort();
        second.await.expect_err("worker cancelled");
        tokio::time::advance(REFRESH * 2).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn failure_retries_only_the_failed_hash() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let failed_tx = tx.clone();
        let failed = tokio::spawn(repeat_announcement(move || {
            failed_tx.send(1).expect("receiver alive");
            async { false }
        }));
        assert_eq!(rx.recv().await, Some(1));
        let healthy = tokio::spawn(repeat_announcement(move || {
            tx.send(2).expect("receiver alive");
            async { true }
        }));
        assert_eq!(rx.recv().await, Some(2));
        let started = Instant::now();
        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(started.elapsed(), RETRY);
        failed.abort();
        healthy.abort();
    }
}
