//! Periodically announces Mainline infohashes and publishes the observed
//! Mainline socket as an addr → eid lookup key.

use std::{
    collections::{HashMap, hash_map::Entry},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{Mutex, PoisonError},
    time::Duration,
};

use anyhow::{Context, Result};
use iroh::{Endpoint, EndpointId};
use iroh_addr_index_proto::{MAX_ALPN_LEN, SignedRecord};
use n0_mainline::{Dht, Id};
use tokio::sync::{Notify, watch};

use crate::Directory;

/// How often to renew all Mainline announcements and the address-index row.
pub const REFRESH: Duration = Duration::from_secs(10 * 60);
/// Delay between announcement queries to avoid a burst.
pub const ANNOUNCE_SPACING: Duration = Duration::from_millis(250);

/// Keeps Mainline announcements and the address-index row current.
pub struct Publisher {
    endpoint: Endpoint,
    dht: Dht,
    dir: Directory,
    entries: Mutex<HashMap<Id, Vec<u8>>>,
    notify: Notify,
    published: watch::Sender<Option<SocketAddrV4>>,
}

impl Publisher {
    /// Use an externally managed endpoint, shared Mainline node, and directory.
    pub async fn bind(endpoint: Endpoint, dht: Dht, dir: Directory) -> Result<Self> {
        Ok(Self {
            endpoint,
            dht,
            dir,
            entries: Mutex::new(HashMap::new()),
            notify: Notify::new(),
            published: watch::channel(None).0,
        })
    }

    /// This publisher's endpoint id.
    #[allow(dead_code)]
    pub fn id(&self) -> EndpointId {
        self.endpoint.id()
    }

    /// Directory this publisher writes to.
    #[allow(dead_code)]
    pub fn directory(&self) -> &Directory {
        &self.dir
    }

    /// Most recently published Mainline lookup key.
    #[allow(dead_code)]
    pub async fn public_v4(&self) -> Vec<SocketAddrV4> {
        self.published.borrow().iter().copied().collect()
    }

    /// Infohashes currently registered for announcement.
    #[allow(dead_code)]
    pub fn infohashes(&self) -> Vec<Id> {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .copied()
            .collect()
    }

    /// Add an infohash and the ALPN used to probe its endpoint.
    #[allow(dead_code)]
    pub fn add_infohash(&self, infohash: Id, alpn: impl AsRef<[u8]>) -> Result<bool> {
        let alpn = alpn.as_ref();
        if alpn.is_empty() || alpn.len() > MAX_ALPN_LEN {
            anyhow::bail!("publisher ALPN must contain 1..={MAX_ALPN_LEN} bytes");
        }
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let inserted = match entries.entry(infohash) {
            Entry::Vacant(entry) => {
                entry.insert(alpn.to_vec());
                true
            }
            Entry::Occupied(entry) if entry.get().as_slice() == alpn => false,
            Entry::Occupied(_) => anyhow::bail!("infohash {infohash} already has another ALPN"),
        };
        drop(entries);
        if inserted {
            self.notify.notify_one();
        }
        Ok(inserted)
    }

    /// Stop renewing an infohash.
    #[allow(dead_code)]
    pub fn remove_infohash(&self, infohash: &Id) -> bool {
        let removed = self
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(infohash)
            .is_some();
        if removed {
            self.notify.notify_one();
        }
        removed
    }

    /// Wait until a Mainline announce and address-index publish have succeeded.
    pub async fn wait_published(&self) {
        let mut rx = self.published.subscribe();
        while rx.borrow().is_none() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }

    /// Reconcile when hashes are added and periodically thereafter.
    pub async fn run(&self) -> Result<()> {
        let mut refresh = tokio::time::interval_at(tokio::time::Instant::now() + REFRESH, REFRESH);
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut mapping = None;
        loop {
            tokio::select! {
                _ = self.endpoint.closed() => break,
                _ = self.notify.notified() => self.reconcile(&mut mapping).await?,
                _ = refresh.tick() => self.reconcile(&mut mapping).await?,
            }
        }
        Ok(())
    }

    async fn reconcile(&self, mapping: &mut Option<SocketAddrV4>) -> Result<()> {
        let mut entries: Vec<_> = self
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(id, alpn)| (*id, alpn.clone()))
            .collect();
        entries.sort_by_key(|(id, _)| *id);
        if entries.is_empty() {
            return Ok(());
        }
        let mut alpns: Vec<_> = entries.iter().map(|(_, alpn)| alpn.clone()).collect();
        alpns.sort();
        alpns.dedup();

        let mut index_renewed = false;
        let regular_spacing = REFRESH / entries.len() as u32;
        let mut accelerated = false;
        for (i, (infohash, _)) in entries.iter().enumerate() {
            let infohash = *infohash;
            // Refresh public-address votes and prime token-bearing closest nodes
            // for the immediately following announce PUT.
            self.dht
                .get_closest_nodes(infohash)
                .await
                .with_context(|| format!("get_closest_nodes for {infohash}"))?;
            let observed_ip = *self
                .dht
                .info()
                .await?
                .public_address()
                .context("Mainline has no observed public address")?
                .ip();

            let next_mapping = match *mapping {
                Some(current) if *current.ip() == observed_ip => current,
                _ => public_v4s(&self.endpoint.addr())
                    .into_iter()
                    .find(|addr| *addr.ip() == observed_ip)
                    .with_context(|| {
                        format!("Mainline sees {observed_ip}, but iroh advertises no matching IP")
                    })?,
            };

            if *mapping != Some(next_mapping) || !index_renewed {
                let changed = *mapping != Some(next_mapping);
                accelerated |= changed;
                let rec = SignedRecord::sign(
                    self.endpoint.secret_key(),
                    vec![next_mapping],
                    alpns.clone(),
                );
                self.dir.publish(rec).await?;
                self.published.send_replace(Some(next_mapping));
                *mapping = Some(next_mapping);
                index_renewed = true;
                tracing::info!(mapping = %next_mapping, changed, "published Mainline mapping");
            }

            self.dht
                .announce_peer(infohash, Some(next_mapping.port()))
                .await
                .with_context(|| format!("announce_peer infohash {infohash}"))?;

            if i + 1 != entries.len() {
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

fn public_v4s(addr: &iroh::EndpointAddr) -> Vec<SocketAddrV4> {
    let mut out: Vec<_> = addr
        .ip_addrs()
        .filter_map(|addr| match addr {
            SocketAddr::V4(addr) if is_public_v4(*addr.ip()) => Some(*addr),
            _ => None,
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_multicast())
}
