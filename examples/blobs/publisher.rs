//! Announces Mainline infohashes (`SHA-1`, 20 bytes) and publishes this
//! endpoint's public sockets on an addr → eid directory.
//!
//! The infohash is opaque: BLAKE3 content is `SHA-1(blake3)` at the call
//! site; gossip topics or other keys can use the same path.
//!
//! Public ip:port comes from iroh [`Endpoint::watch_addr`][watch]. Those ports
//! are used for `announce_peer`. No announce or directory publish until iroh
//! reports a public address and at least one infohash is registered.
//! Address changes are debounced, then those ports are used for `announce_peer`
//! and a directory publish. The same runs periodically so DHT entries do not
//! expire.
//!
//! [watch]: https://docs.rs/iroh/latest/iroh/struct.Endpoint.html#method.watch_addr

use std::{
    collections::HashSet,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{Mutex, PoisonError},
    time::Duration,
};

use anyhow::{Context, Result};
use iroh::{Endpoint, EndpointAddr, Watcher};
use iroh_endpoint_tracker::{Directory, MAX_ALPN_LEN, SignedRecord};
use n0_future::StreamExt;
use n0_mainline::{Dht, Id};
use tokio::sync::{Notify, watch};

/// How often to re-announce and republish while the public addr is unchanged.
pub const REFRESH: Duration = Duration::from_secs(10 * 60);

/// Wait this long after the last address change before announcing.
pub const DEBOUNCE: Duration = Duration::from_secs(2);
const MAINLINE_ADDR_POLL: Duration = Duration::from_secs(60);

/// Watches iroh public addresses and keeps Mainline + directory rows current.
pub struct Publisher {
    endpoint: Endpoint,
    dht: Dht,
    dir: Directory,
    infohashes: Mutex<HashSet<Id>>,
    alpns: Vec<Vec<u8>>,
    notify: Notify,
    published: watch::Sender<bool>,
}

impl Publisher {
    /// Observe an externally managed iroh endpoint and publish through the
    /// supplied shared Mainline node and tracker directory.
    ///
    /// The endpoint owner remains responsible for installing handlers for the
    /// announced ALPNs and for endpoint shutdown.
    pub async fn bind(
        endpoint: Endpoint,
        dht: Dht,
        dir: Directory,
        alpns: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> Result<Self> {
        let alpns: Vec<Vec<u8>> = alpns
            .into_iter()
            .map(|alpn| alpn.as_ref().to_vec())
            .collect();
        if alpns.is_empty() {
            anyhow::bail!("publisher requires at least one ALPN");
        }
        if alpns
            .iter()
            .any(|alpn| alpn.is_empty() || alpn.len() > MAX_ALPN_LEN)
        {
            anyhow::bail!("publisher ALPNs must contain 1..={MAX_ALPN_LEN} bytes");
        }
        Ok(Self {
            endpoint,
            dht,
            dir,
            infohashes: Mutex::new(HashSet::new()),
            alpns,
            notify: Notify::new(),
            published: watch::channel(false).0,
        })
    }

    /// The iroh endpoint (for address inspection).
    #[allow(dead_code)]
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Infohashes currently registered for announce.
    #[allow(dead_code)]
    pub fn infohashes(&self) -> Vec<Id> {
        self.infohashes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .copied()
            .collect()
    }

    /// Add a Mainline infohash to announce. Returns whether it was newly inserted.
    #[allow(dead_code)]
    pub fn add_infohash(&self, infohash: Id) -> bool {
        let inserted = self
            .infohashes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(infohash);
        if inserted {
            self.notify.notify_waiters();
        }
        inserted
    }

    /// Stop announcing an infohash. Returns whether it was present.
    ///
    /// Mainline entries expire on their own; this only stops further announces
    /// and, if the set is empty, further directory publishes.
    #[allow(dead_code)]
    pub fn remove_infohash(&self, infohash: &Id) -> bool {
        let removed = self
            .infohashes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(infohash);
        if removed {
            self.notify.notify_waiters();
        }
        removed
    }

    /// Wait until at least one Mainline announce + directory publish succeeded.
    pub async fn wait_published(&self) {
        let mut rx = self.published.subscribe();
        loop {
            if *rx.borrow() {
                return;
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    /// Watch addresses and keep announces + directory rows up to date until
    /// the endpoint closes.
    pub async fn run(&self) -> Result<()> {
        let mut addr_stream = self.endpoint.watch_addr().stream();
        let mut last_public: Vec<SocketAddrV4> = Vec::new();
        let mut mainline_ip = None;
        let mut debounce_at: Option<tokio::time::Instant> = None;
        let mut refresh = tokio::time::interval_at(tokio::time::Instant::now() + REFRESH, REFRESH);
        let mut mainline_poll = tokio::time::interval(MAINLINE_ADDR_POLL);
        loop {
            tokio::select! {
                _ = self.endpoint.closed() => break,
                _ = self.notify.notified() => {
                    if !last_public.is_empty() && mainline_ip.is_some() {
                        debounce_at = Some(tokio::time::Instant::now() + DEBOUNCE);
                    }
                }
                _ = refresh.tick() => {
                    if let Some(ip) = mainline_ip {
                        self.publish_and_announce(&last_public, ip).await?;
                    }
                }
                _ = sleep_until(debounce_at) => {
                    debounce_at = None;
                    if let Some(ip) = mainline_ip {
                        self.publish_and_announce(&last_public, ip).await?;
                    }
                }
                _ = mainline_poll.tick() => {
                    let own_id = *self.dht.info().await?.id();
                    self.dht.find_node(own_id).await?;
                    let observed = self.dht.info().await?.public_address().map(|addr| *addr.ip());
                    if observed != mainline_ip {
                        tracing::info!(?mainline_ip, ?observed, "Mainline public IP changed");
                        mainline_ip = observed;
                        if !last_public.is_empty() && mainline_ip.is_some() {
                            debounce_at = Some(tokio::time::Instant::now() + DEBOUNCE);
                        }
                    }
                }
                next = addr_stream.next() => {
                    let Some(ep_addr) = next else { break };
                    let public = public_v4s(&ep_addr);
                    if public == last_public {
                        continue;
                    }
                    last_public = public;
                    if last_public.is_empty() {
                        tracing::debug!("no public addr yet, not publishing");
                        debounce_at = None;
                        continue;
                    }
                    let own_id = *self.dht.info().await?.id();
                    self.dht.find_node(own_id).await?;
                    let observed = self.dht.info().await?.public_address().map(|addr| *addr.ip());
                    if observed != mainline_ip {
                        tracing::info!(?mainline_ip, ?observed, "Mainline public IP changed");
                        mainline_ip = observed;
                    }
                    if mainline_ip.is_some() {
                        debounce_at = Some(tokio::time::Instant::now() + DEBOUNCE);
                    }
                }
            }
        }
        Ok(())
    }

    async fn publish_and_announce(
        &self,
        public: &[SocketAddrV4],
        mainline_ip: Ipv4Addr,
    ) -> Result<()> {
        if public.is_empty() {
            return Ok(());
        }
        let infohashes: Vec<Id> = self
            .infohashes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .copied()
            .collect();
        if infohashes.is_empty() {
            tracing::debug!("no infohashes, not publishing");
            return Ok(());
        }
        let matching: Vec<_> = public
            .iter()
            .copied()
            .filter(|addr| *addr.ip() == mainline_ip)
            .collect();
        if matching.is_empty() {
            anyhow::bail!("Mainline sees public IP {mainline_ip}, but iroh advertises {public:?}");
        }
        let mut ports: Vec<u16> = matching.iter().map(|a| a.port()).collect();
        ports.sort();
        ports.dedup();
        for infohash in &infohashes {
            for port in &ports {
                self.dht
                    .announce_peer(*infohash, Some(*port))
                    .await
                    .with_context(|| format!("announce_peer port {port} infohash {infohash}"))?;
            }
        }
        let rec = SignedRecord::sign(
            self.endpoint.secret_key(),
            matching.clone(),
            self.alpns.clone(),
        );
        self.dir.publish(rec).await?;
        self.published.send_replace(true);
        tracing::info!(addrs = ?matching, n_infohashes = infohashes.len(), "published mapping");
        Ok(())
    }
}

fn public_v4s(addr: &EndpointAddr) -> Vec<SocketAddrV4> {
    let mut out: Vec<SocketAddrV4> = addr
        .ip_addrs()
        .filter_map(|a| match a {
            SocketAddr::V4(v4) if is_public_v4(*v4.ip()) => Some(*v4),
            _ => None,
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    !ip.is_unspecified()
        && !ip.is_loopback()
        && !ip.is_private()
        && !ip.is_link_local()
        && !ip.is_broadcast()
        && !ip.is_multicast()
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending::<()>().await,
    }
}
