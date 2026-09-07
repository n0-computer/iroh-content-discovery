//! Mapping probes and directory server state.

use std::{
    net::{IpAddr, SocketAddrV4},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use iroh::{
    Endpoint, EndpointAddr, EndpointId,
    endpoint::{ConnectOptions, Connection},
    protocol::{AcceptError, ProtocolHandler},
};
use iroh_addr_index_proto::{Alpn, SignedRecord};
use tokio::task::JoinSet;
use tracing::{debug, trace};

use crate::store::{Limits, PublishError, RateLimiters, Store};

/// Dedicated mapping-probe ALPN. Handshake success is enough; no payload.
pub const PROBE_ALPN: &[u8] = b"/iroh-addr-index/probe/0";

/// Default budget for one mapping-probe connect.
pub const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Accepts probe connections and closes them.
#[derive(Debug, Clone, Default)]
pub struct ProbeAccept;

impl ProtocolHandler for ProbeAccept {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        conn.close(0u32.into(), b"probe");
        conn.closed().await;
        Ok(())
    }
}

/// Dial `addr` as `eid` offering `alpns`. True only if TLS authenticates as `eid`.
///
/// The destination contains solely this IP socket (no relay). Empty `alpns`
/// cannot complete a handshake.
pub async fn confirm_socket(
    endpoint: &Endpoint,
    eid: EndpointId,
    addr: SocketAddrV4,
    alpns: impl IntoIterator<Item = impl AsRef<[u8]>>,
    timeout: Duration,
) -> bool {
    let alpns: Vec<Alpn> = alpns
        .into_iter()
        .map(|a| Alpn::from_slice(a.as_ref()))
        .filter(|a| !a.is_empty())
        .collect();
    confirm_socket_inner(endpoint, eid, addr, &alpns, timeout).await
}

async fn confirm_socket_inner(
    endpoint: &Endpoint,
    eid: EndpointId,
    addr: SocketAddrV4,
    alpns: &[Alpn],
    timeout: Duration,
) -> bool {
    let Some((first, rest)) = alpns.split_first() else {
        debug!(%eid, %addr, "probe has no ALPNs");
        return false;
    };
    if eid == endpoint.id() {
        return endpoint.addr().ip_addrs().any(|a| *a == addr.into());
    }
    let dest = EndpointAddr::new(eid).with_ip_addr(addr.into());
    let opts = ConnectOptions::new()
        .with_additional_alpns(rest.iter().map(|alpn| alpn.to_vec()).collect());
    let ok = async {
        let connecting = endpoint.connect_with_opts(dest, first, opts).await.ok()?;
        let conn = connecting.await.ok()?;
        let match_id = conn.remote_id() == eid;
        conn.close(0u32.into(), b"probe");
        Some(match_id)
    };
    match tokio::time::timeout(timeout, ok).await {
        Ok(Some(true)) => true,
        Ok(other) => {
            debug!(%eid, %addr, ?other, "probe failed");
            false
        }
        Err(_) => {
            debug!(%eid, %addr, "probe timed out");
            false
        }
    }
}

/// Keep rows whose eid answers on `query_addr` using the ALPNs they announced.
pub async fn confirm_records(
    endpoint: &Endpoint,
    query_addr: SocketAddrV4,
    records: impl IntoIterator<Item = SignedRecord>,
    timeout: Duration,
) -> Vec<SignedRecord> {
    let records: Vec<_> = records
        .into_iter()
        .filter(|r| r.verify_sig().is_ok() && r.covers_addr(query_addr))
        .collect();
    let mut set = JoinSet::new();
    for rec in records {
        let ep = endpoint.clone();
        set.spawn(async move {
            let ok = confirm_socket_inner(&ep, rec.eid, query_addr, &rec.alpns, timeout).await;
            (rec, ok)
        });
    }
    let mut out = Vec::new();
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((rec, true)) => out.push(rec),
            Ok((_, false)) => {}
            Err(_) => {}
        }
    }
    out.sort_by_key(|r| r.eid);
    out
}

/// UDP directory replica over an in-memory [`Store`].
#[derive(Debug, Clone)]
pub struct Server {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    store: Mutex<Store>,
    limits: Limits,
    rate: Mutex<RateLimiters>,
    probe: Mutex<Option<Endpoint>>,
}

impl Server {
    /// New replica with the given caps.
    pub fn new(limits: Limits) -> Self {
        Self {
            inner: Arc::new(Inner {
                store: Mutex::new(Store::new()),
                limits,
                rate: Mutex::new(RateLimiters::default()),
                probe: Mutex::new(None),
            }),
        }
    }

    /// Caps used by this replica.
    pub fn limits(&self) -> &Limits {
        &self.inner.limits
    }

    /// Probe on contention: first eid for an addr is stored as-is; a later eid
    /// must answer on a claimed socket or the publish is dropped.
    pub fn set_probe(&self, endpoint: Endpoint) {
        *self.inner.probe.lock().expect("poisoned") = Some(endpoint);
    }

    /// Publish locally (no RPC). Does not probe.
    pub fn publish_local(&self, record: SignedRecord) -> Result<(), PublishError> {
        let now = unix_secs();
        self.inner
            .store
            .lock()
            .expect("poisoned")
            .publish(record, now, &self.inner.limits)
    }

    /// Publish. With [`Self::set_probe`], a second eid for an occupied addr is
    /// probed: if it answers it replaces the previous occupant, otherwise it
    /// is not stored.
    pub async fn publish_confirmed_local(&self, record: SignedRecord) -> Result<(), PublishError> {
        let Some(ep) = self.inner.probe.lock().expect("poisoned").clone() else {
            return self.publish_local(record);
        };
        let now = unix_secs();
        // Do all attacker-controlled record validation before an expensive
        // probe and, critically, before removing any existing rows.
        self.inner
            .store
            .lock()
            .expect("poisoned")
            .validate_publish(&record, now, &self.inner.limits)?;
        let contended: Vec<EndpointId> = {
            let mut store = self.inner.store.lock().expect("poisoned");
            let mut ids = Vec::new();
            for addr in &record.addrs {
                for existing in store.lookup(*addr, now) {
                    if existing.eid != record.eid && !ids.contains(&existing.eid) {
                        ids.push(existing.eid);
                    }
                }
            }
            ids
        };
        if contended.is_empty() {
            let eid = record.eid;
            let res = self.publish_local(record);
            match &res {
                Ok(()) => trace!(%eid, "publish stored (no contention)"),
                Err(err) => trace!(%eid, %err, "publish not stored"),
            }
            return res;
        }
        let mut ok = false;
        for addr in &record.addrs {
            if confirm_socket(&ep, record.eid, *addr, &record.alpns, DEFAULT_PROBE_TIMEOUT).await {
                ok = true;
                break;
            }
        }
        if !ok {
            trace!(
                eid = %record.eid,
                addrs = ?record.addrs,
                "publish not stored: spoof or unreachable"
            );
            return Err(PublishError::ProbeFailed);
        }
        let eid = record.eid;
        let res = self
            .inner
            .store
            .lock()
            .expect("poisoned")
            .publish_replacing_validated(record, &self.inner.limits, now);
        if let Ok(replaced) = &res {
            for replaced in replaced {
                trace!(%replaced, by = %eid, "replaced occupant");
            }
            trace!(%eid, "publish stored (replaced occupants)");
        }
        res.map(|_| ())
    }

    /// Lookup locally (no RPC). Does not probe.
    pub fn lookup_local(&self, addr: SocketAddrV4) -> Vec<SignedRecord> {
        let now = unix_secs();
        self.inner.store.lock().expect("poisoned").lookup(addr, now)
    }

    pub(crate) fn allow_udp_publish(&self, record_eid: EndpointId, ip: IpAddr) -> bool {
        self.inner.rate.lock().expect("poisoned").allow_udp_publish(
            &self.inner.limits,
            record_eid,
            ip,
        )
    }

    pub(crate) fn verify_udp_source_ip(&self) -> bool {
        self.inner.limits.verify_udp_source_ip
    }

    pub(crate) fn allow_udp(&self, ip: IpAddr) -> bool {
        self.inner
            .rate
            .lock()
            .expect("poisoned")
            .allow_udp(&self.inner.limits, ip)
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new(Limits::default())
    }
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}
