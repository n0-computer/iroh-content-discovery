//! In-memory reverse index: `ip:port` → live [`EndpointId`].

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    net::{IpAddr, SocketAddrV4},
    time::{Duration, Instant},
};

use iroh::EndpointId;
use iroh_addr_index_proto::{Index, RecordLimits, SignedRecord, VerifyError};
use serde::{Deserialize, Serialize};

const MAX_RATE_LIMIT_BUCKETS: usize = 65_536;

/// Store and UDP request limits.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Require a UDP publish's source IP to occur in its record (default true).
    pub verify_udp_source_ip: bool,
    /// Maximum addresses in a single record (default 16).
    pub max_addrs: usize,
    /// Maximum ALPNs in a single record (default 8).
    pub max_alpns: usize,
    /// How long accepted records are retained (default 60 minutes).
    pub record_ttl_secs: u64,
    /// How far in the future `ts` may be, in seconds (default 60).
    pub clock_skew_secs: u64,
    /// Publish token rate per record eid (default 1/s).
    pub publish_per_eid_per_sec: f64,
    /// Publish burst per record eid (default 4).
    pub publish_burst: f64,
    /// Request token rate per source IP (default 50/s).
    pub connect_per_sec: f64,
    /// Request burst per source IP (default 100).
    pub connect_burst: f64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            verify_udp_source_ip: true,
            max_addrs: 16,
            max_alpns: 8,
            record_ttl_secs: 60 * 60,
            clock_skew_secs: 60,
            publish_per_eid_per_sec: 1.0,
            publish_burst: 4.0,
            connect_per_sec: 50.0,
            connect_burst: 100.0,
        }
    }
}

impl Limits {
    /// Loose limits for unit tests.
    pub fn for_tests() -> Self {
        Self {
            verify_udp_source_ip: false,
            record_ttl_secs: 24 * 60 * 60,
            clock_skew_secs: 0,
            publish_per_eid_per_sec: 1_000.0,
            publish_burst: 1_000.0,
            connect_per_sec: 1_000.0,
            connect_burst: 1_000.0,
            ..Self::default()
        }
    }
}

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

impl Bucket {
    fn try_take(&mut self, now: Instant, rate: f64, burst: f64) -> bool {
        let dt = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + dt * rate).min(burst);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn stale(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.last) > Duration::from_secs(600)
    }
}

#[derive(Debug, Default)]
pub(crate) struct RateLimiters {
    by_eid: HashMap<EndpointId, Bucket>,
    by_ip: HashMap<IpAddr, Bucket>,
    last_gc: Option<Instant>,
}

impl RateLimiters {
    pub(crate) fn allow_udp_publish(
        &mut self,
        limits: &Limits,
        record_eid: EndpointId,
        ip: IpAddr,
    ) -> bool {
        let now = Instant::now();
        self.gc(now);
        if !take(
            &mut self.by_ip,
            ip,
            now,
            limits.connect_per_sec,
            limits.connect_burst,
        ) {
            return false;
        }
        take(
            &mut self.by_eid,
            record_eid,
            now,
            limits.publish_per_eid_per_sec,
            limits.publish_burst,
        )
    }

    pub(crate) fn allow_udp(&mut self, limits: &Limits, ip: IpAddr) -> bool {
        let now = Instant::now();
        self.gc(now);
        take(
            &mut self.by_ip,
            ip,
            now,
            limits.connect_per_sec,
            limits.connect_burst,
        )
    }

    fn gc(&mut self, now: Instant) {
        if self
            .last_gc
            .is_some_and(|last| now.saturating_duration_since(last) < Duration::from_secs(60))
        {
            return;
        }
        self.last_gc = Some(now);
        self.by_eid.retain(|_, b| !b.stale(now));
        self.by_ip.retain(|_, b| !b.stale(now));
    }
}

fn take<K: Eq + Hash>(
    map: &mut HashMap<K, Bucket>,
    key: K,
    now: Instant,
    rate: f64,
    burst: f64,
) -> bool {
    if !map.contains_key(&key) && map.len() >= MAX_RATE_LIMIT_BUCKETS {
        return false;
    }
    map.entry(key)
        .or_insert(Bucket {
            tokens: burst,
            last: now,
        })
        .try_take(now, rate, burst)
}

/// Rejection of a [`Store::publish`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum PublishError {
    /// Signature or field checks failed.
    Verify(VerifyError),
    /// This ip:port is currently assigned to another eid.
    AddrContended,
    /// Incoming `ts` is older than the row we already have.
    Stale,
    /// Publisher or connecting peer exceeded the rate limit.
    RateLimited,
    /// Row does not permit reverse indexing (`addr` → eid).
    IndexNotAllowed,
    /// Claimed sockets did not answer a connect as this eid.
    ProbeFailed,
}

impl std::fmt::Display for PublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Verify(e) => write!(f, "{e}"),
            Self::AddrContended => write!(f, "address is assigned to another endpoint id"),
            Self::Stale => write!(f, "stale record (older than stored row)"),
            Self::RateLimited => write!(f, "rate limited"),
            Self::IndexNotAllowed => write!(f, "record does not permit reverse indexing"),
            Self::ProbeFailed => write!(f, "claimed socket did not answer"),
        }
    }
}

impl std::error::Error for PublishError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Verify(e) => Some(e),
            _ => None,
        }
    }
}

impl From<VerifyError> for PublishError {
    fn from(value: VerifyError) -> Self {
        Self::Verify(value)
    }
}

/// In-memory directory state.
#[derive(Debug, Default)]
pub struct Store {
    by_eid: HashMap<EndpointId, SignedRecord>,
    by_addr: HashMap<SocketAddrV4, EndpointId>,
    expires_at: HashMap<EndpointId, u64>,
    last_gc: Option<u64>,
}

impl Store {
    /// Empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of retained endpoint rows.
    ///
    /// Expired rows may remain until the next publish, lookup, or explicit
    /// [`Store::gc`] call.
    pub fn len(&self) -> usize {
        self.by_eid.len()
    }

    /// Whether the store retains no rows.
    ///
    /// As with [`Store::len`], expiry collection is activity-driven.
    pub fn is_empty(&self) -> bool {
        self.by_eid.is_empty()
    }

    /// Upsert `record` as the sole row for its eid.
    pub fn publish(
        &mut self,
        record: SignedRecord,
        now: u64,
        limits: &Limits,
    ) -> Result<(), PublishError> {
        self.maintain(now);
        self.validate_publish(&record, now, limits)?;

        for addr in &record.addrs {
            if self
                .by_addr
                .get(addr)
                .is_some_and(|existing| *existing != record.eid)
            {
                return Err(PublishError::AddrContended);
            }
        }

        if let Some(prev) = self.by_eid.remove(&record.eid) {
            self.unindex(prev.eid, &prev.addrs);
        }
        let eid = record.eid;
        self.index(eid, &record.addrs);
        self.by_eid.insert(eid, record);
        self.expires_at
            .insert(eid, now.saturating_add(limits.record_ttl_secs));
        Ok(())
    }

    pub(crate) fn validate_publish(
        &self,
        record: &SignedRecord,
        now: u64,
        limits: &Limits,
    ) -> Result<(), PublishError> {
        record.verify_at(
            now,
            RecordLimits {
                max_addrs: limits.max_addrs,
                max_alpns: limits.max_alpns,
                clock_skew_secs: limits.clock_skew_secs,
            },
        )?;
        if !record.index.contains(&Index::Reverse) {
            return Err(PublishError::IndexNotAllowed);
        }
        self.validate_version(record)
    }

    fn validate_version(&self, record: &SignedRecord) -> Result<(), PublishError> {
        if let Some(prev) = self.by_eid.get(&record.eid)
            && (prev.ts > record.ts || (prev.ts == record.ts && prev != record))
        {
            return Err(PublishError::Stale);
        }
        Ok(())
    }

    /// Commit a previously validated, successfully probed row, replacing all
    /// current occupants of its addresses atomically.
    pub(crate) fn publish_replacing_validated(
        &mut self,
        record: SignedRecord,
        limits: &Limits,
        now: u64,
    ) -> Result<Vec<EndpointId>, PublishError> {
        // The store may have changed while the probe was in flight.
        self.validate_version(&record)?;
        let mut replaced = HashSet::new();
        for addr in &record.addrs {
            if let Some(eid) = self.by_addr.get(addr)
                && *eid != record.eid
            {
                replaced.insert(*eid);
            }
        }

        let mut replaced: Vec<_> = replaced.into_iter().collect();
        replaced.sort();
        for eid in &replaced {
            self.remove(*eid);
        }
        if let Some(prev) = self.by_eid.remove(&record.eid) {
            self.unindex(prev.eid, &prev.addrs);
        }
        let eid = record.eid;
        self.index(eid, &record.addrs);
        self.by_eid.insert(eid, record);
        self.expires_at
            .insert(eid, now.saturating_add(limits.record_ttl_secs));
        Ok(replaced)
    }

    /// Drop the row for `eid`, if any.
    pub(crate) fn remove(&mut self, eid: EndpointId) -> Option<SignedRecord> {
        let prev = self.by_eid.remove(&eid)?;
        self.expires_at.remove(&eid);
        self.unindex(prev.eid, &prev.addrs);
        Some(prev)
    }

    /// All non-expired eids that listed this exact ip:port.
    pub fn lookup(&mut self, addr: SocketAddrV4, now: u64) -> Vec<SignedRecord> {
        self.maintain(now);
        let eids: Vec<EndpointId> = self.by_addr.get(&addr).copied().into_iter().collect();
        self.collect_live(eids, now)
    }

    /// Drop expired rows.
    pub fn gc(&mut self, now: u64) {
        self.last_gc = Some(now);
        let expired: Vec<EndpointId> = self
            .by_eid
            .iter()
            .filter(|(eid, _)| self.expires_at.get(*eid).is_none_or(|exp| *exp <= now))
            .map(|(eid, _)| *eid)
            .collect();
        for eid in expired {
            self.remove(eid);
        }
    }

    fn maintain(&mut self, now: u64) {
        const GC_INTERVAL_SECS: u64 = 60;
        if self
            .last_gc
            .is_none_or(|last| now.saturating_sub(last) >= GC_INTERVAL_SECS)
        {
            self.gc(now);
        }
    }

    fn collect_live(&mut self, eids: Vec<EndpointId>, now: u64) -> Vec<SignedRecord> {
        let mut out = Vec::new();
        let mut expired = Vec::new();
        for eid in eids {
            match self.by_eid.get(&eid) {
                Some(rec) if self.expires_at.get(&eid).is_some_and(|exp| *exp > now) => {
                    out.push(rec.clone())
                }
                Some(_) => expired.push(eid),
                None => {}
            }
        }
        for eid in expired {
            self.remove(eid);
        }
        out.sort_by_key(|r| r.eid);
        out
    }

    fn index(&mut self, eid: EndpointId, addrs: &[SocketAddrV4]) {
        for addr in addrs {
            let previous = self.by_addr.insert(*addr, eid);
            debug_assert!(previous.is_none_or(|previous| previous == eid));
        }
    }

    fn unindex(&mut self, eid: EndpointId, addrs: &[SocketAddrV4]) {
        for addr in addrs {
            if self.by_addr.get(addr) == Some(&eid) {
                self.by_addr.remove(addr);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use iroh::SecretKey;

    use super::*;

    fn rec(sk: &SecretKey, addr: &str, ts: u64, _exp: u64) -> SignedRecord {
        SignedRecord::sign_at(
            sk,
            vec![addr.parse().unwrap()],
            [b"test/0"],
            [Index::Reverse],
            ts,
        )
    }

    #[test]
    fn upsert_replaces_same_eid_only() {
        let limits = Limits::for_tests();
        let mut store = Store::new();
        let a = SecretKey::generate();
        let b = SecretKey::generate();
        store
            .publish(rec(&a, "1.2.3.4:6881", 10, 1000), 10, &limits)
            .unwrap();
        store
            .publish(rec(&b, "1.2.3.4:6882", 10, 1000), 10, &limits)
            .unwrap();
        store
            .publish(rec(&a, "1.2.3.4:9999", 11, 1000), 11, &limits)
            .unwrap();
        assert!(store.lookup("1.2.3.4:6881".parse().unwrap(), 12).is_empty());
        assert_eq!(store.lookup("1.2.3.4:6882".parse().unwrap(), 12).len(), 1);
        assert_eq!(store.lookup("1.2.3.4:9999".parse().unwrap(), 12).len(), 1);
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn unconfirmed_publish_rejects_contention() {
        let limits = Limits::for_tests();
        let mut store = Store::new();
        let addr = "1.2.3.4:6881";
        store
            .publish(rec(&SecretKey::generate(), addr, 10, 1000), 10, &limits)
            .unwrap();
        let err = store
            .publish(rec(&SecretKey::generate(), addr, 10, 1000), 10, &limits)
            .unwrap_err();
        assert_eq!(err, PublishError::AddrContended);
        assert_eq!(store.lookup(addr.parse().unwrap(), 11).len(), 1);
    }

    #[test]
    fn existing_eid_can_refresh() {
        let limits = Limits::for_tests();
        let mut store = Store::new();
        let a = SecretKey::generate();
        let addr = "1.2.3.4:6881";
        store.publish(rec(&a, addr, 10, 1000), 10, &limits).unwrap();
        store.publish(rec(&a, addr, 20, 2000), 20, &limits).unwrap();
        let hits = store.lookup(addr.parse().unwrap(), 21);
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|r| r.eid == a.public() && r.ts == 20));
    }

    #[test]
    fn stale_publish_rejected() {
        let limits = Limits::for_tests();
        let mut store = Store::new();
        let a = SecretKey::generate();
        store
            .publish(rec(&a, "1.2.3.4:1", 20, 1000), 20, &limits)
            .unwrap();
        let err = store
            .publish(rec(&a, "1.2.3.4:1", 10, 1000), 20, &limits)
            .unwrap_err();
        assert_eq!(err, PublishError::Stale);
    }

    #[test]
    fn different_record_with_equal_timestamp_is_rejected() {
        let limits = Limits::for_tests();
        let mut store = Store::new();
        let key = SecretKey::generate();
        store
            .publish(rec(&key, "1.2.3.4:1", 20, 1000), 20, &limits)
            .unwrap();
        let err = store
            .publish(rec(&key, "1.2.3.4:2", 20, 1000), 20, &limits)
            .unwrap_err();
        assert_eq!(err, PublishError::Stale);
        assert_eq!(store.lookup("1.2.3.4:1".parse().unwrap(), 21).len(), 1);
        assert!(store.lookup("1.2.3.4:2".parse().unwrap(), 21).is_empty());
    }

    #[test]
    fn rate_limit_bucket_map_is_bounded() {
        let now = Instant::now();
        let mut map = HashMap::new();
        for key in 0..MAX_RATE_LIMIT_BUCKETS {
            assert!(take(&mut map, key, now, 1.0, 1.0));
        }
        assert!(!take(&mut map, MAX_RATE_LIMIT_BUCKETS, now, 1.0, 1.0));
        assert_eq!(map.len(), MAX_RATE_LIMIT_BUCKETS);
    }

    #[test]
    fn lookup_lazily_expires() {
        let limits = Limits {
            record_ttl_secs: 40,
            ..Limits::for_tests()
        };
        let mut store = Store::new();
        let a = SecretKey::generate();
        store
            .publish(rec(&a, "1.2.3.4:1", 10, 50), 10, &limits)
            .unwrap();
        store
            .publish(
                rec(&SecretKey::generate(), "1.2.3.4:2", 20, 1000),
                20,
                &limits,
            )
            .unwrap();
        assert_eq!(store.lookup("1.2.3.4:1".parse().unwrap(), 20).len(), 1);
        assert_eq!(store.lookup("1.2.3.4:2".parse().unwrap(), 20).len(), 1);
        assert_eq!(store.lookup("1.2.3.4:1".parse().unwrap(), 50).len(), 0);
        assert_eq!(store.lookup("1.2.3.4:2".parse().unwrap(), 50).len(), 1);
    }

    #[test]
    fn activity_collects_expired_rows_globally() {
        let limits = Limits {
            record_ttl_secs: 40,
            ..Limits::for_tests()
        };
        let mut store = Store::new();
        store
            .publish(
                rec(&SecretKey::generate(), "1.2.3.4:1", 10, 50),
                10,
                &limits,
            )
            .unwrap();
        assert_eq!(store.len(), 1);

        // Activity after the maintenance interval collects all expired rows,
        // even though it addresses a different mapping.
        assert!(store.lookup("1.2.3.4:2".parse().unwrap(), 70).is_empty());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn rejects_bad_sig() {
        let limits = Limits::for_tests();
        let mut store = Store::new();
        let mut rec = rec(&SecretKey::generate(), "1.2.3.4:1", 10, 1000);
        rec.payload
            .v1_mut()
            .addrs
            .push("5.6.7.8:9".parse().unwrap());
        assert!(matches!(
            store.publish(rec, 10, &limits),
            Err(PublishError::Verify(VerifyError::BadSignature))
        ));
    }

    #[test]
    fn rejects_forward_only() {
        let limits = Limits::for_tests();
        let mut store = Store::new();
        let rec = SignedRecord::sign_at(
            &SecretKey::generate(),
            vec!["1.2.3.4:1".parse().unwrap()],
            [b"test/0"],
            [Index::Forward],
            10,
        );
        assert_eq!(
            store.publish(rec, 10, &limits),
            Err(PublishError::IndexNotAllowed)
        );
    }
}
