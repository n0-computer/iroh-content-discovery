//! Opaque UDP address index.
//!
//! A successful put is stored under the packet's observed public IPv4 socket.
//! A stateless write token prevents blind source-address spoofing. Values are
//! opaque to the replica.

#![deny(missing_docs, rustdoc::broken_intra_doc_links)]

mod store;

pub mod udp;

use std::{
    collections::HashMap,
    hash::Hash,
    net::{IpAddr, SocketAddrV4},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rand::Rng;

pub use store::{PutError, Store};
pub use udp::UdpHandle;

const MAX_RATE_LIMIT_BUCKETS: usize = 65_536;
const TOKEN_DOMAIN: &[u8] = b"udp-addr-index-token-v1";

/// Storage, token, and request limits for a replica.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Maximum opaque value length.
    pub max_value_len: usize,
    /// Maximum number of live mappings.
    pub max_entries: usize,
    /// How long an accepted value remains live.
    pub value_ttl_secs: u64,
    /// Length of one token validity bucket.
    ///
    /// The current and previous bucket are accepted.
    pub token_bucket_secs: u64,
    /// Request token rate per source IP.
    pub requests_per_ip_per_sec: f64,
    /// Request burst per source IP.
    pub request_burst: f64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_value_len: iroh_addr_index_proto::MAX_VALUE_LEN,
            max_entries: 2_000_000,
            value_ttl_secs: 60 * 60,
            token_bucket_secs: 30,
            requests_per_ip_per_sec: 50.0,
            request_burst: 100.0,
        }
    }
}

impl Limits {
    /// Loose limits suitable for tests.
    pub fn for_tests() -> Self {
        Self {
            value_ttl_secs: 24 * 60 * 60,
            requests_per_ip_per_sec: 1_000.0,
            request_burst: 1_000.0,
            ..Self::default()
        }
    }
}

/// In-memory address-index replica.
#[derive(Debug, Clone)]
pub struct Server {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    store: Mutex<Store>,
    limits: Limits,
    rate: Mutex<RateLimiters>,
    token_key: [u8; 32],
}

impl Server {
    /// Create an empty replica with a randomly generated token key.
    pub fn new(limits: Limits) -> Self {
        let mut token_key = [0; 32];
        rand::rng().fill_bytes(&mut token_key);
        Self {
            inner: Arc::new(Inner {
                store: Mutex::new(Store::new()),
                limits,
                rate: Mutex::new(RateLimiters::default()),
                token_key,
            }),
        }
    }

    /// Limits used by this replica.
    pub fn limits(&self) -> &Limits {
        &self.inner.limits
    }

    /// Store bytes directly, bypassing UDP token validation.
    pub fn put_local(&self, addr: SocketAddrV4, value: Vec<u8>) -> Result<(), PutError> {
        self.inner
            .store
            .lock()
            .expect("poisoned")
            .put(addr, value, unix_secs(), &self.inner.limits)
    }

    /// Read a live value directly.
    pub fn get_local(&self, addr: SocketAddrV4) -> Option<Vec<u8>> {
        self.inner
            .store
            .lock()
            .expect("poisoned")
            .get(addr, unix_secs())
    }

    pub(crate) fn issue_token(&self, addr: SocketAddrV4, now: u64) -> [u8; 16] {
        self.token_for(addr, self.bucket(now))
    }

    pub(crate) fn verify_token(&self, addr: SocketAddrV4, token: &[u8; 16], now: u64) -> bool {
        let bucket = self.bucket(now);
        token_eq(&self.token_for(addr, bucket), token)
            || bucket
                .checked_sub(1)
                .is_some_and(|previous| token_eq(&self.token_for(addr, previous), token))
    }

    fn bucket(&self, now: u64) -> u64 {
        now / self.inner.limits.token_bucket_secs.max(1)
    }

    fn token_for(&self, addr: SocketAddrV4, bucket: u64) -> [u8; 16] {
        let mut input = Vec::with_capacity(TOKEN_DOMAIN.len() + 4 + 2 + 8);
        input.extend_from_slice(TOKEN_DOMAIN);
        input.extend_from_slice(&addr.ip().octets());
        input.extend_from_slice(&addr.port().to_be_bytes());
        input.extend_from_slice(&bucket.to_be_bytes());
        let hash = blake3::keyed_hash(&self.inner.token_key, &input);
        hash.as_bytes()[..16].try_into().expect("fixed length")
    }

    pub(crate) fn allow_request(&self, ip: IpAddr) -> bool {
        self.inner
            .rate
            .lock()
            .expect("poisoned")
            .allow(&self.inner.limits, ip)
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new(Limits::default())
    }
}

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

impl Bucket {
    fn take(&mut self, now: Instant, rate: f64, burst: f64) -> bool {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * rate).min(burst);
        self.last = now;
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

#[derive(Debug, Default)]
struct RateLimiters {
    by_ip: HashMap<IpAddr, Bucket>,
    last_gc: Option<Instant>,
}

impl RateLimiters {
    fn allow(&mut self, limits: &Limits, ip: IpAddr) -> bool {
        let now = Instant::now();
        if self
            .last_gc
            .is_none_or(|last| now.saturating_duration_since(last) >= Duration::from_secs(60))
        {
            self.last_gc = Some(now);
            self.by_ip.retain(|_, bucket| {
                now.saturating_duration_since(bucket.last) <= Duration::from_secs(600)
            });
        }
        take(
            &mut self.by_ip,
            ip,
            now,
            limits.requests_per_ip_per_sec,
            limits.request_burst,
        )
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
        .take(now, rate, burst)
}

fn token_eq(left: &[u8; 16], right: &[u8; 16]) -> bool {
    left.iter()
        .zip(right)
        .fold(0, |difference, (left, right)| difference | (left ^ right))
        == 0
}

pub(crate) fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_is_bound_to_socket_and_recent_buckets() {
        let server = Server::new(Limits {
            token_bucket_secs: 10,
            ..Limits::for_tests()
        });
        let addr = "203.0.113.9:6881".parse().unwrap();
        let token = server.issue_token(addr, 19);
        assert!(server.verify_token(addr, &token, 20));
        assert!(!server.verify_token(addr, &token, 30));
        assert!(!server.verify_token("203.0.113.9:6882".parse().unwrap(), &token, 20));
    }
}
