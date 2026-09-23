//! In-memory `SocketAddrV4 → opaque bytes` store.

use std::{
    collections::{BTreeSet, HashMap},
    net::SocketAddrV4,
};

use crate::Limits;

/// Rejection of a local put.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PutError {
    /// The opaque value exceeds the server's configured limit.
    TooLarge,
    /// The server reached its entry limit.
    Full,
}

impl std::fmt::Display for PutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooLarge => write!(f, "value is too large"),
            Self::Full => write!(f, "address index is full"),
        }
    }
}

impl std::error::Error for PutError {}

#[derive(Debug)]
struct Entry {
    value: Vec<u8>,
    expires_at: u64,
}

/// In-memory address-index state.
#[derive(Debug, Default)]
pub struct Store {
    entries: HashMap<SocketAddrV4, Entry>,
    expirations: BTreeSet<(u64, SocketAddrV4)>,
}

impl Store {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of retained entries, including entries pending lazy collection.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the store contains no retained entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Insert or replace one value using server receipt time for expiry.
    pub fn put(
        &mut self,
        addr: SocketAddrV4,
        value: Vec<u8>,
        now: u64,
        limits: &Limits,
    ) -> Result<(), PutError> {
        self.gc(now);
        if value.len() > limits.max_value_len || value.len() > udp_addr_index_proto::MAX_VALUE_LEN {
            return Err(PutError::TooLarge);
        }
        if !self.entries.contains_key(&addr) && self.entries.len() >= limits.max_entries {
            return Err(PutError::Full);
        }
        let expires_at = now.saturating_add(limits.value_ttl_secs);
        if let Some(previous) = self.entries.insert(addr, Entry { value, expires_at }) {
            self.expirations.remove(&(previous.expires_at, addr));
        }
        self.expirations.insert((expires_at, addr));
        Ok(())
    }

    /// Clone the live value stored under `addr`.
    pub fn get(&mut self, addr: SocketAddrV4, now: u64) -> Option<Vec<u8>> {
        self.gc(now);
        self.entries.get(&addr).map(|entry| entry.value.clone())
    }

    /// Remove expired entries in expiry order without scanning live entries.
    pub fn gc(&mut self, now: u64) {
        while let Some(&(expires_at, addr)) = self.expirations.first() {
            if expires_at > now {
                break;
            }
            self.expirations.pop_first();
            self.entries.remove(&addr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replace_and_expire() {
        let limits = Limits {
            value_ttl_secs: 10,
            ..Limits::for_tests()
        };
        let addr = "203.0.113.9:6881".parse().unwrap();
        let mut store = Store::new();
        store.put(addr, b"a".to_vec(), 10, &limits).unwrap();
        store.put(addr, b"b".to_vec(), 11, &limits).unwrap();
        assert_eq!(store.get(addr, 20).unwrap(), b"b");
        assert!(store.get(addr, 21).is_none());
    }

    #[test]
    fn caps_values_and_entries() {
        let limits = Limits {
            max_value_len: 1,
            max_entries: 1,
            ..Limits::for_tests()
        };
        let mut store = Store::new();
        let a = "203.0.113.9:1".parse().unwrap();
        let b = "203.0.113.9:2".parse().unwrap();
        assert_eq!(
            store.put(a, vec![0, 1], 0, &limits),
            Err(PutError::TooLarge)
        );
        store.put(a, vec![0], 0, &limits).unwrap();
        assert_eq!(store.put(b, vec![0], 0, &limits), Err(PutError::Full));
        store.put(a, vec![1], 0, &limits).unwrap();
    }

    #[test]
    fn refresh_keeps_one_expiration_and_reclaims_capacity() {
        let limits = Limits {
            value_ttl_secs: 10,
            max_entries: 2,
            ..Limits::for_tests()
        };
        let a = "203.0.113.9:1".parse().unwrap();
        let b = "203.0.113.9:2".parse().unwrap();
        let c = "203.0.113.9:3".parse().unwrap();
        let mut store = Store::new();
        store.put(a, vec![1], 0, &limits).unwrap();
        store.put(b, vec![2], 0, &limits).unwrap();
        for now in 1..10 {
            store.put(a, vec![3], now, &limits).unwrap();
            assert_eq!(store.expirations.len(), 2);
        }
        // The original deadline removes b, but not the refreshed a.
        store.put(c, vec![4], 10, &limits).unwrap();
        assert_eq!(store.get(a, 10), Some(vec![3]));
        assert_eq!(store.get(b, 10), None);
        assert_eq!(store.len(), 2);
        assert_eq!(store.get(a, 19), None);
        assert_eq!(store.get(c, 19), Some(vec![4]));
        store.gc(20);
        assert!(store.is_empty());
        assert!(store.expirations.is_empty());
    }

    #[test]
    fn expiration_order_does_not_require_monotonic_deadlines() {
        let mut limits = Limits {
            value_ttl_secs: 100,
            ..Limits::for_tests()
        };
        let a = "203.0.113.9:1".parse().unwrap();
        let b = "203.0.113.9:2".parse().unwrap();
        let mut store = Store::new();
        store.put(a, vec![1], 0, &limits).unwrap();
        limits.value_ttl_secs = 5;
        store.put(b, vec![2], 1, &limits).unwrap();
        assert_eq!(store.get(b, 6), None);
        assert_eq!(store.get(a, 6), Some(vec![1]));
        assert_eq!(store.expirations.len(), 1);
    }
}
