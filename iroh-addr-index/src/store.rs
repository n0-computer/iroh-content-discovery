//! In-memory `SocketAddrV4 → opaque bytes` store.

use std::{collections::HashMap, net::SocketAddrV4};

use crate::Limits;

/// Rejection of a local put.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PutError {
    /// The opaque value exceeds the replica's configured limit.
    TooLarge,
    /// The replica reached its entry limit.
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
    last_gc: Option<u64>,
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

    /// Insert or replace one value using replica receipt time for expiry.
    pub fn put(
        &mut self,
        addr: SocketAddrV4,
        value: Vec<u8>,
        now: u64,
        limits: &Limits,
    ) -> Result<(), PutError> {
        self.maintain(now);
        if value.len() > limits.max_value_len || value.len() > iroh_addr_index_proto::MAX_VALUE_LEN
        {
            return Err(PutError::TooLarge);
        }
        if !self.entries.contains_key(&addr) && self.entries.len() >= limits.max_entries {
            self.gc(now);
            if self.entries.len() >= limits.max_entries {
                return Err(PutError::Full);
            }
        }
        self.entries.insert(
            addr,
            Entry {
                value,
                expires_at: now.saturating_add(limits.value_ttl_secs),
            },
        );
        Ok(())
    }

    /// Clone the live value stored under `addr`.
    pub fn get(&mut self, addr: SocketAddrV4, now: u64) -> Option<Vec<u8>> {
        self.maintain(now);
        match self.entries.get(&addr) {
            Some(entry) if entry.expires_at > now => Some(entry.value.clone()),
            Some(_) => {
                self.entries.remove(&addr);
                None
            }
            None => None,
        }
    }

    /// Remove all expired entries.
    pub fn gc(&mut self, now: u64) {
        self.last_gc = Some(now);
        self.entries.retain(|_, entry| entry.expires_at > now);
    }

    fn maintain(&mut self, now: u64) {
        if self
            .last_gc
            .is_none_or(|last| now.saturating_sub(last) >= 60)
        {
            self.gc(now);
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
}
