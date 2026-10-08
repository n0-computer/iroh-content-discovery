//! Providers that links named for a hash, asked before Mainline.
//!
//! `?provider={id}` on a URL of a hash names an endpoint that serves it, so
//! content can load without being announced. The gateway remembers the
//! providers for the hash, in memory only, so the page's subresources, which
//! carry no query, find them too. A hint only adds candidates: the URL means
//! the same content either way, and Mainline remains the fallback.

use std::{num::NonZeroUsize, str::FromStr, sync::Mutex};

use axum::http::StatusCode;
use iroh::EndpointId;
use iroh_blobs::Hash;
use lru::LruCache;

use crate::{HttpError, parse_z32_bytes};

/// The query parameter naming a provider; it may repeat.
const PARAM: &str = "provider";
/// Providers a single URL may name.
const MAX_PER_URL: usize = 4;
/// Providers remembered per hash; the least recently named ones go first.
const MAX_PER_HASH: usize = 8;
/// Hashes remembered at once.
const SLOTS: NonZeroUsize = NonZeroUsize::new(1024).expect("nonzero");

/// Providers that links named, per hash.
#[derive(Debug)]
pub(crate) struct Hints(Mutex<LruCache<Hash, Vec<EndpointId>>>);

impl Default for Hints {
    fn default() -> Self {
        Self(Mutex::new(LruCache::new(SLOTS)))
    }
}

impl Hints {
    /// Remembers the providers `query` names for `hash`.
    ///
    /// # Errors
    ///
    /// Rejects a malformed provider, or more than [`MAX_PER_URL`], rather than
    /// ignoring them.
    pub(crate) fn add_from_query(&self, hash: Hash, query: Option<&str>) -> Result<(), HttpError> {
        let providers = parse_query(query)?;
        if !providers.is_empty() {
            self.add(hash, &providers);
        }
        Ok(())
    }

    /// Adds `providers` for `hash`, ahead of the ones known before.
    fn add(&self, hash: Hash, providers: &[EndpointId]) {
        let mut hints = self.0.lock().expect("poisoned");
        let known = hints.get_or_insert_mut(hash, Vec::new);
        known.retain(|provider| !providers.contains(provider));
        known.splice(0..0, providers.iter().copied());
        known.truncate(MAX_PER_HASH);
    }

    /// Returns the providers known for `hash`, most recently named first.
    pub(crate) fn get(&self, hash: Hash) -> Vec<EndpointId> {
        self.0
            .lock()
            .expect("poisoned")
            .get(&hash)
            .cloned()
            .unwrap_or_default()
    }
}

/// Returns the distinct providers `query` names.
fn parse_query(query: Option<&str>) -> Result<Vec<EndpointId>, HttpError> {
    let mut providers = Vec::new();
    for pair in query.into_iter().flat_map(|query| query.split('&')) {
        let value = match pair.split_once('=') {
            Some((PARAM, value)) => value,
            _ if pair == PARAM => "",
            _ => continue,
        };
        let provider = parse_provider(value).ok_or(HttpError(
            StatusCode::BAD_REQUEST,
            "invalid provider: expected an endpoint ID in z-base-32 or hex",
        ))?;
        if !providers.contains(&provider) {
            providers.push(provider);
        }
    }
    if providers.len() > MAX_PER_URL {
        return Err(HttpError(
            StatusCode::BAD_REQUEST,
            "a URL may name at most four providers",
        ));
    }
    Ok(providers)
}

/// Parses an endpoint ID in z-base-32, as hashes are written, or in hex, as iroh prints it.
fn parse_provider(value: &str) -> Option<EndpointId> {
    match parse_z32_bytes(value) {
        Ok(bytes) => EndpointId::from_bytes(&bytes).ok(),
        Err(_) => EndpointId::from_str(value).ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u8) -> EndpointId {
        iroh::SecretKey::from_bytes(&[n; 32]).public()
    }

    #[test]
    fn query_accepts_both_encodings_and_ignores_other_parameters() {
        let (a, b) = (id(1), id(2));
        let z32 = z32::encode(a.as_bytes());
        let query = format!("x=1&provider={z32}&listing&provider={b}&provider={z32}");
        assert_eq!(parse_query(Some(&query)).unwrap(), [a, b]);
        assert!(parse_query(Some("x=1&listing")).unwrap().is_empty());
        assert!(parse_query(None).unwrap().is_empty());
        assert!(parse_query(Some("provider=nope")).is_err());
        assert!(parse_query(Some("provider")).is_err());
        let five: Vec<_> = (1..=5).map(|n| format!("provider={}", id(n))).collect();
        assert!(parse_query(Some(&five.join("&"))).is_err());
    }

    #[test]
    fn hints_put_recent_providers_first_and_stay_bounded() {
        let hints = Hints::default();
        let hash = Hash::new(b"content");
        hints.add(hash, &[id(1), id(2)]);
        hints.add(hash, &[id(3), id(1)]);
        assert_eq!(hints.get(hash), [id(3), id(1), id(2)]);
        for n in 4..20 {
            hints.add(hash, &[id(n)]);
        }
        let known = hints.get(hash);
        assert_eq!(known.len(), MAX_PER_HASH);
        assert_eq!(known[0], id(19));
        assert!(hints.get(Hash::new(b"other")).is_empty());
    }
}
