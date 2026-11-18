//! Resolves and caches signed Pkarr records through the gateway's shared DHT.

use lru::LruCache;
use std::{
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use axum::{
    Extension,
    extract::{OriginalUri, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use n0_future::StreamExt;
use simple_dns::{
    CLASS, Packet,
    rdata::{RData, SVCB, SVCParam},
};

use percent_encoding::percent_decode_str;

use crate::{
    Caching, Gateway, HttpError, LOOKUP_TIMEOUT, Root, Subdomain, error_page::RETRY_SOON,
    parse_hash, parse_z32_bytes, serve_path, serve_root,
};

/// Targets under this domain name content this gateway can serve itself.
use iroh_mainline_endpoint_discovery::{BLAKE3_DOMAIN, is_hostname};
use n0_mainline::{Dht, MutableItem};
use tracing::{debug, info, warn};

// Bound both memory use and how long changed names can remain stale.
const MAX_CACHE_TTL: Duration = Duration::from_secs(30);

/// How long to keep reading answers after the first one arrives.
///
/// Any node may answer with an older packet that still verifies, so taking the
/// first answer lets one stale node roll a name back. Waiting for the whole
/// lookup would cost seconds, so we take the newest answer within this window.
const NEWEST_GRACE: Duration = Duration::from_millis(300);

/// How long a lookup waits for a Mainline node without peers to find some.
const BOOTSTRAP_WAIT: Duration = Duration::from_secs(10);

/// Mainline lookups per request while they end without an answer.
const QUERY_ATTEMPTS: u32 = 3;

/// Pause before the second lookup; doubled before each one after.
const QUERY_BACKOFF: Duration = Duration::from_secs(1);

/// When a browser tries again while the node has no peers.
const RETRY_NOT_CONNECTED: Duration = Duration::from_secs(5);

/// When a browser tries again after Mainline had no packet.
const RETRY_NOT_FOUND: Duration = Duration::from_secs(30);

/// Verified packets kept between requests for the same key.
const PACKET_SLOTS: NonZeroUsize = NonZeroUsize::new(1024).expect("nonzero");

pub(crate) struct Cache(LruCache<[u8; 32], (Instant, n0_mainline::MutableItem)>);

impl Default for Cache {
    fn default() -> Self {
        Self(LruCache::new(PACKET_SLOTS))
    }
}

impl Cache {
    fn get(&mut self, key: &[u8; 32], now: Instant) -> Option<n0_mainline::MutableItem> {
        if self.0.peek(key).is_some_and(|(expires, _)| *expires <= now) {
            self.0.pop(key);
        }
        self.0.get(key).map(|(_, item)| item.clone())
    }

    fn insert(
        &mut self,
        key: [u8; 32],
        item: n0_mainline::MutableItem,
        packet: &Packet<'_>,
        now: Instant,
    ) {
        // Using the minimum answer TTL is conservative when a packet contains
        // multiple records. Zero TTL explicitly disables caching.
        let ttl = Duration::from_secs(
            packet
                .answers
                .iter()
                .map(|rr| rr.ttl)
                .min()
                .unwrap_or(0)
                .into(),
        )
        .min(MAX_CACHE_TTL);
        if !ttl.is_zero() {
            self.0.put(key, (now + ttl, item));
        }
    }
}

pub(crate) async fn redirect(
    State(gateway): State<Gateway>,
    OriginalUri(uri): OriginalUri,
    subdomain: Option<Extension<Subdomain>>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    // Keep the original escaping, including encoded slashes and query values.
    let rest = uri
        .path()
        .strip_prefix("/pkarr/")
        .expect("routed under /pkarr/");
    let (encoded, path) = rest
        .split_once('/')
        .map_or((rest, "/".to_owned()), |(key, path)| {
            (key, format!("/{path}"))
        });
    let key = parse_key(encoded)?;
    if crate::has_flag(uri.query(), "debug") {
        let host = subdomain
            .as_ref()
            .and_then(|_| headers.get(header::HOST)?.to_str().ok());
        return Ok(crate::debug_page::pkarr(&gateway, &key, encoded, host).await);
    }
    let started = std::time::Instant::now();
    debug!(key = encoded, "resolving Pkarr record");
    let cached = gateway
        .0
        .pkarr
        .lock()
        .expect("poisoned")
        .get(&key, Instant::now());
    let cache_hit = cached.is_some();
    let item = if let Some(item) = cached {
        debug!(key = encoded, "Pkarr cache hit");
        item
    } else {
        tokio::time::timeout(LOOKUP_TIMEOUT, resolve(&gateway, &key))
            .await
            .map_err(|_| {
                warn!(
                    key = encoded,
                    elapsed_ms = started.elapsed().as_millis(),
                    "Pkarr lookup timed out"
                );
                HttpError::transient(
                    StatusCode::GATEWAY_TIMEOUT,
                    "Pkarr lookup timed out: Mainline nodes did not answer in time; retrying",
                    RETRY_SOON,
                )
            })??
    };
    let packet = Packet::parse(item.value()).map_err(|error| {
        warn!(%error, "invalid Pkarr DNS packet");
        HttpError::new(StatusCode::BAD_GATEWAY, "invalid Pkarr DNS packet")
    })?;
    debug!(
        key = encoded,
        sequence = item.seq(),
        answers = packet.answers.len(),
        elapsed_ms = started.elapsed().as_millis(),
        "Pkarr DNS packet decoded"
    );
    if !cache_hit {
        gateway.0.pkarr.lock().expect("poisoned").insert(
            key,
            item.clone(),
            &packet,
            Instant::now(),
        );
    }
    let authority = target(&packet, encoded).ok_or_else(|| {
        info!(key = encoded, "Pkarr packet has no usable target");
        HttpError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "the Pkarr packet for this key has no usable target: no supported apex HTTPS record",
        )
    })?;
    // A content-addressed target is served here instead of being handed back
    // to the browser, so the key stays in the address bar and the bytes stay
    // verified.
    if let Some(hash) = content_hash(&authority) {
        let base = match subdomain {
            Some(_) => String::new(),
            None => format!("/pkarr/{encoded}"),
        };
        // The key names content that changes, so responses must revalidate.
        let root = Root::at(encoded.to_owned(), base, Caching::Revalidate);
        let query = uri.query().map(str::to_owned);
        let path = percent_decode_str(path.trim_start_matches('/'))
            .decode_utf8()
            .map_err(|_| HttpError::new(StatusCode::BAD_REQUEST, "path is not valid UTF-8"))?
            .into_owned();
        return if path.is_empty() {
            // `/pkarr/{key}` has no slash; a subdomain's root is always `/`.
            let slash = subdomain.is_some() || rest.contains('/');
            serve_root(&gateway, root, hash, slash, query, method, headers).await
        } else {
            serve_path(gateway, root, hash, path, query, method, headers).await
        };
    }
    let mut location = format!("https://{authority}{path}");
    if let Some(query) = uri.query() {
        location.push('?');
        location.push_str(query);
    }
    debug!(key = encoded, %location, "redirecting Pkarr request");
    Ok((
        StatusCode::TEMPORARY_REDIRECT,
        [
            (header::LOCATION, location),
            (header::CACHE_CONTROL, "no-store".to_owned()),
        ],
    )
        .into_response())
}

/// Returns the hash when a target names content, as `<z32>.blake3.net`.
pub(crate) fn content_hash(authority: &str) -> Option<iroh_blobs::Hash> {
    let label = authority.strip_suffix(BLAKE3_DOMAIN)?.strip_suffix('.')?;
    parse_hash(label).ok()
}

fn parse_key(encoded: &str) -> Result<[u8; 32], HttpError> {
    parse_z32_bytes(encoded).map_err(|_| {
        HttpError::new(
            StatusCode::BAD_REQUEST,
            "invalid z-base-32 Pkarr public key",
        )
    })
}

/// Finds the newest packet for `key`, trying again while it looks transient.
///
/// An empty answer from a node with no peers says nothing about the key, so
/// it waits for the node to find peers first. A lookup that ends without an
/// answer is repeated, since one lookup can miss the nodes that hold the
/// packet. HTTP relays, if any, are asked last, also when Mainline is out of
/// reach.
async fn resolve(gateway: &Gateway, key: &[u8; 32]) -> Result<MutableItem, HttpError> {
    let encoded = z32::encode(key);
    let mainline = mainline(gateway.resolver().dht(), key).await;
    if let Ok(Some(item)) = mainline {
        return Ok(item);
    }
    // Relays also help when Mainline is out of reach, as with UDP blocked.
    let relays = gateway.0.pkarr_relays.lock().expect("poisoned").clone();
    if let Some(relays) = relays
        && let Some(item) = relays.resolve(key).await
    {
        info!(
            key = encoded,
            "Pkarr packet found on a relay, not on Mainline"
        );
        return Ok(item);
    }
    mainline?;
    info!(
        key = encoded,
        lookups = QUERY_ATTEMPTS,
        "no Pkarr packet found on Mainline"
    );
    Err(HttpError::transient(
        StatusCode::NOT_FOUND,
        "no Pkarr packet found for this key: Mainline answered, but no node holds a record; \
         the publisher may be offline or its record may have expired; retrying",
        RETRY_NOT_FOUND,
    ))
}

/// Asks Mainline up to [`QUERY_ATTEMPTS`] times while lookups end empty.
async fn mainline(dht: &Dht, key: &[u8; 32]) -> Result<Option<MutableItem>, HttpError> {
    let mut backoff = QUERY_BACKOFF;
    for attempt in 1..=QUERY_ATTEMPTS {
        connected(dht).await?;
        let mut items = dht.get_mutable(key, None, None).await.map_err(|error| {
            warn!(%error, "Pkarr lookup failed");
            HttpError::new(StatusCode::BAD_GATEWAY, "Pkarr lookup failed")
        })?;
        if let Some(item) = newest_verified(&mut items, key).await? {
            return Ok(Some(item));
        }
        debug!(attempt, "Mainline lookup found no Pkarr packet");
        if attempt < QUERY_ATTEMPTS {
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
    }
    // The node may have lost its peers during the last lookup.
    connected(dht).await?;
    Ok(None)
}

/// Waits up to [`BOOTSTRAP_WAIT`] for the Mainline node to know some peers.
async fn connected(dht: &Dht) -> Result<(), HttpError> {
    let peers = async || dht.info().await.map_or(0, |info| info.routing_table_size());
    if peers().await > 0 {
        return Ok(());
    }
    debug!("Mainline routing table is empty; waiting for bootstrap");
    let bootstrap = async {
        // Each round asks the bootstrap nodes again.
        while !dht.bootstrapped().await.unwrap_or(false) {
            tokio::time::sleep(QUERY_BACKOFF).await;
        }
    };
    if tokio::time::timeout(BOOTSTRAP_WAIT, bootstrap)
        .await
        .is_ok()
    {
        return Ok(());
    }
    warn!(
        "not connected to Mainline: the routing table is empty; \
         outbound UDP may be blocked, the network may be IPv6 only, or it just changed"
    );
    Err(HttpError::transient(
        StatusCode::SERVICE_UNAVAILABLE,
        "not connected to the Mainline DHT yet: the gateway knows no DHT nodes; retrying",
        RETRY_NOT_CONNECTED,
    ))
}

/// Takes the newest answer, waiting [`NEWEST_GRACE`] after the first one.
///
/// Every answer is signature-checked by `get_mutable`, but an old packet
/// verifies just as well as a current one, so the sequence number decides.
/// Returns `None` if the lookup ends without an answer.
async fn newest_verified(
    items: &mut (impl n0_future::Stream<Item = MutableItem> + Unpin),
    key: &[u8; 32],
) -> Result<Option<MutableItem>, HttpError> {
    let Some(first) = items.next().await else {
        return Ok(None);
    };
    let mut newest = verified(first, key)?;
    let deadline = tokio::time::sleep(NEWEST_GRACE);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            () = &mut deadline => break,
            item = items.next() => {
                let Some(item) = item else { break };
                let item = verified(item, key)?;
                if item.seq() > newest.seq() {
                    newest = item;
                }
            }
        }
    }
    debug!(
        sequence = newest.seq(),
        bytes = newest.value().len(),
        "using newest verified Pkarr item"
    );
    Ok(Some(newest))
}

/// Rejects an item that is not a well-formed answer for `key`.
fn verified(
    item: n0_mainline::MutableItem,
    key: &[u8; 32],
) -> Result<n0_mainline::MutableItem, HttpError> {
    // get_mutable verifies BEP44 signatures and binds each item to the requested
    // key. Its value is the DNS wire packet; no Pkarr envelope is needed.
    if item.seq() < 0 || item.key() != key {
        return Err(HttpError::new(
            StatusCode::BAD_GATEWAY,
            "invalid Pkarr key or timestamp",
        ));
    }
    Ok(item)
}

pub(crate) fn target(packet: &Packet<'_>, key: &str) -> Option<String> {
    packet
        .answers
        .iter()
        .filter(|record| {
            record.class == CLASS::IN && record.name.to_string().eq_ignore_ascii_case(key)
        })
        .filter_map(|record| {
            let RData::HTTPS(https) = &record.rdata else {
                return None;
            };
            authority(&https.0).map(|target| (https.0.priority, target))
        })
        .min()
        .map(|(_, target)| target)
}

fn authority(svcb: &SVCB<'_>) -> Option<String> {
    let name = svcb.target.to_string();
    let name = name.trim_end_matches('.');
    // Redirects require a conventional hostname. Root targets and bare Pkarr
    // keys require endpoint resolution rather than an HTTP redirect. The
    // publisher applies the same test, or it would accept names we refuse.
    if !is_hostname(name) {
        return None;
    }
    let mut target = name.to_owned();
    if svcb.priority != 0 {
        for param in svcb.iter_params() {
            match param {
                // A browser redirect cannot convey mandatory SVCB parameters.
                SVCParam::Mandatory(_) | SVCParam::NoDefaultAlpn | SVCParam::Port(0) => {
                    return None;
                }
                SVCParam::Port(port) if *port != 443 => target.push_str(&format!(":{port}")),
                _ => {}
            }
        }
    }
    Some(target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use simple_dns::{ResourceRecord, rdata::HTTPS};

    fn signed_packet(ttl: u32) -> (n0_mainline::MutableItem, Vec<u8>) {
        signed_packet_with_sequence(ttl, 1)
    }

    fn signed_packet_with_sequence(ttl: u32, sequence: i64) -> (n0_mainline::MutableItem, Vec<u8>) {
        let key = n0_mainline::SigningKey::from_bytes(&[9; 32]);
        let mut packet = Packet::new_reply(0);
        packet.answers.push(ResourceRecord::new(
            "example.com".try_into().unwrap(),
            CLASS::IN,
            ttl,
            RData::HTTPS(HTTPS(SVCB::new(0, "target.example".try_into().unwrap()))),
        ));
        let bytes = packet.build_bytes_vec_compressed().unwrap();
        (
            n0_mainline::MutableItem::new(&key, &bytes, sequence, None),
            bytes,
        )
    }

    #[tokio::test]
    async fn lookup_does_not_wait_for_completion() {
        let (item, _) = signed_packet(300);
        let key = *item.key();
        let stream = async_stream::stream! {
            yield item;
            std::future::pending::<()>().await;
        };
        let mut stream = Box::pin(stream);
        let result = tokio::time::timeout(
            NEWEST_GRACE + Duration::from_millis(200),
            newest_verified(&mut stream, &key),
        )
        .await;
        assert!(result.is_ok_and(|item| item.is_ok_and(|item| item.is_some())));
    }

    #[tokio::test]
    async fn a_stale_answer_does_not_win_the_race() {
        // A node answering first with an old packet would otherwise roll the
        // name back for as long as the cache holds it.
        let (stale, _) = signed_packet_with_sequence(300, 100);
        let (current, _) = signed_packet_with_sequence(300, 200);
        let key = *stale.key();
        let stream = async_stream::stream! {
            yield stale;
            yield current;
            std::future::pending::<()>().await;
        };
        let mut stream = Box::pin(stream);
        let Ok(Some(newest)) = newest_verified(&mut stream, &key).await else {
            panic!("newest lookup failed")
        };
        assert_eq!(newest.seq(), 200);
    }

    #[tokio::test]
    async fn a_lookup_without_answers_is_no_packet() {
        let mut stream = Box::pin(n0_future::stream::empty());
        assert!(matches!(
            newest_verified(&mut stream, &[1; 32]).await,
            Ok(None)
        ));
    }

    #[test]
    fn cache_expires_without_sliding_and_respects_dns_ttl() {
        let now = Instant::now();
        for (ttl, expires) in [(300, 30), (2, 2)] {
            let (item, bytes) = signed_packet(ttl);
            let key = *item.key();
            let packet = Packet::parse(&bytes).unwrap();
            let mut cache = Cache::default();
            cache.insert(key, item, &packet, now);
            assert!(
                cache
                    .get(&key, now + Duration::from_secs(expires - 1))
                    .is_some()
            );
            assert!(
                cache
                    .get(&key, now + Duration::from_secs(expires))
                    .is_none()
            );
        }
        let (item, bytes) = signed_packet(0);
        let key = *item.key();
        let mut cache = Cache::default();
        cache.insert(key, item, &Packet::parse(&bytes).unwrap(), now);
        assert!(cache.get(&key, now).is_none());
    }

    #[test]
    fn dns_targets_and_validation() {
        use simple_dns::{ResourceRecord, rdata::HTTPS};

        let key = z32::encode(&[7; 32]);
        let mut preferred = SVCB::new(1, "example.com".try_into().unwrap());
        preferred.set_port(8443);
        let mut packet = Packet::new_reply(0);
        for (name, svcb) in [
            (
                key.as_str(),
                SVCB::new(2, "other.example".try_into().unwrap()),
            ),
            (key.as_str(), preferred),
            (
                "unrelated",
                SVCB::new(0, "wrong.example".try_into().unwrap()),
            ),
        ] {
            packet.answers.push(ResourceRecord::new(
                name.try_into().unwrap(),
                CLASS::IN,
                60,
                RData::HTTPS(HTTPS(svcb)),
            ));
        }
        let bytes = packet.build_bytes_vec_compressed().unwrap();
        let packet = Packet::parse(&bytes).unwrap();
        assert_eq!(target(&packet, &key).as_deref(), Some("example.com:8443"));
        assert!(target(&packet, &z32::encode(&[8; 32])).is_none());
        assert!(parse_key(&key).is_ok());
        assert!(parse_key("invalid").is_err());
        let noncanonical = format!("{}b", "y".repeat(51));
        assert!(parse_key(&noncanonical).is_err());
        for name in [
            ".",
            "user@example.com",
            "bad.example/path",
            "bad.example:443",
        ] {
            if let Ok(name) = name.try_into() {
                assert!(authority(&SVCB::new(1, name)).is_none());
            }
        }
    }
}
