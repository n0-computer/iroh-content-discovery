//! Resolve and cache signed Pkarr records through the gateway's shared DHT.

use lru::LruCache;
use std::time::{Duration, Instant};

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
    Caching, Gateway, HttpError, LOOKUP_TIMEOUT, Root, Subdomain, parse_hash, parse_z32_bytes,
    serve_path, serve_root,
};

/// Targets under this suffix name content this gateway can serve itself.
const CONTENT_SUFFIX: &str = ".blake3.link";

// Bound both memory use and how long changed names can remain stale.
const MAX_CACHE_TTL: Duration = Duration::from_secs(30);

pub(crate) struct Cache(LruCache<[u8; 32], (Instant, n0_mainline::MutableItem)>);

impl Default for Cache {
    fn default() -> Self {
        Self(LruCache::new(1024.try_into().unwrap()))
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
    let rest = uri.path().strip_prefix("/pkarr/").unwrap();
    let (encoded, path) = rest
        .split_once('/')
        .map_or((rest, "/".to_owned()), |(key, path)| {
            (key, format!("/{path}"))
        });
    let key = parse_key(encoded)?;
    let started = std::time::Instant::now();
    tracing::debug!(key = encoded, "resolving Pkarr record");
    let cached = gateway.0.pkarr.lock().unwrap().get(&key, Instant::now());
    let cache_hit = cached.is_some();
    let item = if let Some(item) = cached {
        tracing::debug!(key = encoded, "Pkarr cache hit");
        item
    } else {
        tokio::time::timeout(LOOKUP_TIMEOUT, resolve(gateway.0.resolver.dht(), &key))
            .await
            .map_err(|_| {
                tracing::debug!(
                    key = encoded,
                    elapsed_ms = started.elapsed().as_millis(),
                    "Pkarr lookup timed out"
                );
                HttpError(StatusCode::GATEWAY_TIMEOUT, "Pkarr lookup timed out")
            })??
    };
    let packet = Packet::parse(item.value()).map_err(|error| {
        tracing::warn!(%error, "invalid Pkarr DNS packet");
        HttpError(StatusCode::BAD_GATEWAY, "invalid Pkarr DNS packet")
    })?;
    tracing::debug!(
        key = encoded,
        sequence = item.seq(),
        answers = packet.answers.len(),
        elapsed_ms = started.elapsed().as_millis(),
        "Pkarr DNS packet decoded"
    );
    if !cache_hit {
        gateway
            .0
            .pkarr
            .lock()
            .unwrap()
            .insert(key, item.clone(), &packet, Instant::now());
    }
    let authority = target(&packet, encoded).ok_or(HttpError(
        StatusCode::UNPROCESSABLE_ENTITY,
        "no supported apex HTTPS target",
    ))?;
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
            .map_err(|_| HttpError(StatusCode::BAD_REQUEST, "path is not valid UTF-8"))?
            .into_owned();
        return if path.is_empty() {
            serve_root(&gateway, root, hash, query, method, headers).await
        } else {
            serve_path(gateway, root, hash, path, query, method, headers).await
        };
    }
    let mut location = format!("https://{authority}{path}");
    if let Some(query) = uri.query() {
        location.push('?');
        location.push_str(query);
    }
    tracing::debug!(key = encoded, %location, "redirecting Pkarr request");
    Ok((
        StatusCode::TEMPORARY_REDIRECT,
        [
            (header::LOCATION, location),
            (header::CACHE_CONTROL, "no-store".to_owned()),
        ],
    )
        .into_response())
}

/// Returns the hash when a target names content, as `<z32>.blake3.link`.
fn content_hash(authority: &str) -> Option<iroh_blobs::Hash> {
    parse_hash(authority.strip_suffix(CONTENT_SUFFIX)?).ok()
}

fn parse_key(encoded: &str) -> Result<[u8; 32], HttpError> {
    parse_z32_bytes(encoded).map_err(|_| {
        HttpError(
            StatusCode::BAD_REQUEST,
            "invalid z-base-32 Pkarr public key",
        )
    })
}

async fn resolve(
    dht: &n0_mainline::Dht,
    key: &[u8; 32],
) -> Result<n0_mainline::MutableItem, HttpError> {
    let mut items = dht.get_mutable(key, None, None).await.map_err(|error| {
        tracing::warn!(%error, "Pkarr lookup failed");
        HttpError(StatusCode::BAD_GATEWAY, "Pkarr lookup failed")
    })?;
    first_verified(&mut items, key).await
}

async fn first_verified(
    items: &mut (impl n0_future::Stream<Item = n0_mainline::MutableItem> + Unpin),
    key: &[u8; 32],
) -> Result<n0_mainline::MutableItem, HttpError> {
    let item = items
        .next()
        .await
        .ok_or(HttpError(StatusCode::NOT_FOUND, "no Pkarr packet found"))?;
    tracing::debug!(
        sequence = item.seq(),
        bytes = item.value().len(),
        "received verified Pkarr item"
    );
    // get_mutable verifies BEP44 signatures and binds each item to the requested
    // key. Its value is the DNS wire packet; no Pkarr envelope is needed.
    if item.seq() < 0 || item.key() != key {
        return Err(HttpError(
            StatusCode::BAD_GATEWAY,
            "invalid Pkarr key or timestamp",
        ));
    }
    Ok(item)
}

fn target(packet: &Packet<'_>, key: &str) -> Option<String> {
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
    // keys require endpoint resolution rather than an HTTP redirect.
    if name.len() > 253
        || !name.contains('.')
        || !name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    {
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
        let key = n0_mainline::SigningKey::from_bytes(&[9; 32]);
        let mut packet = Packet::new_reply(0);
        packet.answers.push(ResourceRecord::new(
            "example.com".try_into().unwrap(),
            CLASS::IN,
            ttl,
            RData::HTTPS(HTTPS(SVCB::new(0, "target.example".try_into().unwrap()))),
        ));
        let bytes = packet.build_bytes_vec_compressed().unwrap();
        (n0_mainline::MutableItem::new(&key, &bytes, 1, None), bytes)
    }

    #[tokio::test]
    async fn first_record_does_not_wait_for_lookup_completion() {
        let (item, _) = signed_packet(300);
        let key = *item.key();
        let stream = async_stream::stream! {
            yield item;
            std::future::pending::<()>().await;
        };
        let mut stream = Box::pin(stream);
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            first_verified(&mut stream, &key),
        )
        .await;
        assert!(result.is_ok_and(|item| item.is_ok()));
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
