//! Resolve signed Pkarr HTTPS records through the gateway's shared DHT.

use axum::{
    extract::{OriginalUri, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use n0_future::StreamExt;
use pkarr::{
    PublicKey, SignedPacket,
    dns::rdata::{RData, SVCB},
};

use crate::{Gateway, HttpError, LOOKUP_TIMEOUT};

pub(crate) async fn redirect(
    State(gateway): State<Gateway>,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, HttpError> {
    // Keep the original escaping, including encoded slashes and query values.
    let rest = uri.path().strip_prefix("/pkarr/").unwrap();
    let (encoded, path) = rest
        .split_once('/')
        .map_or((rest, "/".to_owned()), |(key, path)| {
            (key, format!("/{path}"))
        });
    let key = parse_key(encoded)?;
    let packet = tokio::time::timeout(LOOKUP_TIMEOUT, resolve(gateway.0.resolver.dht(), &key))
        .await
        .map_err(|_| HttpError(StatusCode::GATEWAY_TIMEOUT, "Pkarr lookup timed out"))??;
    let authority = target(&packet).ok_or(HttpError(
        StatusCode::UNPROCESSABLE_ENTITY,
        "no supported apex HTTPS target",
    ))?;
    let mut location = format!("https://{authority}{path}");
    if let Some(query) = uri.query() {
        location.push('?');
        location.push_str(query);
    }
    Ok((
        StatusCode::TEMPORARY_REDIRECT,
        [
            (header::LOCATION, location),
            (header::CACHE_CONTROL, "no-store".to_owned()),
        ],
    )
        .into_response())
}

fn parse_key(encoded: &str) -> Result<PublicKey, HttpError> {
    let invalid = || {
        HttpError(
            StatusCode::BAD_REQUEST,
            "invalid z-base-32 Pkarr public key",
        )
    };
    let key: PublicKey = encoded.parse().map_err(|_| invalid())?;
    if key.to_z32() != encoded {
        return Err(invalid());
    }
    Ok(key)
}

async fn resolve(dht: &n0_mainline::Dht, key: &PublicKey) -> Result<SignedPacket, HttpError> {
    let mut items = dht
        .get_mutable(key.as_bytes(), None, None)
        .await
        .map_err(|error| {
            tracing::warn!(%error, "Pkarr lookup failed");
            HttpError(StatusCode::BAD_GATEWAY, "Pkarr lookup failed")
        })?;
    let mut latest: Option<n0_mainline::MutableItem> = None;
    while let Some(item) = items.next().await {
        if latest
            .as_ref()
            .is_none_or(|old| (item.seq(), item.value()) > (old.seq(), old.value()))
        {
            latest = Some(item);
        }
    }
    let item = latest.ok_or(HttpError(StatusCode::NOT_FOUND, "no Pkarr packet found"))?;
    verified_packet(key, &item).map_err(|error| {
        tracing::warn!(%error, "invalid Pkarr packet");
        HttpError(StatusCode::BAD_GATEWAY, "invalid signed Pkarr DNS packet")
    })
}

fn verified_packet(
    key: &PublicKey,
    item: &n0_mainline::MutableItem,
) -> anyhow::Result<SignedPacket> {
    anyhow::ensure!(
        item.seq() >= 0 && item.key() == key.as_bytes(),
        "invalid Pkarr key or timestamp"
    );
    let mut payload = Vec::with_capacity(72 + item.value().len());
    payload.extend_from_slice(item.signature());
    payload.extend_from_slice(&item.seq().to_be_bytes());
    payload.extend_from_slice(item.value());
    Ok(SignedPacket::from_relay_payload(key, &payload.into())?)
}

fn target(packet: &SignedPacket) -> Option<String> {
    packet
        .resource_records("@")
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
        // A browser redirect cannot convey mandatory SVCB parameters.
        if svcb.get_param(SVCB::MANDATORY).is_some()
            || svcb.get_param(SVCB::NO_DEFAULT_ALPN).is_some()
        {
            return None;
        }
        if let Some(port) = svcb.get_param(SVCB::PORT) {
            let port = u16::from_be_bytes(port.try_into().ok()?);
            if port == 0 {
                return None;
            }
            if port != 443 {
                target.push_str(&format!(":{port}"));
            }
        }
    }
    Some(target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_targets_and_validation() {
        let key = pkarr::Keypair::from_secret_key(&[7; 32]);
        let mut preferred = SVCB::new(1, "example.com".try_into().unwrap());
        preferred.set_port(8443);
        let packet = SignedPacket::builder()
            .https(
                ".".try_into().unwrap(),
                SVCB::new(2, "other.example".try_into().unwrap()),
                60,
            )
            .https(".".try_into().unwrap(), preferred, 60)
            .https(
                "unrelated".try_into().unwrap(),
                SVCB::new(0, "wrong.example".try_into().unwrap()),
                60,
            )
            .sign(&key)
            .unwrap();
        assert_eq!(target(&packet).as_deref(), Some("example.com:8443"));
        let item = n0_mainline::MutableItem::new_signed_unchecked(
            *key.public_key().as_bytes(),
            packet.signature().to_bytes(),
            &packet.encoded_packet(),
            packet.timestamp().as_u64() as i64,
            None,
        );
        assert!(verified_packet(&key.public_key(), &item).is_ok());
        let corrupt = n0_mainline::MutableItem::new_signed_unchecked(
            *key.public_key().as_bytes(),
            [0; 64],
            item.value(),
            item.seq(),
            None,
        );
        assert!(verified_packet(&key.public_key(), &corrupt).is_err());
        assert!(parse_key(&key.public_key().to_z32()).is_ok());
        assert!(parse_key("invalid").is_err());
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
