//! Resolve signed Pkarr HTTPS records through the gateway's shared DHT.

use axum::{
    extract::{OriginalUri, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use n0_future::StreamExt;
use simple_dns::{
    CLASS, Packet,
    rdata::{RData, SVCB, SVCParam},
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
    let item = tokio::time::timeout(LOOKUP_TIMEOUT, resolve(gateway.0.resolver.dht(), &key))
        .await
        .map_err(|_| HttpError(StatusCode::GATEWAY_TIMEOUT, "Pkarr lookup timed out"))??;
    let packet = Packet::parse(item.value()).map_err(|error| {
        tracing::warn!(%error, "invalid Pkarr DNS packet");
        HttpError(StatusCode::BAD_GATEWAY, "invalid Pkarr DNS packet")
    })?;
    let authority = target(&packet, encoded).ok_or(HttpError(
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

fn parse_key(encoded: &str) -> Result<[u8; 32], HttpError> {
    let invalid = || {
        HttpError(
            StatusCode::BAD_REQUEST,
            "invalid z-base-32 Pkarr public key",
        )
    };
    if encoded.len() != 52 {
        return Err(invalid());
    }
    let key: [u8; 32] = z32::decode(encoded.as_bytes())
        .map_err(|_| invalid())?
        .try_into()
        .map_err(|_| invalid())?;
    if z32::encode(&key) != encoded {
        return Err(invalid());
    }
    Ok(key)
}

async fn resolve(
    dht: &n0_mainline::Dht,
    key: &[u8; 32],
) -> Result<n0_mainline::MutableItem, HttpError> {
    let mut items = dht.get_mutable(key, None, None).await.map_err(|error| {
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
