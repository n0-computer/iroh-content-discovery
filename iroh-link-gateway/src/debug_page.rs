//! `?debug`: what discovery finds right now, bypassing all caches.
//!
//! For a hash: serving stops at the first provider that passes a probe and
//! remembers what it found. This asks Mainline for peers, looks up every peer
//! in the address index and probes every endpoint found, so the page also
//! shows peers that did not resolve and providers that failed. Providers that
//! links named for the hash are listed and probed first.
//!
//! For a Pkarr key: every answer Mainline returns, the newest record in the
//! zone format the iroh-share GUI edits, and a link to the debug page of the
//! content it points to.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    net::{Ipv4Addr, Ipv6Addr, SocketAddrV4},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    http::header,
    response::{IntoResponse, Response},
};
use iroh::{Endpoint, EndpointId};
use iroh_blobs::Hash;
use iroh_mainline_endpoint_discovery::{AddrIndexError, SignedRecord, infohash_from_blake3};
use n0_future::{BufferedStreamExt, StreamExt, stream};
use simple_dns::{
    Packet,
    rdata::{RData, SVCB, SVCParam},
};
use tokio::time::Instant;

use crate::{Gateway, LISTING_CSS, html_escape, pkarr_redirect};

/// How long to keep collecting answers from Mainline.
const MAINLINE_TIMEOUT: Duration = Duration::from_secs(15);
/// Deadline for one probe, as when serving.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
const CONCURRENT_LOOKUPS: usize = 16;
const CONCURRENT_PROBES: usize = 8;
/// Gap between the starts of index lookups.
///
/// TODO: remove once the index servers run n0-mainline 0.7.1 or later, which
/// splits GRO batches (https://github.com/n0-computer/n0-mainline/pull/11).
/// Older Linux servers drop lookups that arrive back to back, since their
/// socket coalesces them into one datagram they cannot parse.
const LOOKUP_SPACING: Duration = Duration::from_millis(20);

/// One index record for a peer, or why there is none.
enum Resolution {
    Record(SignedRecord),
    NotInIndex,
    Failed(AddrIndexError),
}

/// Renders the providers page for `hash`.
pub(crate) async fn providers(gateway: &Gateway, hash: Hash) -> Response {
    let encoded = z32::encode(hash.as_bytes());
    let mut body = hinted_section(gateway, hash).await;
    body.push_str(&providers_section(gateway, hash).await);
    page(&format!("Providers of {encoded}"), &body)
}

/// Renders the providers that links named for `hash`, each probed, if there are any.
async fn hinted_section(gateway: &Gateway, hash: Hash) -> String {
    let hinted = gateway.0.hints.get(hash);
    if hinted.is_empty() {
        return String::new();
    }
    let probes: Vec<(EndpointId, Result<Duration, String>)> = stream::iter(hinted)
        .map(|provider| {
            let endpoint = gateway.0.endpoint.clone();
            async move { (provider, probe(&endpoint, hash, provider).await) }
        })
        .buffered_ordered(CONCURRENT_PROBES)
        .collect()
        .await;
    let mut html = format!(
        "<p class=\"meta\">{} providers named by links, asked before Mainline.</p>\n\
         <table>\n<tr><td>Endpoint</td><td class=\"size\">Probe</td></tr>\n",
        probes.len()
    );
    for (provider, probe) in &probes {
        let id = provider.to_string();
        let probe = match probe {
            Ok(latency) => format!("{} ms", latency.as_millis()),
            Err(error) => html_escape(error),
        };
        html.push_str(&format!(
            "<tr><td class=\"hash\"><span title=\"{id}\">{id}</span></td>\
             <td class=\"size\">{probe}</td></tr>\n"
        ));
    }
    html.push_str("</table>\n");
    html
}

/// Renders the Pkarr page for `key`, linking to its content target's debug page.
///
/// `host` is the request's `Host` header when it came in on a Pkarr
/// subdomain, used to link to the content's debug page on the same port.
pub(crate) async fn pkarr(
    gateway: &Gateway,
    key: &[u8; 32],
    encoded: &str,
    host: Option<&str>,
) -> Response {
    let started = Instant::now();
    let mut items = Vec::new();
    let lookup = async {
        let mut answers = gateway
            .0
            .resolver
            .dht()
            .get_mutable(key, None, None)
            .await
            .map_err(|error| error.to_string())?;
        while let Some(item) = answers.next().await {
            items.push(item);
        }
        Ok::<_, String>(())
    };
    let note = mainline_note(tokio::time::timeout(MAINLINE_TIMEOUT, lookup).await);
    let mut body = format!(
        "<p class=\"meta\">{} answers from Mainline in {:.1} s.</p>\n",
        items.len(),
        started.elapsed().as_secs_f64(),
    );
    if let Some(note) = note {
        body.push_str(&format!("<p class=\"meta\">{}</p>\n", html_escape(&note)));
    }
    // Several sequence numbers mean some nodes still hold an older record.
    let mut answers: BTreeMap<i64, usize> = BTreeMap::new();
    for item in &items {
        *answers.entry(item.seq()).or_default() += 1;
    }
    if !answers.is_empty() {
        body.push_str(
            "<table>\n<tr><td>Sequence</td><td class=\"size\">Published</td>\
             <td class=\"size\">Answers</td></tr>\n",
        );
        for (seq, count) in answers.iter().rev() {
            body.push_str(&format!(
                "<tr><td>{seq}</td><td class=\"size\">{}</td><td class=\"size\">{count}</td></tr>\n",
                published(*seq)
            ));
        }
        body.push_str("</table>\n");
    }
    let newest = items
        .iter()
        .filter(|item| item.seq() >= 0 && item.key() == key)
        .max_by_key(|item| item.seq());
    let mut content = None;
    match newest {
        None => body.push_str("<p>No record found.</p>\n"),
        Some(item) => {
            match text(encoded, item.value()) {
                Ok(text) => body.push_str(&format!(
                    "<h1>Record</h1>\n<pre>{}</pre>\n",
                    html_escape(&text)
                )),
                Err(error) => body.push_str(&format!(
                    "<p>Cannot show the record: {}</p>\n",
                    html_escape(&error)
                )),
            }
            content = Packet::parse(item.value())
                .ok()
                .and_then(|packet| pkarr_redirect::target(&packet, encoded))
                .and_then(|authority| pkarr_redirect::content_hash(&authority));
        }
    }
    if let Some(hash) = content {
        let hash = z32::encode(hash.as_bytes());
        // A subdomain request links to the content's own origin on the same
        // port; a path request stays on this origin.
        let link = match host {
            Some(host) => {
                let port = host
                    .rsplit_once(':')
                    .map_or(String::new(), |(_, port)| format!(":{port}"));
                format!("http://{hash}.blake3.localhost{port}/?debug")
            }
            None => format!("/blake3/{hash}/?debug"),
        };
        body.push_str(&format!(
            "<p><a href=\"{}\">Providers of {hash}</a></p>\n",
            html_escape(&link)
        ));
    }
    page(&format!("Pkarr {encoded}"), &body)
}

/// Describes when a sequence number was published, if it is a timestamp.
///
/// Pkarr publishers use microseconds since the Unix epoch.
fn published(seq: i64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_micros() as i64);
    // Anything before 2017 is not a microsecond timestamp.
    if seq < 1_500_000_000_000_000 {
        return String::new();
    }
    format!(
        "{} ago",
        format_age((now.saturating_sub(seq) / 1_000_000) as u64)
    )
}

/// Renders every peer Mainline returns for `hash`, with its index records and probes.
async fn providers_section(gateway: &Gateway, hash: Hash) -> String {
    let started = Instant::now();
    let (peers, peers_note) = peers(gateway, hash).await;
    let lookups: Vec<(SocketAddrV4, Resolution)> = stream::iter(peers.iter().copied())
        // TODO: drop with LOOKUP_SPACING.
        .then(|peer| async move {
            tokio::time::sleep(LOOKUP_SPACING).await;
            peer
        })
        .map(|peer| {
            let index = gateway.0.resolver.index().clone();
            async move {
                let resolutions = match index.lookup_uncached(peer).await {
                    Ok(records) if records.is_empty() => vec![Resolution::NotInIndex],
                    Ok(records) => records.into_iter().map(Resolution::Record).collect(),
                    Err(error) => vec![Resolution::Failed(error)],
                };
                resolutions
                    .into_iter()
                    .map(move |r| (peer, r))
                    .collect::<Vec<_>>()
            }
        })
        .buffered_unordered(CONCURRENT_LOOKUPS)
        .flat_map(stream::iter)
        .collect()
        .await;
    let endpoints: BTreeSet<EndpointId> = lookups
        .iter()
        .filter_map(|(_, resolution)| match resolution {
            Resolution::Record(record) => Some(record.endpoint_id),
            _ => None,
        })
        .collect();
    let probes: HashMap<EndpointId, Result<Duration, String>> = stream::iter(endpoints)
        .map(|provider| {
            let endpoint = gateway.0.endpoint.clone();
            async move { (provider, probe(&endpoint, hash, provider).await) }
        })
        .buffered_unordered(CONCURRENT_PROBES)
        .collect()
        .await;
    render_providers(&peers, peers_note, lookups, &probes, started.elapsed())
}

/// Collects the peers Mainline returns for `hash`, and a note if that stopped early.
async fn peers(gateway: &Gateway, hash: Hash) -> (BTreeSet<SocketAddrV4>, Option<String>) {
    let infohash = infohash_from_blake3(&blake3::Hash::from_bytes(*hash.as_bytes()));
    let mut peers = BTreeSet::new();
    let lookup = async {
        let mut batches = gateway
            .0
            .resolver
            .dht()
            .get_peers(infohash.into())
            .await
            .map_err(|error| error.to_string())?;
        while let Some(batch) = batches.next().await {
            peers.extend(batch);
        }
        Ok::<_, String>(())
    };
    let note = mainline_note(tokio::time::timeout(MAINLINE_TIMEOUT, lookup).await);
    (peers, note)
}

/// Describes a Mainline lookup that did not run to completion.
fn mainline_note(
    result: Result<Result<(), String>, tokio::time::error::Elapsed>,
) -> Option<String> {
    match result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(format!("Mainline lookup failed: {error}")),
        Err(_) => Some(format!(
            "Stopped collecting answers after {} s.",
            MAINLINE_TIMEOUT.as_secs()
        )),
    }
}

/// Returns how long `provider` took to serve a verified size for `hash`, or why it did not.
async fn probe(endpoint: &Endpoint, hash: Hash, provider: EndpointId) -> Result<Duration, String> {
    let started = Instant::now();
    let attempt = async {
        let connection = endpoint.connect(provider, iroh_blobs::ALPN).await?;
        crate::verified_size(&connection, hash).await
    };
    match tokio::time::timeout(PROBE_TIMEOUT, attempt).await {
        Ok(Ok(_size)) => Ok(started.elapsed()),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err(format!("timed out after {} s", PROBE_TIMEOUT.as_secs())),
    }
}

fn render_providers(
    peers: &BTreeSet<SocketAddrV4>,
    peers_note: Option<String>,
    mut rows: Vec<(SocketAddrV4, Resolution)>,
    probes: &HashMap<EndpointId, Result<Duration, String>>,
    elapsed: Duration,
) -> String {
    // Working providers first, fastest first, then failed probes, then peers
    // without a record.
    let rank = |resolution: &Resolution| match resolution {
        Resolution::Record(record) => match probes.get(&record.endpoint_id) {
            Some(Ok(latency)) => (0, *latency),
            _ => (1, Duration::ZERO),
        },
        Resolution::NotInIndex => (2, Duration::ZERO),
        Resolution::Failed(_) => (3, Duration::ZERO),
    };
    rows.sort_by(|(a_peer, a), (b_peer, b)| rank(a).cmp(&rank(b)).then(a_peer.cmp(b_peer)));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    let mut html = format!(
        "<p class=\"meta\">{} peers from Mainline, {} endpoints, in {:.1} s.</p>\n",
        peers.len(),
        probes.len(),
        elapsed.as_secs_f64(),
    );
    if let Some(note) = peers_note {
        html.push_str(&format!("<p class=\"meta\">{}</p>\n", html_escape(&note)));
    }
    html.push_str(
        "<table>\n<tr><td>Peer</td><td>Endpoint</td><td class=\"size\">Record age</td>\
         <td class=\"size\">Probe</td></tr>\n",
    );
    for (peer, resolution) in &rows {
        let (endpoint, age, probe) = match resolution {
            Resolution::Record(record) => {
                let id = record.endpoint_id.to_string();
                let age = now.saturating_sub(record.payload.v1().ts);
                let probe = match probes.get(&record.endpoint_id) {
                    Some(Ok(latency)) => format!("{} ms", latency.as_millis()),
                    Some(Err(error)) => html_escape(error),
                    None => String::new(),
                };
                (
                    format!("<span title=\"{id}\">{id}</span>"),
                    format_age(age),
                    probe,
                )
            }
            Resolution::NotInIndex => ("not in index".into(), String::new(), String::new()),
            Resolution::Failed(error) => (
                html_escape(&format!("index lookup failed: {error}")),
                String::new(),
                String::new(),
            ),
        };
        html.push_str(&format!(
            "<tr><td>{peer}</td><td class=\"hash\">{endpoint}</td>\
             <td class=\"size\">{age}</td><td class=\"size\">{probe}</td></tr>\n"
        ));
    }
    html.push_str("</table>\n");
    html
}

fn page(title: &str, body: &str) -> Response {
    let title = html_escape(title);
    let html = format!(
        "<!DOCTYPE html>\n<html lang=\"en\">\n<meta charset=\"utf-8\">\n\
         <meta name=\"color-scheme\" content=\"light dark\">\n\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
         <title>{title}</title>\n<style>{LISTING_CSS}</style>\n<h1>{title}</h1>\n{body}"
    );
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        html,
    )
        .into_response()
}

fn format_age(seconds: u64) -> String {
    match seconds {
        0..60 => format!("{seconds} s"),
        60..3600 => format!("{} min", seconds / 60),
        _ => format!("{} h", seconds / 3600),
    }
}

/// Renders a DNS packet as the zone-style lines the iroh-share GUI edits.
///
/// Follows `dns_records::text` in iroh-share, so the records it writes (HTTPS,
/// URI, TXT, A, AAAA) look the same in both. Types simple-dns does not know
/// are shown in the generic RFC 3597 form.
fn text(key: &str, bytes: &[u8]) -> Result<String, String> {
    use std::fmt::Write;
    let packet = Packet::parse(bytes).map_err(|e| e.to_string())?;
    let suffix = format!(".{key}");
    let mut text = String::new();
    for record in &packet.answers {
        let name = record.name.to_string();
        let owner = if name.eq_ignore_ascii_case(key) {
            "@".to_owned()
        } else {
            let cut = name.len().checked_sub(suffix.len());
            match cut.and_then(|cut| Some((name.get(..cut)?, name.get(cut..)?))) {
                Some((label, rest)) if !label.is_empty() && rest.eq_ignore_ascii_case(&suffix) => {
                    label.to_owned()
                }
                _ => return Err("record owner must be within this pkarr name".into()),
            }
        };
        let ttl = record.ttl;
        let (kind, data) = match &record.rdata {
            RData::A(a) => ("A".to_owned(), Ipv4Addr::from(a.address).to_string()),
            RData::AAAA(a) => ("AAAA".to_owned(), Ipv6Addr::from(a.address).to_string()),
            RData::CNAME(cname) => ("CNAME".to_owned(), format!("{}.", cname.0)),
            RData::TXT(txt) => {
                let parts: Vec<_> = txt
                    .iter_raw()
                    .map(|(key, value)| {
                        let mut part = key.to_vec();
                        if let Some(value) = value {
                            part.push(b'=');
                            part.extend_from_slice(value);
                        }
                        quote(&part)
                    })
                    .collect();
                ("TXT".to_owned(), parts.join(" "))
            }
            RData::HTTPS(https) => ("HTTPS".to_owned(), svcb(&https.0)),
            RData::SVCB(svcb_data) => ("SVCB".to_owned(), svcb(svcb_data)),
            // URI (RFC 7553), unknown to simple-dns: priority, weight, target.
            RData::NULL(256, uri) => {
                let (header, target) = uri
                    .get_data()
                    .split_first_chunk::<4>()
                    .ok_or("URI record is too short")?;
                let priority = u16::from_be_bytes([header[0], header[1]]);
                let weight = u16::from_be_bytes([header[2], header[3]]);
                (
                    "URI".to_owned(),
                    format!("{priority} {weight} {}", quote(target)),
                )
            }
            RData::NULL(code, data) => {
                let data = data.get_data();
                let hex: String = data.iter().map(|byte| format!("{byte:02x}")).collect();
                (format!("TYPE{code}"), format!("\\# {} {hex}", data.len()))
            }
            other => {
                writeln!(
                    text,
                    "; {owner} {ttl} IN {:?} (not shown)",
                    other.type_code()
                )
                .expect("writing to a String");
                continue;
            }
        };
        writeln!(text, "{owner} {ttl} IN {kind} {data}").expect("writing to a String");
    }
    Ok(text)
}

/// Renders SVCB or HTTPS data as `priority target. key=value ...`.
fn svcb(svcb: &SVCB<'_>) -> String {
    let join = |items: Vec<String>| items.join(",");
    let mut out = format!("{} {}.", svcb.priority, svcb.target);
    for param in svcb.iter_params() {
        out.push(' ');
        out.push_str(&match param {
            SVCParam::Port(port) => format!("port={port}"),
            SVCParam::Alpn(ids) => format!(
                "alpn={}",
                join(ids.iter().map(ToString::to_string).collect())
            ),
            SVCParam::NoDefaultAlpn => "no-default-alpn".to_owned(),
            SVCParam::Mandatory(keys) => {
                format!(
                    "mandatory={}",
                    join(keys.iter().map(|key| format!("key{key}")).collect())
                )
            }
            SVCParam::Ipv4Hint(ips) => format!(
                "ipv4hint={}",
                join(
                    ips.iter()
                        .map(|ip| Ipv4Addr::from(*ip).to_string())
                        .collect()
                )
            ),
            SVCParam::Ipv6Hint(ips) => format!(
                "ipv6hint={}",
                join(
                    ips.iter()
                        .map(|ip| Ipv6Addr::from(*ip).to_string())
                        .collect()
                )
            ),
            other => format!("key{}", other.key_code()),
        });
    }
    out
}

/// Quotes a character string, escaping what the zone parser would interpret.
fn quote(bytes: &[u8]) -> String {
    let mut quoted = String::from('"');
    for &byte in bytes {
        match byte {
            b'"' | b'\\' => {
                quoted.push('\\');
                quoted.push(byte.into());
            }
            0x20..=0x7e => quoted.push(byte.into()),
            _ => quoted.push_str(&format!("\\{byte:03}")),
        }
    }
    quoted.push('"');
    quoted
}

#[cfg(test)]
mod tests {
    use simple_dns::{
        CLASS, Name as DnsName, ResourceRecord,
        rdata::{A, HTTPS, NULL, RData as DnsRData, TXT},
    };

    use super::*;

    const KEY: &str = "5ti57aszf7kaicsncb4wgigkf9bju39kofiz8dthwdujkmz85u8y";

    #[test]
    fn records_render_like_the_iroh_share_gui() {
        let apex = DnsName::new_unchecked(KEY);
        let uri_owner = format!("_https._tcp.{KEY}");
        let mut with_port = SVCB::new(1, DnsName::new_unchecked("example.com"));
        with_port.set_port(8443);
        let mut uri = 0u16.to_be_bytes().to_vec();
        uri.extend(0u16.to_be_bytes());
        uri.extend(b"https://example.com/a?b");
        let mut packet = Packet::new_reply(0);
        packet.answers = vec![
            ResourceRecord::new(
                apex.clone(),
                CLASS::IN,
                300,
                DnsRData::HTTPS(HTTPS(SVCB::new(0, DnsName::new_unchecked("example.com")))),
            ),
            ResourceRecord::new(apex, CLASS::IN, 300, DnsRData::HTTPS(HTTPS(with_port))),
            ResourceRecord::new(
                DnsName::new_unchecked(&uri_owner),
                CLASS::IN,
                300,
                DnsRData::NULL(256, NULL::new(&uri).unwrap()),
            ),
        ];
        packet.answers.push(ResourceRecord::new(
            DnsName::new_unchecked(KEY),
            CLASS::IN,
            300,
            DnsRData::A(A {
                address: u32::from(Ipv4Addr::new(192, 0, 2, 1)),
            }),
        ));
        packet.answers.push(ResourceRecord::new(
            DnsName::new_unchecked(KEY),
            CLASS::IN,
            300,
            DnsRData::TXT(TXT::new().with_string("hello \"world\"").unwrap()),
        ));
        let bytes = packet.build_bytes_vec().unwrap();
        assert_eq!(
            text(KEY, &bytes).unwrap(),
            "@ 300 IN HTTPS 0 example.com.\n\
             @ 300 IN HTTPS 1 example.com. port=8443\n\
             _https._tcp 300 IN URI 0 0 \"https://example.com/a?b\"\n\
             @ 300 IN A 192.0.2.1\n\
             @ 300 IN TXT \"hello \\\"world\\\"\"\n"
        );
    }
}
