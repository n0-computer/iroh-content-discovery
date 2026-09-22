//! Local HTTP delivery of Bao-verified blobs discovered through Mainline.
//!
//! Adapted from the streaming approach in `iroh-examples/iroh-gateway`.
//! Each content lookup chooses one signed endpoint. Data is streamed directly
//! from that peer, without downloading a whole blob into memory or a store.

use std::{
    net::SocketAddr,
    ops::Range,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::bail;
use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use bao_tree::{ChunkNum, ChunkRanges, io::fsm::BaoContentItem};
use bytes::Bytes;
use iroh::{Endpoint, endpoint::Connection};
use iroh_blobs::{
    Hash,
    format::collection::Collection,
    get::fsm::{self, AtBlobContent, BlobContentNext, ConnectedNext, EndBlobNext},
    protocol::{ChunkRangesExt, ChunkRangesSeq, GetRequest},
};
use iroh_mainline_endpoint_discovery::{Resolver, infohash_from_blake3};
use lru::LruCache;
use mime_classifier::MimeClassifier;
use n0_future::StreamExt;
use tower_http::cors::{Any, CorsLayer};

mod ranges;
use ranges::Selection;

const LOOKUP_TIMEOUT: Duration = Duration::from_secs(60);
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const SNIFF_BYTES: u64 = 8192;

/// An HTTP gateway using a caller-owned iroh endpoint and content resolver.
#[derive(Clone)]
pub struct Gateway(Arc<Inner>);

struct Inner {
    endpoint: Endpoint,
    resolver: Resolver,
    classifier: MimeClassifier,
    // Reuse one peer for repeated video seeks, with bounded metadata memory.
    cache: Mutex<LruCache<Hash, Source>>,
    collections: Mutex<LruCache<Hash, Arc<Collection>>>,
}

#[derive(Clone)]
struct Source {
    connection: Connection,
    size: u64,
    mime: String,
}

impl Gateway {
    /// Construct a gateway. The endpoint is used to dial resolved endpoint IDs.
    pub fn new(endpoint: Endpoint, resolver: Resolver) -> Self {
        Self(Arc::new(Inner {
            endpoint,
            resolver,
            classifier: MimeClassifier::new(),
            cache: Mutex::new(LruCache::new(128.try_into().unwrap())),
            collections: Mutex::new(LruCache::new(32.try_into().unwrap())),
        }))
    }

    /// HTTP routes for `GET` and `HEAD` plus CORS preflight.
    ///
    /// `/blake3/{z32}` serves a blob. `/tree/{z32}` lists the top level of a
    /// collection, and `/tree/{z32}/{path}` serves a file of it or lists a
    /// directory, where directories are the `/`-separated prefixes of names.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/blake3/{hash}", get(blob))
            .route("/tree/{hash}", get(tree_root))
            .route("/tree/{hash}/", get(tree_root))
            .route("/tree/{hash}/{*path}", get(tree))
            .layer(
                CorsLayer::new()
                    .allow_origin(Any)
                    .allow_methods([Method::GET, Method::HEAD, Method::OPTIONS])
                    .allow_headers([header::RANGE, header::IF_RANGE, header::IF_NONE_MATCH])
                    .expose_headers([
                        header::ACCEPT_RANGES,
                        header::CONTENT_RANGE,
                        header::CONTENT_LENGTH,
                        header::ETAG,
                    ]),
            )
            .with_state(self.clone())
    }

    /// Serve plaintext HTTP on a loopback socket until the shutdown future resolves.
    pub async fn serve(
        &self,
        listener: tokio::net::TcpListener,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> anyhow::Result<()> {
        validate_listen_addr(listener.local_addr()?)?;
        axum::serve(listener, self.router())
            .with_graceful_shutdown(shutdown)
            .await?;
        Ok(())
    }

    /// Returns the cached or newly probed source for `hash`.
    ///
    /// Dials a peer found through discovery unless `via` supplies a
    /// connection, which is used for files of an already open collection.
    async fn source(&self, hash: Hash, via: Option<&Connection>) -> Result<Source, HttpError> {
        let cached = self.0.cache.lock().unwrap().get(&hash).cloned();
        if let Some(source) = cached
            && source.connection.close_reason().is_none()
        {
            return Ok(source);
        }
        let connection = match via {
            Some(connection) => connection.clone(),
            None => self.connect(hash).await?,
        };
        let (size, prefix) = sniff(&connection, hash)
            .await
            .map_err(HttpError::upstream)?;
        let mime = self
            .0
            .classifier
            .classify(
                mime_classifier::LoadContext::Browsing,
                mime_classifier::NoSniffFlag::Off,
                mime_classifier::ApacheBugFlag::Off,
                &None,
                &prefix,
            )
            .to_string();
        let source = Source {
            connection,
            size,
            mime,
        };
        self.0.cache.lock().unwrap().put(hash, source.clone());
        Ok(source)
    }

    async fn connect(&self, hash: Hash) -> Result<Connection, HttpError> {
        let infohash = infohash_from_blake3(&blake3::Hash::from_bytes(*hash.as_bytes()));
        let peer = self
            .0
            .resolver
            .resolve_one(infohash.into())
            .await
            .map_err(HttpError::upstream)?
            .ok_or(HttpError(
                StatusCode::NOT_FOUND,
                "no peer found for this hash",
            ))?;
        self.0
            .endpoint
            .connect(peer, iroh_blobs::ALPN)
            .await
            .map_err(HttpError::upstream)
    }

    /// Returns the collection rooted at `hash` and a connection to its peer.
    async fn collection(&self, hash: Hash) -> Result<(Arc<Collection>, Connection), HttpError> {
        let source = self.source(hash, None).await?;
        let cached = self.0.collections.lock().unwrap().get(&hash).cloned();
        if let Some(collection) = cached {
            return Ok((collection, source.connection));
        }
        let collection = Arc::new(read_collection(&source.connection, hash).await.map_err(
            |error| {
                tracing::debug!(%error, "read collection");
                HttpError(StatusCode::UNPROCESSABLE_ENTITY, "not a collection")
            },
        )?);
        self.0
            .collections
            .lock()
            .unwrap()
            .put(hash, collection.clone());
        Ok((collection, source.connection))
    }
}

/// Reject non-loopback HTTP listeners.
pub fn validate_listen_addr(addr: SocketAddr) -> anyhow::Result<()> {
    anyhow::ensure!(
        addr.ip().is_loopback(),
        "HTTP gateway must bind a loopback address"
    );
    Ok(())
}

/// Parse a canonical, lowercase z-base-32 encoded 32-byte BLAKE3 hash.
pub fn parse_hash(value: &str) -> anyhow::Result<Hash> {
    anyhow::ensure!(value.len() == 52, "expected 52 z-base-32 characters");
    let bytes: [u8; 32] = z32::decode(value.as_bytes())?
        .try_into()
        .map_err(|_| anyhow::anyhow!("expected a 32-byte hash"))?;
    anyhow::ensure!(z32::encode(&bytes) == value, "noncanonical z-base-32 hash");
    Ok(Hash::from_bytes(bytes))
}

struct HttpError(StatusCode, &'static str);

impl HttpError {
    fn upstream(error: impl std::fmt::Display) -> Self {
        tracing::warn!(%error, "gateway upstream failed");
        Self(StatusCode::BAD_GATEWAY, "peer lookup or transfer failed")
    }

    fn timeout(_: tokio::time::error::Elapsed) -> Self {
        Self(
            StatusCode::GATEWAY_TIMEOUT,
            "peer lookup or transfer timed out",
        )
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        (self.0, [(header::CACHE_CONTROL, "no-store")], self.1).into_response()
    }
}

fn parse_path_hash(encoded: &str) -> Result<Hash, HttpError> {
    parse_hash(encoded)
        .map_err(|_| HttpError(StatusCode::BAD_REQUEST, "invalid z-base-32 BLAKE3 hash"))
}

async fn blob(
    State(gateway): State<Gateway>,
    Path(encoded): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    let hash = parse_path_hash(&encoded)?;
    let source = tokio::time::timeout(LOOKUP_TIMEOUT, gateway.source(hash, None))
        .await
        .map_err(HttpError::timeout)??;
    serve(source, hash, &method, &headers).await
}

async fn tree_root(
    state: State<Gateway>,
    Path(encoded): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    tree(state, Path((encoded, String::new())), method, headers).await
}

async fn tree(
    State(gateway): State<Gateway>,
    Path((encoded, path)): Path<(String, String)>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    let root = parse_path_hash(&encoded)?;
    let (collection, connection) = tokio::time::timeout(LOOKUP_TIMEOUT, gateway.collection(root))
        .await
        .map_err(HttpError::timeout)??;
    let file = collection
        .iter()
        .find(|(name, _)| *name == path)
        .map(|(_, hash)| *hash);
    if let Some(hash) = file {
        let source = tokio::time::timeout(LOOKUP_TIMEOUT, gateway.source(hash, Some(&connection)))
            .await
            .map_err(HttpError::timeout)??;
        return serve(source, hash, &method, &headers).await;
    }
    let dir = if path.is_empty() || path.ends_with('/') {
        path
    } else {
        format!("{path}/")
    };
    let html = listing(&encoded, &dir, &collection).ok_or(HttpError(
        StatusCode::NOT_FOUND,
        "no such file or directory in collection",
    ))?;
    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::CACHE_CONTROL, "public, max-age=31536000, immutable")
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .body(Body::from(html))
        .unwrap())
}

/// Renders the HTML listing of directory `dir` in the collection at `root`.
///
/// `dir` is empty for the top level and otherwise ends with `/`. Returns
/// `None` if no name in the collection starts with `dir`.
fn listing(root: &str, dir: &str, collection: &Collection) -> Option<String> {
    let mut dirs = std::collections::BTreeSet::new();
    let mut files = Vec::new();
    for (name, hash) in collection.iter() {
        let Some(rest) = name.strip_prefix(dir) else {
            continue;
        };
        match rest.split_once('/') {
            Some((sub, _)) => {
                dirs.insert(sub);
            }
            None => files.push((rest, hash)),
        }
    }
    if !dir.is_empty() && dirs.is_empty() && files.is_empty() {
        return None;
    }
    let title = html_escape(&format!("{root}/{dir}"));
    let link = |path: &str| html_escape(&percent_encode_path(&format!("/tree/{root}/{path}")));
    let mut html = format!(
        "<!DOCTYPE html>\n<meta charset=\"utf-8\">\n<title>{title}</title>\n<h1>{title}</h1>\n<ul>\n"
    );
    if let Some(trimmed) = dir.strip_suffix('/') {
        let parent = trimmed.rsplit_once('/').map_or("", |(parent, _)| parent);
        let parent = if parent.is_empty() {
            String::new()
        } else {
            format!("{parent}/")
        };
        html.push_str(&format!("<li><a href=\"{}\">../</a></li>\n", link(&parent)));
    }
    for sub in dirs {
        html.push_str(&format!(
            "<li><a href=\"{}\">{}/</a></li>\n",
            link(&format!("{dir}{sub}/")),
            html_escape(sub),
        ));
    }
    for (file, hash) in files {
        html.push_str(&format!(
            "<li><a href=\"{}\">{}</a> <code>{}</code></li>\n",
            link(&format!("{dir}{file}")),
            html_escape(file),
            z32::encode(hash.as_bytes()),
        ));
    }
    html.push_str("</ul>\n");
    Some(html)
}

fn html_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

/// Percent-encodes everything except unreserved characters and `/`.
fn percent_encode_path(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~/".contains(&byte) {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

async fn serve(
    source: Source,
    hash: Hash,
    method: &Method,
    headers: &HeaderMap,
) -> Result<Response, HttpError> {
    let etag = format!("\"{}\"", z32::encode(hash.as_bytes()));
    let builder = Response::builder()
        .header(header::CONTENT_TYPE, &source.mime)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::ETAG, &etag)
        .header(header::CACHE_CONTROL, "public, max-age=31536000, immutable")
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff");
    if headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .filter_map(|h| h.to_str().ok())
        .flat_map(|h| h.split(','))
        .any(|h| h.trim() == "*" || h.trim().strip_prefix("W/").unwrap_or(h.trim()) == etag)
    {
        return Ok(builder
            .status(StatusCode::NOT_MODIFIED)
            .body(Body::empty())
            .unwrap());
    }
    let selection = if *method == Method::HEAD
        || headers
            .get(header::IF_RANGE)
            .is_some_and(|value| value.as_bytes() != etag.as_bytes())
    {
        Selection::Full
    } else {
        // Duplicate Range headers are ignored like multipart requests.
        let range = if headers.get_all(header::RANGE).iter().count() == 1 {
            headers.get(header::RANGE).and_then(|h| h.to_str().ok())
        } else {
            None
        };
        ranges::select(range, source.size)
    };
    let (range, builder) = match selection {
        Selection::Unsatisfiable => {
            return Ok(Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(header::CONTENT_RANGE, format!("bytes */{}", source.size))
                .header(header::ACCEPT_RANGES, "bytes")
                .header(header::CACHE_CONTROL, "no-store")
                .body(Body::empty())
                .unwrap());
        }
        Selection::Full => (0..source.size, builder.status(StatusCode::OK)),
        Selection::Partial(ranges) if ranges.len() > 1 => {
            let mut builder = builder;
            builder.headers_mut().unwrap().remove(header::CONTENT_TYPE);
            let boundary = format!("iroh-{:032x}", rand::random::<u128>());
            let length = multipart_length(&source, &ranges, &boundary).ok_or(HttpError(
                StatusCode::BAD_REQUEST,
                "multipart response too large",
            ))?;
            return Ok(builder
                .status(StatusCode::PARTIAL_CONTENT)
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/byteranges; boundary={boundary}"),
                )
                .header(header::CONTENT_LENGTH, length)
                .body(Body::from_stream(multipart_content(
                    source, hash, ranges, boundary,
                )))
                .unwrap());
        }
        Selection::Partial(mut ranges) => {
            let range = ranges.pop().expect("nonempty selection");
            let builder = builder.status(StatusCode::PARTIAL_CONTENT).header(
                header::CONTENT_RANGE,
                format!("bytes {}-{}/{}", range.start, range.end - 1, source.size),
            );
            (range, builder)
        }
    };
    let builder = builder.header(header::CONTENT_LENGTH, range.end - range.start);
    let body = if *method == Method::HEAD || range.is_empty() {
        Body::empty()
    } else {
        let chunks =
            ChunkRanges::from(ChunkNum::full_chunks(range.start)..ChunkNum::chunks(range.end));
        let (content, size) =
            tokio::time::timeout(READ_TIMEOUT, start(&source.connection, hash, chunks))
                .await
                .map_err(HttpError::timeout)?
                .map_err(HttpError::upstream)?;
        if size != source.size {
            return Err(HttpError(StatusCode::BAD_GATEWAY, "peer changed blob size"));
        }
        Body::from_stream(stream_content(content, source.connection, range))
    };
    Ok(builder.body(body).unwrap())
}

fn part_header(source: &Source, range: &Range<u64>, boundary: &str) -> String {
    format!(
        "--{boundary}\r\nContent-Type: {}\r\nContent-Range: bytes {}-{}/{}\r\n\r\n",
        source.mime,
        range.start,
        range.end - 1,
        source.size
    )
}

fn multipart_length(source: &Source, ranges: &[Range<u64>], boundary: &str) -> Option<u64> {
    ranges.iter().try_fold(
        format!("--{boundary}--\r\n").len() as u64,
        |total, range| {
            total
                .checked_add(part_header(source, range, boundary).len() as u64)?
                .checked_add(range.end - range.start)?
                .checked_add(2)
        },
    )
}

fn multipart_content(
    source: Source,
    hash: Hash,
    ranges: Vec<Range<u64>>,
    boundary: String,
) -> impl n0_future::Stream<Item = std::io::Result<Bytes>> + Send {
    async_stream::try_stream! {
        for range in ranges {
            let chunks = ChunkRanges::from(ChunkNum::full_chunks(range.start)..ChunkNum::chunks(range.end));
            let (content, size) = tokio::time::timeout(READ_TIMEOUT, start(&source.connection, hash, chunks)).await
                .map_err(std::io::Error::other)?.map_err(std::io::Error::other)?;
            if size != source.size { Err(std::io::Error::other("peer changed blob size"))?; }
            yield Bytes::from(part_header(&source, &range, &boundary));
            let mut data = Box::pin(stream_content(content, source.connection.clone(), range));
            while let Some(bytes) = data.next().await { yield bytes?; }
            yield Bytes::from_static(b"\r\n");
        }
        yield Bytes::from(format!("--{boundary}--\r\n"));
    }
}

async fn start(
    connection: &Connection,
    hash: Hash,
    ranges: ChunkRanges,
) -> anyhow::Result<(AtBlobContent, u64)> {
    let request = GetRequest::new(hash, ChunkRangesSeq::from_ranges([ranges]));
    let connected = fsm::start(connection.clone(), request, Default::default())
        .next()
        .await?;
    let ConnectedNext::StartRoot(root) = connected.next().await? else {
        bail!("expected blob root");
    };
    Ok(root.next().next().await?)
}

/// Reads the collection metadata rooted at `hash` without fetching its files.
async fn read_collection(connection: &Connection, hash: Hash) -> anyhow::Result<Collection> {
    let request = GetRequest::builder()
        .root(ChunkRanges::all())
        .child(0, ChunkRanges::all())
        .build(hash);
    let connected = fsm::start(connection.clone(), request, Default::default())
        .next()
        .await?;
    let ConnectedNext::StartRoot(root) = connected.next().await? else {
        bail!("expected collection root");
    };
    let (end, _, collection) = Collection::read_fsm(root).await?;
    let EndBlobNext::Closing(closing) = end else {
        bail!("unexpected child blob");
    };
    closing.next().await?;
    Ok(collection)
}

async fn sniff(connection: &Connection, hash: Hash) -> anyhow::Result<(u64, Vec<u8>)> {
    // Include the last chunk to authenticate the size as well as the prefix.
    let ranges = ChunkRanges::from(..ChunkNum::chunks(SNIFF_BYTES)) | ChunkRanges::last_chunk();
    let (mut content, size) = start(connection, hash, ranges).await?;
    let mut prefix = Vec::with_capacity(SNIFF_BYTES as usize);
    let end = loop {
        match content.next().await {
            BlobContentNext::More((next, item)) => {
                if let BaoContentItem::Leaf(leaf) = item? {
                    let end =
                        (SNIFF_BYTES.saturating_sub(leaf.offset) as usize).min(leaf.data.len());
                    if end > 0 {
                        anyhow::ensure!(leaf.offset == prefix.len() as u64, "noncontiguous prefix");
                        prefix.extend_from_slice(&leaf.data[..end]);
                    }
                }
                content = next;
            }
            BlobContentNext::Done(end) => break end,
        }
    };
    let EndBlobNext::Closing(closing) = end.next() else {
        bail!("unexpected child blob");
    };
    closing.next().await?;
    anyhow::ensure!(
        prefix.len() as u64 == size.min(SNIFF_BYTES),
        "incomplete prefix"
    );
    Ok((size, prefix))
}

fn stream_content(
    mut content: AtBlobContent,
    connection: Connection,
    range: Range<u64>,
) -> impl n0_future::Stream<Item = std::io::Result<Bytes>> + Send {
    async_stream::try_stream! {
        // Keep the connection alive until the HTTP body is consumed or dropped.
        let _connection = connection;
        let mut offset = range.start;
        let end = loop {
            match tokio::time::timeout(READ_TIMEOUT, content.next()).await.map_err(std::io::Error::other)? {
                BlobContentNext::More((next, item)) => {
                    if let BaoContentItem::Leaf(leaf) = item.map_err(std::io::Error::other)? {
                        let start = leaf.offset.max(range.start);
                        let end = (leaf.offset + leaf.data.len() as u64).min(range.end);
                        if start < end {
                            if start != offset { Err(std::io::Error::other("noncontiguous blob data"))?; }
                            offset = end;
                            yield leaf.data.slice((start - leaf.offset) as usize..(end - leaf.offset) as usize);
                        }
                    }
                    content = next;
                }
                BlobContentNext::Done(end) => break end,
            }
        };
        if offset != range.end { Err(std::io::Error::other("incomplete blob data"))?; }
        let closing = match end.next() {
            EndBlobNext::Closing(closing) => closing,
            _ => Err(std::io::Error::other("unexpected child blob"))?,
        };
        tokio::time::timeout(READ_TIMEOUT, closing.next()).await
            .map_err(std::io::Error::other)?.map_err(std::io::Error::other)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_z32_and_loopback_only() {
        let hash = Hash::new(b"hello");
        assert_eq!(parse_hash(&z32::encode(hash.as_bytes())).unwrap(), hash);
        assert!(parse_hash(&hash.to_hex()).is_err());
        assert!(parse_hash(&"0".repeat(52)).is_err());
        let mut alias = z32::encode(&[0; 32]);
        alias.pop();
        alias.push('b');
        assert!(parse_hash(&alias).is_err());
        assert!(validate_listen_addr("127.0.0.1:8080".parse().unwrap()).is_ok());
        assert!(validate_listen_addr("[::1]:8080".parse().unwrap()).is_ok());
        assert!(validate_listen_addr("0.0.0.0:8080".parse().unwrap()).is_err());
    }
}
