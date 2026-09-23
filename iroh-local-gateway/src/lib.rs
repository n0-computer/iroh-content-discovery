//! Local HTTP delivery of Bao-verified blobs discovered through Mainline.
//!
//! Adapted from the streaming approach in `iroh-examples/iroh-gateway`.
//! Each content lookup chooses one signed endpoint. Data is streamed directly
//! from that peer, without downloading a whole blob into memory or a store.

use std::{
    collections::{BTreeSet, HashMap},
    net::SocketAddr,
    ops::Range,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::bail;
use axum::{
    Extension, Router,
    body::Body,
    extract::{Path, RawQuery, Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    middleware::map_request,
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
use n0_future::{BufferedStreamExt, StreamExt};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use tower_http::cors::{Any, CorsLayer};

mod ranges;
use ranges::Selection;

const LOOKUP_TIMEOUT: Duration = Duration::from_secs(60);
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const SNIFF_BYTES: u64 = 8192;
const MAX_AUTO_COLLECTION_ROOT_BYTES: u64 = 8 * 1024 * 1024;
const COLLECTION_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const PATH_SEGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'%')
    .add(b'#')
    .add(b'?')
    .add(b'/')
    .add(b'<')
    .add(b'>')
    .add(b'"')
    .add(b'\'')
    .add(b'&');

/// Concurrent size requests per collection listing.
const SIZE_REQUESTS: usize = 16;
const REPO_URL: &str = "https://github.com/n0-computer/iroh-content-discovery";
const LISTING_CSS: &str = include_str!("listing.css");
/// Path separator in listing headings; `<wbr>` lets long paths wrap after it.
const SEPARATOR: &str = "&nbsp;/&nbsp;<wbr>";

/// An HTTP gateway using a caller-owned iroh endpoint and content resolver.
#[derive(Clone)]
pub struct Gateway(Arc<Inner>);

struct Inner {
    endpoint: Endpoint,
    resolver: Resolver,
    classifier: MimeClassifier,
    // Reuse one peer for repeated video seeks, with bounded metadata memory.
    cache: Mutex<LruCache<Hash, Source>>,
    collections: Mutex<LruCache<Hash, CollectionSource>>,
    sizes: Mutex<LruCache<Hash, u64>>,
}

#[derive(Clone)]
struct Source {
    connection: Connection,
    size: u64,
    mime: String,
}

#[derive(Clone)]
struct CollectionSource {
    connection: Connection,
    collection: Collection,
}

impl Gateway {
    /// Construct a gateway. The endpoint is used to dial resolved endpoint IDs.
    pub fn new(endpoint: Endpoint, resolver: Resolver) -> Self {
        Self(Arc::new(Inner {
            endpoint,
            resolver,
            classifier: MimeClassifier::new(),
            cache: Mutex::new(LruCache::new(128.try_into().unwrap())),
            collections: Mutex::new(LruCache::new(128.try_into().unwrap())),
            sizes: Mutex::new(LruCache::new(4096.try_into().unwrap())),
        }))
    }

    /// HTTP routes for blobs and paths inside collections, plus CORS preflight.
    ///
    /// `/blake3/{hash}` serves a blob, or lists the top level of a detected
    /// collection. `/blake3/{hash}/{path}` serves a file of a collection or
    /// lists a directory, where directories are the `/`-separated prefixes of
    /// names. Listings show file sizes with `?sizes`.
    ///
    /// Requests to `http://{z32}.localhost/{path}` are handled like
    /// `/blake3/{z32}/{path}`, giving each hash its own browser origin.
    pub fn router(&self) -> Router {
        let routes = Router::new()
            .route("/blake3/{hash}", get(blob))
            .route("/blake3/{hash}/", get(collection_root))
            .route("/blake3/{hash}/{*path}", get(collection_path))
            .with_state(self.clone());
        // The rewrite has to happen before routing, so it wraps the routes as
        // the fallback of an otherwise empty router.
        Router::new()
            .fallback_service(routes)
            .layer(map_request(rewrite_subdomain))
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

    async fn source(&self, hash: Hash) -> Result<Source, HttpError> {
        let cached = self.0.cache.lock().unwrap().get(&hash).cloned();
        if let Some(source) = cached
            && source.connection.close_reason().is_none()
        {
            return Ok(source);
        }
        let connection = self.connection(hash).await?;
        let source = self.source_on_connection(connection, hash, None).await?;
        self.0.cache.lock().unwrap().put(hash, source.clone());
        Ok(source)
    }

    async fn connection(&self, hash: Hash) -> Result<Connection, HttpError> {
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

    async fn source_on_connection(
        &self,
        connection: Connection,
        hash: Hash,
        name: Option<&str>,
    ) -> Result<Source, HttpError> {
        let (size, prefix) = sniff(&connection, hash)
            .await
            .map_err(HttpError::upstream)?;
        let supplied_type = name
            .and_then(|name| std::path::Path::new(name).extension())
            .and_then(|extension| extension.to_str())
            .and_then(|extension| mime_guess::from_ext(extension).first());
        let mime = self
            .0
            .classifier
            .classify(
                mime_classifier::LoadContext::Browsing,
                if supplied_type.is_some() {
                    mime_classifier::NoSniffFlag::On
                } else {
                    mime_classifier::NoSniffFlag::Off
                },
                mime_classifier::ApacheBugFlag::Off,
                &supplied_type,
                &prefix,
            )
            .to_string();
        let source = Source {
            connection,
            size,
            mime: with_charset(mime, &prefix),
        };
        Ok(source)
    }

    /// Returns the verified size of `hash`, fetched over `connection` if not cached.
    async fn size(&self, hash: Hash, connection: &Connection) -> anyhow::Result<u64> {
        if let Some(source) = self.0.cache.lock().unwrap().peek(&hash) {
            return Ok(source.size);
        }
        if let Some(size) = self.0.sizes.lock().unwrap().get(&hash) {
            return Ok(*size);
        }
        let size = verified_size(connection, hash).await?;
        self.0.sizes.lock().unwrap().put(hash, size);
        Ok(size)
    }

    /// Returns the sizes of `hashes`, fetching up to [`SIZE_REQUESTS`] at a time.
    ///
    /// Hashes whose size could not be fetched are missing from the result.
    async fn sizes(&self, hashes: Vec<Hash>, connection: Connection) -> HashMap<Hash, u64> {
        n0_future::stream::iter(hashes)
            .map(|hash| {
                let gateway = self.clone();
                let connection = connection.clone();
                async move {
                    match gateway.size(hash, &connection).await {
                        Ok(size) => Some((hash, size)),
                        Err(error) => {
                            tracing::debug!(%error, %hash, "fetch size");
                            None
                        }
                    }
                }
            })
            .buffered_unordered(SIZE_REQUESTS)
            .filter_map(|entry| entry)
            .collect()
            .await
    }

    async fn collection(&self, hash: Hash) -> Result<CollectionSource, HttpError> {
        let cached = self.0.collections.lock().unwrap().get(&hash).cloned();
        if let Some(source) = cached
            && source.connection.close_reason().is_none()
        {
            return Ok(source);
        }
        let connection = self.connection(hash).await?;
        self.collection_on_connection(hash, connection).await
    }

    async fn collection_on_connection(
        &self,
        hash: Hash,
        connection: Connection,
    ) -> Result<CollectionSource, HttpError> {
        let cached = self.0.collections.lock().unwrap().get(&hash).cloned();
        if let Some(source) = cached
            && source.connection.close_reason().is_none()
        {
            return Ok(source);
        }
        let collection = read_collection(&connection, hash).await.map_err(|error| {
            tracing::debug!(%error, "read collection");
            HttpError(StatusCode::UNPROCESSABLE_ENTITY, "not a collection")
        })?;
        let source = CollectionSource {
            connection,
            collection,
        };
        self.0.collections.lock().unwrap().put(hash, source.clone());
        Ok(source)
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

/// Marks a request that named its hash as a `{z32}.localhost` subdomain.
#[derive(Debug, Clone, Copy)]
struct Subdomain;

/// Rewrites requests for `{z32}.localhost` to the equivalent `/blake3/{z32}` path.
async fn rewrite_subdomain(mut request: Request) -> Request {
    let host = request.uri().host().map(str::to_owned).or_else(|| {
        let host = request.headers().get(header::HOST)?.to_str().ok()?;
        Some(
            host.rsplit_once(':')
                .map_or(host, |(host, _)| host)
                .to_owned(),
        )
    });
    let Some(label) = host
        .as_deref()
        .and_then(|host| host.strip_suffix(".localhost"))
        .filter(|label| parse_hash(label).is_ok())
    else {
        return request;
    };
    let path_and_query = request
        .uri()
        .path_and_query()
        .map_or("/", |value| value.as_str());
    // `/` maps to the bare hash route, which also detects collections.
    let rest = path_and_query.strip_prefix('/').unwrap_or(path_and_query);
    let rewritten = if rest.is_empty() || rest.starts_with('?') {
        format!("/blake3/{label}{rest}")
    } else {
        format!("/blake3/{label}/{rest}")
    };
    if let Ok(uri) = rewritten.parse() {
        *request.uri_mut() = uri;
        request.extensions_mut().insert(Subdomain);
    }
    request
}

/// Adds a charset to textual content types that carry none.
///
/// Without it browsers decode text with a locale-dependent fallback, which
/// renders UTF-8 wrongly. UTF-8 is assumed unless a byte order mark says the
/// content is UTF-16.
fn with_charset(mime: String, prefix: &[u8]) -> String {
    if !mime.starts_with("text/") || mime.contains("charset=") {
        return mime;
    }
    let charset = match prefix {
        [0xff, 0xfe, ..] => "utf-16le",
        [0xfe, 0xff, ..] => "utf-16be",
        _ => "utf-8",
    };
    format!("{mime}; charset={charset}")
}

/// Returns whether the query string sets `name`, as `?name` or `?name=...`.
fn has_flag(query: Option<&str>, name: &str) -> bool {
    query.is_some_and(|query| {
        query.split('&').any(|pair| {
            pair.strip_prefix(name)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('='))
        })
    })
}

/// Returns a `Content-Disposition` value that saves the body as `filename`.
///
/// Sends both the plain and the RFC 6266 extended form, since the plain one
/// cannot express non-ASCII names.
fn attachment(filename: &str) -> String {
    let ascii: String = filename
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '-' | '_' | ' ' => c,
            _ => '_',
        })
        .collect();
    let encoded = utf8_percent_encode(filename, PATH_SEGMENT);
    format!("attachment; filename=\"{ascii}\"; filename*=UTF-8''{encoded}")
}

/// The collection a listing belongs to, and the path its links start with.
struct Root {
    encoded: String,
    /// `/blake3/{z32}`, or empty when the hash is the request's subdomain.
    base: String,
}

impl Root {
    fn new(encoded: String, subdomain: Option<Extension<Subdomain>>) -> Self {
        let base = match subdomain {
            Some(_) => String::new(),
            None => format!("/blake3/{encoded}"),
        };
        Self { encoded, base }
    }
}

async fn blob(
    State(gateway): State<Gateway>,
    Path(encoded): Path<String>,
    RawQuery(query): RawQuery,
    subdomain: Option<Extension<Subdomain>>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    let hash = parse_hash(&encoded)
        .map_err(|_| HttpError(StatusCode::BAD_REQUEST, "invalid z-base-32 BLAKE3 hash"))?;
    let download = has_flag(query.as_deref(), "download");
    // `?tree` states that the blob is a collection, which skips both the size
    // probe and the detection limits. `?download` wins, and asks for the bytes.
    if !download && has_flag(query.as_deref(), "tree") {
        let collection = tokio::time::timeout(LOOKUP_TIMEOUT, gateway.collection(hash))
            .await
            .map_err(HttpError::timeout)??;
        let root = Root::new(encoded, subdomain);
        return collection_entry(
            &gateway,
            &root,
            collection,
            String::new(),
            query,
            method,
            headers,
        )
        .await;
    }
    let source = tokio::time::timeout(LOOKUP_TIMEOUT, gateway.source(hash))
        .await
        .map_err(HttpError::timeout)??;
    // `?download` saves a collection root as the hash sequence it is.
    if !download
        && source.size >= 32
        && source.size.is_multiple_of(32)
        && source.size <= MAX_AUTO_COLLECTION_ROOT_BYTES
        && let Ok(Ok(collection)) = tokio::time::timeout(
            COLLECTION_PROBE_TIMEOUT,
            gateway.collection_on_connection(hash, source.connection.clone()),
        )
        .await
    {
        let root = Root::new(encoded, subdomain);
        return collection_entry(
            &gateway,
            &root,
            collection,
            String::new(),
            query,
            method,
            headers,
        )
        .await;
    }
    let download = download.then(|| encoded.clone());
    serve_blob(source, hash, &encoded, download, method, headers).await
}

async fn collection_root(
    state: State<Gateway>,
    Path(encoded): Path<String>,
    query: RawQuery,
    subdomain: Option<Extension<Subdomain>>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    collection_path(
        state,
        Path((encoded, String::new())),
        query,
        subdomain,
        method,
        headers,
    )
    .await
}

async fn collection_path(
    State(gateway): State<Gateway>,
    Path((encoded, path)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    subdomain: Option<Extension<Subdomain>>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    let root = parse_hash(&encoded)
        .map_err(|_| HttpError(StatusCode::BAD_REQUEST, "invalid z-base-32 BLAKE3 hash"))?;
    let collection = tokio::time::timeout(LOOKUP_TIMEOUT, gateway.collection(root))
        .await
        .map_err(HttpError::timeout)??;
    let path = path.strip_prefix('/').map(str::to_owned).unwrap_or(path);
    let root = Root::new(encoded, subdomain);
    collection_entry(&gateway, &root, collection, path, query, method, headers).await
}

/// Serves the file at `path` in a collection, or lists it as a directory.
async fn collection_entry(
    gateway: &Gateway,
    root: &Root,
    source: CollectionSource,
    path: String,
    query: Option<String>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    let CollectionSource {
        collection,
        connection,
    } = source;
    let file = collection
        .iter()
        .find(|(name, _)| *name == path)
        .map(|(_, hash)| *hash);
    if let Some(hash) = file {
        let source = tokio::time::timeout(
            LOOKUP_TIMEOUT,
            gateway.source_on_connection(connection, hash, Some(&path)),
        )
        .await
        .map_err(HttpError::timeout)??;
        // Save under the file's own name, not the hash.
        let download = has_flag(query.as_deref(), "download").then(|| {
            path.rsplit_once('/')
                .map_or(path.as_str(), |(_, file)| file)
                .to_owned()
        });
        return serve_blob(
            source,
            hash,
            &z32::encode(hash.as_bytes()),
            download,
            method,
            headers,
        )
        .await;
    }
    let dir = if path.is_empty() || path.ends_with('/') {
        path
    } else {
        format!("{path}/")
    };
    let entries = Entries::new(&dir, &collection).ok_or(HttpError(
        StatusCode::NOT_FOUND,
        "path not found in collection",
    ))?;
    let with_sizes = has_flag(query.as_deref(), "sizes");
    let sizes = if with_sizes {
        let hashes = entries.files.iter().map(|(_, hash)| *hash).collect();
        Some(
            tokio::time::timeout(LOOKUP_TIMEOUT, gateway.sizes(hashes, connection))
                .await
                .map_err(HttpError::timeout)?,
        )
    } else {
        None
    };
    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::CACHE_CONTROL, "public, max-age=31536000, immutable")
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .body(Body::from(listing(root, &dir, &entries, sizes.as_ref())))
        .unwrap())
}

/// The subdirectories and files directly inside one directory of a collection.
struct Entries<'a> {
    dirs: BTreeSet<&'a str>,
    files: Vec<(&'a str, Hash)>,
}

impl<'a> Entries<'a> {
    /// Collects the entries of `dir`, which is empty for the top level and
    /// otherwise ends with `/`. Returns `None` if no name starts with `dir`.
    fn new(dir: &str, collection: &'a Collection) -> Option<Self> {
        let mut dirs = BTreeSet::new();
        let mut files = Vec::new();
        for (name, hash) in collection.iter() {
            let Some(rest) = name.strip_prefix(dir) else {
                continue;
            };
            match rest.split_once('/') {
                Some((sub, _)) => {
                    dirs.insert(sub);
                }
                None => files.push((rest, *hash)),
            }
        }
        if !dir.is_empty() && dirs.is_empty() && files.is_empty() {
            return None;
        }
        Some(Self { dirs, files })
    }
}

/// Renders the HTML listing of `dir` in the collection at `root`.
///
/// With `sizes`, file sizes are shown and directory links keep `?sizes`.
/// Without, the page links to the same listing with sizes.
fn listing(
    root: &Root,
    dir: &str,
    entries: &Entries<'_>,
    sizes: Option<&HashMap<Hash, u64>>,
) -> String {
    let base = &root.base;
    let root = &root.encoded;
    let title = html_escape(&format!("{root}/{dir}"));
    let query = if sizes.is_some() { "?sizes" } else { "" };
    let link = |path: &str| {
        let path = path
            .split('/')
            .map(|segment| utf8_percent_encode(segment, PATH_SEGMENT).to_string())
            .collect::<Vec<_>>()
            .join("/");
        html_escape(&format!("{base}/{path}"))
    };
    // Breadcrumbs: every ancestor links to its listing, the current directory
    // is plain text.
    let segments: Vec<&str> = dir.split('/').filter(|s| !s.is_empty()).collect();
    let mut heading = if segments.is_empty() {
        root.to_string()
    } else {
        format!("<a href=\"{}{query}\">{root}</a>", link(""))
    };
    let mut path = String::new();
    for (index, segment) in segments.iter().enumerate() {
        path.push_str(segment);
        path.push('/');
        heading.push_str(SEPARATOR);
        if index + 1 == segments.len() {
            heading.push_str(&html_escape(segment));
        } else {
            heading.push_str(&format!(
                "<a href=\"{}{query}\">{}</a>",
                link(&path),
                html_escape(segment)
            ));
        }
    }
    heading.push_str(SEPARATOR);
    let mut html = format!(
        "<!DOCTYPE html>\n<html lang=\"en\">\n<meta charset=\"utf-8\">\n\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
         <title>{title}</title>\n<style>{LISTING_CSS}</style>\n\
         <header><a href=\"{REPO_URL}\">iroh content discovery</a></header>\n\
         <h1>{heading}</h1>\n"
    );
    if sizes.is_none() {
        html.push_str("<p class=\"meta\"><a href=\"?sizes\">Fetch sizes</a></p>\n");
    }
    html.push_str("<table>\n");
    if let Some(trimmed) = dir.strip_suffix('/') {
        let parent = trimmed.rsplit_once('/').map_or("", |(parent, _)| parent);
        let parent = if parent.is_empty() {
            String::new()
        } else {
            format!("{parent}/")
        };
        html.push_str(&format!(
            "<tr><td><a href=\"{}{query}\">../</a></td><td class=\"size\"></td><td class=\"hash\"></td><td class=\"download\"></td></tr>\n",
            link(&parent)
        ));
    }
    for sub in &entries.dirs {
        html.push_str(&format!(
            "<tr><td><a href=\"{}{query}\">{}/</a></td><td class=\"size\"></td><td class=\"hash\"></td><td class=\"download\"></td></tr>\n",
            link(&format!("{dir}{sub}/")),
            html_escape(sub),
        ));
    }
    for (file, hash) in &entries.files {
        let size = match sizes {
            Some(sizes) => sizes
                .get(hash)
                .map_or("?".to_string(), |size| format_size(*size)),
            None => String::new(),
        };
        html.push_str(&format!(
            "<tr><td><a href=\"{}\">{}</a></td><td class=\"size\">{size}</td><td class=\"hash\">{}</td>\
             <td class=\"download\"><a href=\"{}?download\">Download</a></td></tr>\n",
            link(&format!("{dir}{file}")),
            html_escape(file),
            z32::encode(hash.as_bytes()),
            link(&format!("{dir}{file}")),
        ));
    }
    html.push_str("</table>\n");
    html
}

/// Formats a byte count with binary units, e.g. `1.5 MiB`.
fn format_size(size: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = size as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{size} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
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

async fn serve_blob(
    source: Source,
    hash: Hash,
    encoded: &str,
    download: Option<String>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    let etag = format!("\"{encoded}\"");
    let builder = Response::builder()
        .header(header::CONTENT_TYPE, &source.mime)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::ETAG, &etag)
        .header(header::CACHE_CONTROL, "public, max-age=31536000, immutable")
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff");
    let builder = match &download {
        Some(filename) => builder.header(header::CONTENT_DISPOSITION, attachment(filename)),
        None => builder,
    };
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
    let selection = if method == Method::HEAD
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
    let body = if method == Method::HEAD || range.is_empty() {
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

async fn read_collection(connection: &Connection, hash: Hash) -> anyhow::Result<Collection> {
    let request = GetRequest::new(
        hash,
        ChunkRangesSeq::from_ranges([ChunkRanges::all(), ChunkRanges::all()]),
    );
    let connected = fsm::start(connection.clone(), request, Default::default())
        .next()
        .await?;
    let ConnectedNext::StartRoot(root) = connected.next().await? else {
        bail!("expected collection root");
    };
    let (end, _, collection) = Collection::read_fsm(root).await?;
    let EndBlobNext::Closing(closing) = end else {
        bail!("unexpected collection child");
    };
    closing.next().await?;
    Ok(collection)
}

/// Returns the size of `hash`, authenticated by fetching only its last chunk.
async fn verified_size(connection: &Connection, hash: Hash) -> anyhow::Result<u64> {
    let (mut content, size) = start(connection, hash, ChunkRanges::last_chunk()).await?;
    let end = loop {
        match content.next().await {
            BlobContentNext::More((next, item)) => {
                item?;
                content = next;
            }
            BlobContentNext::Done(end) => break end,
        }
    };
    let EndBlobNext::Closing(closing) = end.next() else {
        bail!("unexpected child blob");
    };
    closing.next().await?;
    Ok(size)
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
