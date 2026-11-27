//! An optional iroh-blobs store behind the gateway.
//!
//! An [`iroh_blobs_cache::BlobCache`] bounds its size. With a store, verified
//! content is kept: requests are served from the store, and only chunks it
//! lacks are fetched from a provider, Bao-verified into the store on the way.
//! Content that is fully in the store needs no provider, so it works offline
//! and survives restarts. The newest verified Pkarr record of each name is
//! kept as well, as a blob under a `pkarr/<key>` tag, and used when a lookup
//! fails or finds an older one.
//!
//! The store is keyed by hash, not URL, so the same file under different
//! names or hosts is stored once, and a transfer that broke off continues
//! where it stopped: the store knows which chunks it has.
//!
//! Every blob the gateway stores or serves is recorded as used, so the cache
//! evicts the least recently used ones once the store is over its limit.
//! Fetches and responses hold their blob, so garbage collection cannot delete
//! a partial download.
//!
//! Content can also be kept for offline use: [`Gateway::keep`] fetches all of
//! it and pins it under a name, so eviction never drops it.

use std::ops::Range;

use axum::http::StatusCode;
use bytes::Bytes;
use iroh::endpoint::Connection;
use iroh_blobs::{
    BlobFormat, Hash, HashAndFormat,
    api::{Store, proto::ExportRangesItem, remote::GetProgressItem},
    format::collection::Collection,
    hashseq::HashSeq,
    protocol::GetRequest,
};
use iroh_blobs_cache::BlobCache;
use iroh_mainline_endpoint_discovery::{decode_signed_packet, encode_signed_packet};
use n0_error::{AnyError, e};
use n0_future::{Stream, StreamExt};
use tracing::{debug, warn};

use crate::{Gateway, HttpError, READ_TIMEOUT, chunks_for};

/// Bytes fetched and served in the first step of a read.
///
/// Small, so that a request for missing data gets its first bytes after a
/// short fetch rather than after a whole [`MAX_WINDOW`].
const FIRST_WINDOW: u64 = 256 * 1024;
/// Largest number of bytes fetched and served in one step.
///
/// Windows double from [`FIRST_WINDOW`] up to this size. A large file thus
/// streams while it arrives, with few round trips to the provider.
const MAX_WINDOW: u64 = 4 * 1024 * 1024;

/// The gateway's store.
#[derive(Clone)]
pub(crate) struct Local {
    cache: BlobCache,
    store: Store,
}

impl Local {
    pub(crate) fn new(cache: BlobCache) -> Self {
        let store = cache.store().clone();
        Self { cache, store }
    }

    /// Records a use of `hash`, so the cache keeps it over older blobs.
    async fn touch(&self, hash: Hash) {
        if let Err(error) = self.cache.touch(hash).await {
            warn!(%error, %hash, "cannot record blob use");
        }
    }

    /// Returns the verified size of `hash` if the store can prove it.
    pub(crate) async fn size(&self, hash: Hash) -> Option<u64> {
        self.store.observe(hash).await.ok()?.validated_size()
    }

    /// Returns the size and the first `limit` bytes of `hash`.
    ///
    /// Returns `None` unless the store has those bytes and can prove the size.
    pub(crate) async fn prefix(&self, hash: Hash, limit: u64) -> Option<(u64, Vec<u8>)> {
        let bitfield = self.store.observe(hash).await.ok()?;
        let size = bitfield.validated_size()?;
        let end = size.min(limit);
        if !bitfield.ranges.is_superset(&chunks_for(&(0..end))) {
            return None;
        }
        let prefix = self
            .store
            .export_ranges(hash, 0..end)
            .concatenate()
            .await
            .ok()?;
        Some((size, prefix))
    }

    /// Returns the collection at `hash` if it is in the store.
    pub(crate) async fn collection(&self, hash: Hash) -> Option<Collection> {
        if !self.store.observe(hash).await.ok()?.is_complete() {
            return None;
        }
        let collection = Collection::load(hash, &self.store).await.ok()?;
        self.touch_collection(hash).await;
        Some(collection)
    }

    /// Keeps a collection read from a provider.
    ///
    /// Storing it again yields the same blobs, so the hash is checked: a
    /// collection that encodes differently is not kept. Its root and name
    /// list are cached like other blobs, without keeping its files: those
    /// are cached as they are served, and evicted on their own.
    pub(crate) async fn keep_collection(&self, hash: Hash, collection: &Collection) {
        match collection.clone().store(&self.store).await {
            Ok(tag) if tag.hash() == hash => self.touch_collection(hash).await,
            Ok(tag) => {
                debug!(%hash, stored = %tag.hash(), "collection encodes differently; not kept")
            }
            Err(error) => warn!(%error, %hash, "cannot store collection"),
        }
    }

    /// Records a use of the collection at `hash`: its root and its name list.
    ///
    /// Both are needed to load it from the store, so they age together.
    /// Otherwise the name list, used once, is evicted long before the root.
    async fn touch_collection(&self, hash: Hash) {
        self.touch(hash).await;
        if let Ok(bytes) = self.store.get_bytes(hash).await
            && let Some(names) = HashSeq::new(bytes).and_then(|seq| seq.into_iter().next())
        {
            self.touch(names).await;
        }
    }

    /// Opens `range` of `hash` for reading, fetching missing chunks into the store.
    ///
    /// The first window is in the store before this returns, so a request
    /// that cannot get it fails with a status instead of a body that stops
    /// short. Later windows are fetched while the body streams. See
    /// [`Fetcher::fill`] for which provider is asked.
    pub(crate) async fn read(
        self,
        gateway: Gateway,
        root: Hash,
        hash: Hash,
        connection: Option<Connection>,
        range: Range<u64>,
    ) -> Result<impl Stream<Item = std::io::Result<Bytes>> + Send, HttpError> {
        // Keeps a partial download through garbage collection, and the blob
        // out of eviction while it is read.
        let hold = self.cache.hold(hash).await.map_err(store_failed)?;
        self.touch(hash).await;
        let mut fetcher = Fetcher {
            store: self.store.clone(),
            gateway,
            root,
            hash,
            connection,
        };
        let mut windows = windows(range);
        let mut window = windows.next();
        if let Some(first) = &window {
            fetcher.fill(first).await?;
        }
        Ok(async_stream::try_stream! {
            let _hold = hold;
            while let Some(current) = window {
                let mut data = self.store.export_ranges(hash, current.clone()).stream();
                let mut offset = current.start;
                while let Some(item) = data.next().await {
                    match item {
                        ExportRangesItem::Size(_) => {}
                        ExportRangesItem::Data(leaf) => {
                            if leaf.offset != offset {
                                Err(std::io::Error::other("noncontiguous data in the store"))?;
                            }
                            offset += leaf.data.len() as u64;
                            yield leaf.data;
                        }
                        ExportRangesItem::Error(error) => Err(std::io::Error::other(error))?,
                    }
                }
                if offset != current.end {
                    Err(std::io::Error::other("incomplete data in the store"))?;
                }
                window = windows.next();
                if let Some(next) = &window {
                    fetcher.fill(next).await?;
                }
            }
        })
    }

    /// Keeps the newest verified Pkarr record of `key`.
    pub(crate) async fn keep_packet(&self, key: &[u8; 32], item: &n0_mainline::MutableItem) {
        let Some(bytes) = encode_signed_packet(item) else {
            return;
        };
        // Awaiting `add_bytes` on its own would also create an `auto-` tag,
        // one per lookup, that nothing ever deletes.
        let result = self
            .store
            .add_bytes(bytes)
            .with_named_tag(packet_tag(key))
            .await;
        if let Err(error) = result {
            warn!(%error, "cannot keep Pkarr record");
        }
    }

    /// Returns the last kept Pkarr record of `key`, checking its signature again.
    pub(crate) async fn packet(&self, key: &[u8; 32]) -> Option<n0_mainline::MutableItem> {
        let tag = self.store.tags().get(packet_tag(key)).await.ok()??;
        let bytes = self.store.get_bytes(tag.hash).await.ok()?;
        decode_signed_packet(&bytes).filter(|item| item.key() == key)
    }
}

impl Local {
    /// Fetches everything `content` lacks into the store and pins it as `name`.
    ///
    /// `connection` is a provider of `content`'s root, if there is one open.
    pub(crate) async fn keep(
        &self,
        gateway: &Gateway,
        name: &str,
        content: HashAndFormat,
        mut connection: Option<Connection>,
        progress: &mut (dyn FnMut(KeepProgress) + Send),
    ) -> Result<Kept, KeepError> {
        // Keeps the root and every child fetched so far through garbage
        // collection until the pin is set. Their blobs have no cache tags,
        // so eviction does not see them.
        let _hold = self
            .store
            .tags()
            .temp_tag(content)
            .await
            .map_err(keep_store_failed)?;
        let local = self
            .store
            .remote()
            .local(content)
            .await
            .map_err(keep_store_failed)?;
        if !local.is_complete() {
            let connection = match connection.take() {
                Some(connection) if connection.close_reason().is_none() => connection,
                _ => gateway
                    .connection(content.hash)
                    .await
                    .map_err(keep_fetch_failed)?,
            };
            let start = local.local_bytes();
            let total = self.total(gateway, content, &connection).await;
            progress(KeepProgress { have: start, total });
            debug!(?content, name, start, ?total, "keeping content");
            let fetch = self.store.remote().fetch(connection, content).stream();
            let mut fetch = std::pin::pin!(fetch);
            loop {
                let item = tokio::time::timeout(READ_TIMEOUT, fetch.next())
                    .await
                    .map_err(|elapsed| keep_fetch_failed(HttpError::timeout(elapsed)))?;
                match item {
                    Some(GetProgressItem::Progress(fetched)) => progress(KeepProgress {
                        have: start + fetched,
                        total,
                    }),
                    Some(GetProgressItem::Done(_)) => break,
                    Some(GetProgressItem::Error(error)) => {
                        return Err(keep_fetch_failed(HttpError::upstream(error)));
                    }
                    None => {
                        return Err(keep_fetch_failed(HttpError::upstream(
                            "fetch ended without a result",
                        )));
                    }
                }
            }
        }
        self.cache
            .pin(name, content)
            .await
            .map_err(keep_store_failed)?;
        let kept = self.kept(name.to_owned(), content).await?;
        progress(KeepProgress {
            have: kept.bytes,
            total: Some(kept.bytes),
        });
        Ok(kept)
    }

    /// Returns the size of `content` in bytes, root and children, if every
    /// size is known or can be fetched over `connection`.
    async fn total(
        &self,
        gateway: &Gateway,
        content: HashAndFormat,
        connection: &Connection,
    ) -> Option<u64> {
        let root = gateway.size(content.hash, Some(connection)).await.ok()?;
        if content.format == BlobFormat::Raw {
            return Some(root);
        }
        let children: Vec<Hash> = HashSeq::new(self.store.get_bytes(content.hash).await.ok()?)?
            .into_iter()
            .collect();
        let sizes = gateway
            .sizes(children.clone(), Some(connection.clone()))
            .await;
        let mut total = root;
        for hash in &children {
            total += sizes.get(hash)?;
        }
        Some(total)
    }

    /// Removes the pin `name`. Its blobs go with the next garbage
    /// collection, unless they are cached or pinned otherwise.
    pub(crate) async fn unkeep(&self, name: &str) -> Result<(), KeepError> {
        self.cache.unpin(name).await.map_err(keep_store_failed)
    }

    /// Returns every pin, with how much of it is in the store.
    pub(crate) async fn kept_all(&self) -> Result<Vec<Kept>, KeepError> {
        let pins = self.cache.pins().await.map_err(keep_store_failed)?;
        let mut kept = Vec::with_capacity(pins.len());
        for (name, content) in pins {
            kept.push(self.kept(name, content).await?);
        }
        Ok(kept)
    }

    /// Returns how much of the pin `name` of `content` is in the store.
    async fn kept(&self, name: String, content: HashAndFormat) -> Result<Kept, KeepError> {
        let local = self
            .store
            .remote()
            .local(content)
            .await
            .map_err(keep_store_failed)?;
        Ok(Kept {
            name,
            hash: content.hash,
            collection: content.format == BlobFormat::HashSeq,
            bytes: local.local_bytes(),
            complete: local.is_complete(),
        })
    }
}

/// Progress of [`Gateway::keep`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct KeepProgress {
    /// Bytes of the content in the store.
    pub have: u64,
    /// Bytes of the whole content, if known.
    pub total: Option<u64>,
}

/// Content pinned with [`Gateway::keep`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Kept {
    /// The name it was pinned under.
    pub name: String,
    /// Its root.
    pub hash: Hash,
    /// Whether it is a collection, kept with all its files.
    pub collection: bool,
    /// Bytes of it in the store.
    pub bytes: u64,
    /// Whether all of it is in the store.
    pub complete: bool,
}

/// Failure of [`Gateway::keep`], [`Gateway::unkeep`] or [`Gateway::kept`].
#[n0_error::stack_error(derive, add_meta)]
#[non_exhaustive]
pub enum KeepError {
    /// The gateway was made without a store.
    #[error("the gateway has no store")]
    NoStore,
    /// The content could not be fetched.
    #[error("{reason}")]
    Fetch {
        /// What went wrong, as the gateway would answer a request.
        reason: &'static str,
    },
    /// The store did not carry out a request.
    #[error("store failed")]
    Store {
        /// The store's error.
        source: AnyError,
    },
}

/// Turns the answer to a request into a [`KeepError`].
pub(crate) fn keep_fetch_failed(error: HttpError) -> KeepError {
    e!(KeepError::Fetch { reason: error.1 })
}

/// Turns a failure of the store into a [`KeepError`].
fn keep_store_failed(error: impl std::error::Error + Send + Sync + 'static) -> KeepError {
    e!(KeepError::Store, AnyError::from_std(error))
}

fn packet_tag(key: &[u8; 32]) -> String {
    format!("pkarr/{}", z32::encode(key))
}

/// Fetches the chunks of one blob that the store lacks.
struct Fetcher {
    store: Store,
    gateway: Gateway,
    /// The hash the request named, whose providers also have `hash`.
    root: Hash,
    hash: Hash,
    /// The provider of the last fetch, used again while it stays open.
    connection: Option<Connection>,
}

impl Fetcher {
    /// Fetches the chunks of `window` that the store lacks.
    ///
    /// A provider is dialled only when chunks are missing: the last one if it
    /// is still open, otherwise one for `root`.
    async fn fill(&mut self, window: &Range<u64>) -> Result<(), HttpError> {
        let have = self
            .store
            .observe(self.hash)
            .await
            .map_err(store_failed)?
            .ranges;
        let missing = chunks_for(window) - have;
        if missing.is_empty() {
            return Ok(());
        }
        let connection = match &self.connection {
            Some(connection) if connection.close_reason().is_none() => connection.clone(),
            _ => {
                let connection = self.gateway.connection(self.root).await?;
                self.connection = Some(connection.clone());
                connection
            }
        };
        debug!(hash = %self.hash, ?missing, "fetching into the store");
        let progress = self
            .store
            .remote()
            .execute_get(connection, GetRequest::blob_ranges(self.hash, missing))
            .stream();
        let mut progress = std::pin::pin!(progress);
        // A provider that stops sending fails the request, as it does
        // without a store.
        loop {
            let item = tokio::time::timeout(READ_TIMEOUT, progress.next())
                .await
                .map_err(HttpError::timeout)?;
            match item {
                Some(GetProgressItem::Progress(_)) => {}
                Some(GetProgressItem::Done(_)) => return Ok(()),
                Some(GetProgressItem::Error(error)) => return Err(HttpError::upstream(error)),
                None => return Err(HttpError::upstream("fetch ended without a result")),
            }
        }
    }
}

/// Returns the windows of a read of `range`, in order.
///
/// The first is [`FIRST_WINDOW`] long, and each next one twice as long as the
/// one before, up to [`MAX_WINDOW`].
fn windows(range: Range<u64>) -> impl Iterator<Item = Range<u64>> {
    let mut start = range.start;
    let mut size = FIRST_WINDOW;
    std::iter::from_fn(move || {
        if start >= range.end {
            return None;
        }
        let window = start..start.saturating_add(size).min(range.end);
        start = window.end;
        size = (size * 2).min(MAX_WINDOW);
        Some(window)
    })
}

/// Logs a failure of the store and returns the answer to the request.
fn store_failed(error: impl std::fmt::Display) -> HttpError {
    warn!(%error, "store failed");
    HttpError(StatusCode::INTERNAL_SERVER_ERROR, "store failed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_double_up_to_the_largest_and_cover_the_range() {
        let range = 100..20 * 1024 * 1024;
        let windows: Vec<_> = windows(range.clone()).collect();
        assert_eq!(windows[0], 100..100 + FIRST_WINDOW);
        assert_eq!(windows[1].end - windows[1].start, 2 * FIRST_WINDOW);
        assert!(
            windows
                .iter()
                .all(|window| window.end - window.start <= MAX_WINDOW)
        );
        assert_eq!(windows.last().unwrap().end, range.end);
        for pair in windows.windows(2) {
            assert_eq!(pair[0].end, pair[1].start);
        }
    }

    #[test]
    fn a_short_range_is_one_window_and_an_empty_one_none() {
        let mut short = windows(5..10);
        assert_eq!(short.next(), Some(5..10));
        assert_eq!(short.next(), None);
        assert_eq!(windows(7..7).count(), 0);
    }
}
