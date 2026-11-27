//! An optional iroh-blobs store behind the gateway, bounded by an
//! [`iroh_blobs_cache::BlobCache`].
//!
//! With a store, verified content is kept: requests are served from the
//! store, and only chunks it lacks are fetched from a provider, Bao-verified
//! into the store on the way. Content that is fully in the store needs no
//! provider, so it works offline and survives restarts. The newest verified
//! Pkarr record of each name is kept as well, as a blob under a
//! `pkarr/<key>` tag, and used when a lookup fails.
//!
//! The store is keyed by hash, not URL, so the same file under different
//! names or hosts is stored once, and a transfer that broke off continues
//! where it stopped: the store knows which chunks it has.
//!
//! Every blob the gateway stores or serves is recorded as used, so the cache
//! evicts the least recently used ones once the store is over its budget.
//! Fetches and responses hold their blob, so garbage collection cannot delete
//! a partial download.

use std::ops::Range;

use axum::http::StatusCode;
use bytes::Bytes;
use iroh::endpoint::Connection;
use iroh_blobs::{
    Hash,
    api::{Store, proto::ExportRangesItem, remote::GetProgressItem},
    format::collection::Collection,
    hashseq::HashSeq,
    protocol::GetRequest,
};
use iroh_blobs_cache::BlobCache;
use iroh_mainline_endpoint_discovery::{decode_signed_packet, encode_signed_packet};
use n0_future::{Stream, StreamExt};
use tracing::{debug, warn};

use crate::{Gateway, HttpError, READ_TIMEOUT, chunks_for};

/// Bytes fetched and served per step, so a large file streams while it
/// arrives instead of after the whole range is in the store.
const WINDOW: u64 = 4 * 1024 * 1024;

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

    /// Returns the size and the first `limit` bytes of `hash`, if all of it
    /// is in the store.
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
        fetcher.fill(&window_at(range.start, range.end)).await?;
        Ok(async_stream::try_stream! {
            let _hold = hold;
            let mut start = range.start;
            while start < range.end {
                let window = window_at(start, range.end);
                if start > range.start {
                    fetcher.fill(&window).await?;
                }
                let mut data = self.store.export_ranges(hash, window.clone()).stream();
                let mut next = window.start;
                while let Some(item) = data.next().await {
                    match item {
                        ExportRangesItem::Size(_) => {}
                        ExportRangesItem::Data(leaf) => {
                            if leaf.offset != next {
                                Err(std::io::Error::other("noncontiguous data in the store"))?;
                            }
                            next += leaf.data.len() as u64;
                            yield leaf.data;
                        }
                        ExportRangesItem::Error(error) => Err(std::io::Error::other(error))?,
                    }
                }
                if next != window.end {
                    Err(std::io::Error::other("incomplete data in the store"))?;
                }
                start = window.end;
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

/// Returns the window of a read from `start`, which ends at `end` at most.
fn window_at(start: u64, end: u64) -> Range<u64> {
    start..(start + WINDOW).min(end)
}

/// Logs a failure of the store and returns the answer to the request.
fn store_failed(error: impl std::fmt::Display) -> HttpError {
    warn!(%error, "store failed");
    HttpError(StatusCode::INTERNAL_SERVER_ERROR, "store failed")
}
