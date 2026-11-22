//! An optional iroh-blobs store behind the gateway.
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

use std::ops::Range;

use bytes::Bytes;
use iroh::endpoint::Connection;
use iroh_blobs::{
    Hash, HashAndFormat,
    api::{Store, proto::ExportRangesItem},
    format::collection::Collection,
    protocol::GetRequest,
};
use iroh_mainline_endpoint_discovery::{decode_signed_packet, encode_signed_packet};
use n0_future::{Stream, StreamExt};
use tracing::{debug, warn};

use crate::{Gateway, HttpError, chunks_for};

/// Bytes fetched and served per step, so a large file streams while it
/// arrives instead of after the whole range is in the store.
const WINDOW: u64 = 4 * 1024 * 1024;

/// The gateway's store.
#[derive(Clone)]
pub(crate) struct Local {
    store: Store,
}

impl Local {
    pub(crate) fn new(store: Store) -> Self {
        Self { store }
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
        Collection::load(hash, &self.store).await.ok()
    }

    /// Keeps a collection read from a provider.
    ///
    /// Storing it again yields the same blobs, so the hash is checked: a
    /// collection that encodes differently is not kept.
    pub(crate) async fn keep_collection(&self, hash: Hash, collection: &Collection) {
        match collection.clone().store(&self.store).await {
            Ok(tag) if tag.hash() == hash => {
                let name = format!("collection/{hash}");
                if let Err(error) = self
                    .store
                    .tags()
                    .set(name, HashAndFormat::hash_seq(hash))
                    .await
                {
                    warn!(%error, %hash, "cannot tag collection");
                }
            }
            Ok(tag) => {
                debug!(%hash, stored = %tag.hash(), "collection encodes differently; not kept")
            }
            Err(error) => warn!(%error, %hash, "cannot store collection"),
        }
    }

    /// Streams `range` of `hash`, fetching missing chunks into the store first.
    ///
    /// A provider is dialled only when chunks are missing: `connection` if
    /// given, otherwise one for `root`, the hash the request named.
    pub(crate) fn stream(
        self,
        gateway: Gateway,
        root: Hash,
        hash: Hash,
        mut connection: Option<Connection>,
        range: Range<u64>,
    ) -> impl Stream<Item = std::io::Result<Bytes>> + Send {
        async_stream::try_stream! {
            let mut start = range.start;
            while start < range.end {
                let window = start..(start + WINDOW).min(range.end);
                let have = self.store.observe(hash).await.map_err(std::io::Error::other)?.ranges;
                let missing = chunks_for(&window) - have;
                if !missing.is_empty() {
                    let conn = match &connection {
                        Some(conn) if conn.close_reason().is_none() => conn.clone(),
                        _ => {
                            let conn = gateway.connection(root).await.map_err(io_error)?;
                            connection = Some(conn.clone());
                            conn
                        }
                    };
                    debug!(%hash, ?missing, "fetching into the store");
                    self.store
                        .remote()
                        .execute_get(conn, GetRequest::blob_ranges(hash, missing))
                        .complete()
                        .await
                        .map_err(std::io::Error::other)?;
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
        }
    }

    /// Keeps the newest verified Pkarr record of `key`.
    pub(crate) async fn keep_packet(&self, key: &[u8; 32], item: &n0_mainline::MutableItem) {
        let Some(bytes) = encode_signed_packet(item) else {
            return;
        };
        let result = async {
            let tag = self.store.add_bytes(bytes).await?;
            self.store.tags().set(packet_tag(key), tag.hash).await?;
            n0_error::Ok(())
        };
        if let Err(error) = result.await {
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

fn io_error(error: HttpError) -> std::io::Error {
    std::io::Error::other(error.1)
}
