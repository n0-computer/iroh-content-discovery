//! A size-bounded, least-recently-used cache on an iroh-blobs file store.
//!
//! [`BlobCache`] opens an [`FsStore`] with garbage collection on and keeps it
//! within a byte budget:
//!
//! - Every blob the cache knows has a raw tag `cache/<hash>`, which keeps it
//!   through garbage collection. [`BlobCache::touch`] creates the tag and
//!   records the use.
//! - Over the high water mark, the least recently used blobs lose their tags
//!   until the footprint gets down to the target, and the next garbage
//!   collection deletes them.
//! - [`BlobCache::hold`] keeps a blob from garbage collection and eviction
//!   while it is being fetched or read, also before it has a tag.
//! - [`BlobCache::pin`] sets a tag `pin/<name>` that eviction never drops. A
//!   hash sequence pin keeps the blobs it names too.
//!
//! The footprint is what the store occupies on disk: allocated blocks of its
//! files (partial blobs are sparse) and its database. The budget shrinks to
//! keep [`CacheConfig::min_free_bytes`] of the disk free.
//!
//! iroh-blobs offers no way to run garbage collection on demand, so released
//! bytes come back within [`CacheConfig::gc_interval`]. Until then they count
//! as pending and are not released again.
//!
//! The use order is written to a file in the store directory now and then and
//! on [`BlobCache::shutdown`], so it survives restarts. Tagged blobs it does
//! not list count as the oldest.

use std::{
    collections::{HashMap, HashSet},
    io,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use iroh_blobs::{
    BlobFormat, Hash, HashAndFormat,
    api::{Store, TempTag, proto::BlobStatus},
    hashseq::HashSeq,
    store::{
        GcConfig,
        fs::{FsStore, options::Options},
    },
};
use n0_error::{Result, StdResultExt};
use n0_future::{StreamExt, task::AbortOnDropHandle};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tracing::{debug, info, warn};

/// Tag prefix of cached blobs.
const CACHE_PREFIX: &str = "cache/";
/// Tag prefix of pins.
const PIN_PREFIX: &str = "pin/";
/// File with the use order, in the store directory.
const RECENCY_FILE: &str = "recency";
/// Shortest pause between two eviction passes.
const MIN_PASS_GAP: Duration = Duration::from_secs(1);
/// Blobs up to this size live in the store's database, which never shrinks,
/// so dropping them frees no disk space (iroh-blobs' inline limit).
const INLINE_BYTES: u64 = 16 * 1024;

/// Limits and timing of a [`BlobCache`].
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Upper bound for the store's footprint on disk.
    pub max_bytes: u64,
    /// Disk space to leave free. The budget shrinks below
    /// [`Self::max_bytes`] when the disk fills up.
    pub min_free_bytes: u64,
    /// Share of the budget above which eviction starts.
    pub high_water: f64,
    /// Share of the budget that eviction brings the footprint down to.
    pub target: f64,
    /// Interval of the store's garbage collection, which deletes evicted blobs.
    pub gc_interval: Duration,
    /// Interval of eviction passes when nothing asks for one sooner.
    pub tick: Duration,
    /// How often the use order is written to disk.
    pub save_interval: Duration,
}

impl CacheConfig {
    /// Returns a configuration with a budget of `max_bytes` and defaults
    /// otherwise: 1 GiB kept free, eviction from 90% down to 70%, garbage
    /// collection every 30 seconds.
    pub fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            min_free_bytes: 1024 * 1024 * 1024,
            high_water: 0.9,
            target: 0.7,
            gc_interval: Duration::from_secs(30),
            tick: Duration::from_secs(10),
            save_interval: Duration::from_secs(300),
        }
    }
}

/// The state of a cache after its last eviction pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    /// Bytes the store occupies on disk.
    pub footprint: u64,
    /// Bytes released by eviction that garbage collection has not freed yet.
    pub pending: u64,
    /// The budget, after keeping free disk space.
    pub budget: u64,
    /// Blobs with a cache tag.
    pub blobs: usize,
}

/// A size-bounded cache on an iroh-blobs [`FsStore`].
///
/// Cheap to clone. The eviction task runs until the last clone is dropped or
/// [`Self::shutdown`] is called.
#[derive(Debug, Clone)]
pub struct BlobCache {
    inner: Arc<Inner>,
    _task: Arc<AbortOnDropHandle<()>>,
}

#[derive(Debug)]
struct Inner {
    store: FsStore,
    dir: PathBuf,
    config: CacheConfig,
    /// [`CacheConfig::max_bytes`], changeable at runtime.
    max_bytes: AtomicU64,
    state: Mutex<State>,
    /// Wakes the eviction task early.
    wake: Notify,
}

#[derive(Debug, Default)]
struct State {
    /// Last use of every cached blob, in milliseconds since the Unix epoch.
    used: HashMap<Hash, u64>,
    /// Holds per blob.
    held: HashMap<Hash, usize>,
    /// Whether `used` changed since it was written.
    dirty: bool,
    usage: Usage,
    /// When eviction last released something.
    released_at: Option<std::time::Instant>,
}

/// Keeps a blob from garbage collection and eviction until dropped.
#[derive(Debug)]
pub struct Hold {
    inner: Arc<Inner>,
    hash: Hash,
    _tag: TempTag,
}

impl Drop for Hold {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().expect("poisoned");
        if let Some(count) = state.held.get_mut(&self.hash) {
            *count -= 1;
            if *count == 0 {
                state.held.remove(&self.hash);
            }
        }
    }
}

impl BlobCache {
    /// Opens or creates the store in `dir` and starts the eviction task.
    pub async fn load(dir: impl AsRef<Path>, config: CacheConfig) -> Result<Self> {
        let dir = dir.as_ref().to_owned();
        let mut options = Options::new(&dir);
        options.gc = Some(GcConfig {
            interval: config.gc_interval,
            add_protected: None,
        });
        let store = FsStore::load_with_opts(dir.join("blobs.db"), options).await?;
        let used = load_recency(&dir.join(RECENCY_FILE));
        let mut state = State::default();
        // The tags decide what is cached. The file only orders it.
        let mut tags = store.tags().list_prefix(CACHE_PREFIX).await?;
        while let Some(tag) = tags.next().await {
            let tag = tag?;
            state
                .used
                .insert(tag.hash, used.get(&tag.hash).copied().unwrap_or(0));
        }
        info!(blobs = state.used.len(), dir = %dir.display(), "blob cache loaded");
        let inner = Arc::new(Inner {
            store,
            dir,
            max_bytes: AtomicU64::new(config.max_bytes),
            config,
            state: Mutex::new(state),
            wake: Notify::new(),
        });
        let task = n0_future::task::spawn(run(inner.clone()));
        Ok(Self {
            inner,
            _task: Arc::new(AbortOnDropHandle::new(task)),
        })
    }

    /// Returns the store.
    pub fn store(&self) -> &Store {
        &self.inner.store
    }

    /// Records a use of `hash`, adding it to the cache if it is new.
    ///
    /// Call it when a blob was stored or read. It also wakes the eviction
    /// task, since the store may have grown.
    pub async fn touch(&self, hash: Hash) -> Result<()> {
        if !self.inner.renew(hash) {
            // The tag goes first. If this future is dropped on the way, the
            // blob is not listed, and the next touch sets the tag again.
            self.inner
                .store
                .tags()
                .set(cache_tag(hash), HashAndFormat::raw(hash))
                .await?;
            self.inner.insert(hash);
        }
        self.inner.wake.notify_one();
        Ok(())
    }

    /// Keeps `hash` from garbage collection and eviction while the returned
    /// [`Hold`] lives.
    ///
    /// Take it before fetching a blob into the store, or garbage collection
    /// may delete a partial download, and while reading one.
    pub async fn hold(&self, hash: Hash) -> Result<Hold> {
        let tag = self
            .inner
            .store
            .tags()
            .temp_tag(HashAndFormat::raw(hash))
            .await
            .anyerr()?;
        *self
            .inner
            .state
            .lock()
            .expect("poisoned")
            .held
            .entry(hash)
            .or_default() += 1;
        Ok(Hold {
            inner: self.inner.clone(),
            hash,
            _tag: tag,
        })
    }

    /// Pins `content` under `name`. Eviction never drops it. A hash sequence
    /// pin keeps the blobs it names too.
    pub async fn pin(&self, name: &str, content: HashAndFormat) -> Result<()> {
        self.inner
            .store
            .tags()
            .set(format!("{PIN_PREFIX}{name}"), content)
            .await?;
        Ok(())
    }

    /// Removes the pin `name`. Its blobs stay cached if they have been used.
    pub async fn unpin(&self, name: &str) -> Result<()> {
        self.inner
            .store
            .tags()
            .delete(format!("{PIN_PREFIX}{name}"))
            .await?;
        Ok(())
    }

    /// Returns the upper bound for the store's footprint.
    pub fn max_bytes(&self) -> u64 {
        self.inner.max_bytes.load(Ordering::Relaxed)
    }

    /// Changes the upper bound for the store's footprint. A smaller bound
    /// starts an eviction pass.
    pub fn set_max_bytes(&self, max_bytes: u64) {
        self.inner.max_bytes.store(max_bytes, Ordering::Relaxed);
        self.inner.wake.notify_one();
    }

    /// Drops every cached blob. Garbage collection deletes them, except
    /// pinned and held ones.
    pub async fn clear(&self) -> Result<()> {
        self.inner.store.tags().delete_prefix(CACHE_PREFIX).await?;
        let mut state = self.inner.state.lock().expect("poisoned");
        state.used.clear();
        state.dirty = true;
        Ok(())
    }

    /// Returns the state after the last eviction pass.
    pub fn usage(&self) -> Usage {
        self.inner.state.lock().expect("poisoned").usage
    }

    /// Runs an eviction pass now and returns the state after it.
    pub async fn evict(&self) -> Result<Usage> {
        self.inner.pass().await
    }

    /// Writes the use order and shuts the store down.
    pub async fn shutdown(&self) -> Result<()> {
        self.inner.save().await;
        self.inner.store.shutdown().await?;
        Ok(())
    }
}

impl Inner {
    /// Records a use of `hash` if it is listed, and returns whether it is.
    fn renew(&self, hash: Hash) -> bool {
        let mut state = self.state.lock().expect("poisoned");
        let Some(used) = state.used.get_mut(&hash) else {
            return false;
        };
        *used = now_ms();
        state.dirty = true;
        true
    }

    /// Lists `hash` as used now.
    fn insert(&self, hash: Hash) {
        let mut state = self.state.lock().expect("poisoned");
        state.used.insert(hash, now_ms());
        state.dirty = true;
    }

    /// Measures the store, and releases the least recently used blobs if it
    /// is over the high water mark.
    async fn pass(&self) -> Result<Usage> {
        let footprint = footprint(self.dir.clone()).await?;
        let free = fs4::available_space(&self.dir).unwrap_or(u64::MAX);
        let budget = self
            .max_bytes
            .load(Ordering::Relaxed)
            .min(footprint.saturating_add(free.saturating_sub(self.config.min_free_bytes)));
        let (pending, candidates) = {
            let mut state = self.state.lock().expect("poisoned");
            // A smaller footprint means garbage collection freed bytes.
            let freed = state.usage.footprint.saturating_sub(footprint);
            state.usage.pending = state.usage.pending.saturating_sub(freed);
            // After two collections, whatever was released is gone.
            if state
                .released_at
                .is_some_and(|at| at.elapsed() > self.config.gc_interval * 2)
            {
                state.usage.pending = 0;
                state.released_at = None;
            }
            let mut candidates: Vec<(u64, Hash)> = state
                .used
                .iter()
                .filter(|(hash, _)| !state.held.contains_key(*hash))
                .map(|(hash, used)| (*used, *hash))
                .collect();
            candidates.sort_unstable();
            (state.usage.pending, candidates)
        };
        let effective = footprint.saturating_sub(pending);
        let high = (budget as f64 * self.config.high_water) as u64;
        let mut released = 0;
        if effective > high {
            let need = effective - (budget as f64 * self.config.target) as u64;
            let pinned = self.pinned().await?;
            for (_, hash) in candidates {
                if released >= need {
                    break;
                }
                if pinned.contains(&hash) {
                    continue;
                }
                let bytes = self.reclaimable(hash).await;
                // Tag first, then the use: if dropping the tag fails, the
                // blob stays a candidate.
                if let Err(error) = self.store.tags().delete(cache_tag(hash)).await {
                    warn!(%error, %hash, "cannot drop cache tag");
                    continue;
                }
                let held_again = {
                    let mut state = self.state.lock().expect("poisoned");
                    let held = state.held.contains_key(&hash);
                    if !held {
                        state.used.remove(&hash);
                        state.dirty = true;
                    }
                    held
                };
                if held_again {
                    // Held while we were busy: keep it.
                    let _ = self
                        .store
                        .tags()
                        .set(cache_tag(hash), HashAndFormat::raw(hash))
                        .await;
                    continue;
                }
                released += bytes;
                debug!(%hash, bytes, "evicted");
            }
            info!(
                footprint,
                budget, released, "evicted least recently used blobs"
            );
        }
        let mut state = self.state.lock().expect("poisoned");
        if released > 0 {
            state.released_at = Some(std::time::Instant::now());
        }
        state.usage = Usage {
            footprint,
            pending: state.usage.pending + released,
            budget,
            blobs: state.used.len(),
        };
        Ok(state.usage)
    }

    /// Returns the blobs that pins keep: the pinned roots and the children of
    /// hash sequence pins.
    async fn pinned(&self) -> Result<HashSet<Hash>> {
        let mut pinned = HashSet::new();
        let mut tags = self.store.tags().list_prefix(PIN_PREFIX).await?;
        while let Some(tag) = tags.next().await {
            let tag = tag?;
            pinned.insert(tag.hash);
            if tag.format == BlobFormat::HashSeq
                && let Ok(bytes) = self.store.get_bytes(tag.hash).await
                && let Some(seq) = HashSeq::new(bytes)
            {
                pinned.extend(seq);
            }
        }
        Ok(pinned)
    }

    /// Estimates the disk space that deleting `hash` frees: its data and
    /// outboard, or nothing for blobs inlined into the database.
    async fn reclaimable(&self, hash: Hash) -> u64 {
        let bytes = match self.store.blobs().status(hash).await {
            Ok(BlobStatus::Complete { size }) => size,
            Ok(BlobStatus::Partial { .. }) => match self.store.observe(hash).await {
                Ok(bitfield) => bitfield.total_bytes(),
                Err(_) => 0,
            },
            _ => 0,
        };
        if bytes <= INLINE_BYTES {
            0
        } else {
            // The outboard holds 64 bytes per 16 KiB.
            bytes + bytes / 256
        }
    }

    /// Writes the use order if it changed.
    async fn save(&self) {
        let entries: Vec<([u8; 32], u64)> = {
            let mut state = self.state.lock().expect("poisoned");
            if !state.dirty {
                return;
            }
            state.dirty = false;
            state
                .used
                .iter()
                .map(|(hash, used)| (*hash.as_bytes(), *used))
                .collect()
        };
        let path = self.dir.join(RECENCY_FILE);
        let result = tokio::task::spawn_blocking(move || -> io::Result<()> {
            let bytes = postcard::to_stdvec(&Recency { entries }).map_err(io::Error::other)?;
            let temp = path.with_extension("tmp");
            std::fs::write(&temp, bytes)?;
            std::fs::rename(temp, path)
        })
        .await;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!(%error, "cannot save the use order"),
            Err(error) => warn!(%error, "cannot save the use order"),
        }
    }
}

/// The eviction task: a pass every tick, or sooner after a touch.
///
/// [`BlobCache`] aborts it when its last clone is dropped.
async fn run(inner: Arc<Inner>) {
    let mut last_save = std::time::Instant::now();
    loop {
        if let Err(error) = inner.pass().await {
            warn!(%error, "eviction pass failed");
        }
        if last_save.elapsed() >= inner.config.save_interval {
            inner.save().await;
            last_save = std::time::Instant::now();
        }
        let woken = tokio::select! {
            _ = inner.wake.notified() => true,
            _ = tokio::time::sleep(inner.config.tick) => false,
        };
        if woken {
            tokio::time::sleep(MIN_PASS_GAP).await;
        }
    }
}

/// Bytes allocated by the files below `dir`.
async fn footprint(dir: PathBuf) -> Result<u64> {
    tokio::task::spawn_blocking(move || allocated(&dir))
        .await
        .anyerr()?
        .anyerr()
}

fn allocated(dir: &Path) -> io::Result<u64> {
    let mut total = 0;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total += allocated(&entry.path())?;
        } else {
            total += allocated_size(&metadata);
        }
    }
    Ok(total)
}

/// Disk space a file occupies, which is less than its length if it is sparse.
#[cfg(unix)]
fn allocated_size(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.blocks() * 512
}

#[cfg(not(unix))]
fn allocated_size(metadata: &std::fs::Metadata) -> u64 {
    metadata.len()
}

#[derive(Serialize, Deserialize)]
struct Recency {
    entries: Vec<([u8; 32], u64)>,
}

fn load_recency(path: &Path) -> HashMap<Hash, u64> {
    let Ok(bytes) = std::fs::read(path) else {
        return HashMap::new();
    };
    match postcard::from_bytes::<Recency>(&bytes) {
        Ok(recency) => recency
            .entries
            .into_iter()
            .map(|(hash, used)| (Hash::from_bytes(hash), used))
            .collect(),
        Err(error) => {
            warn!(%error, "cannot read the use order; starting without it");
            HashMap::new()
        }
    }
}

fn cache_tag(hash: Hash) -> String {
    format!("{CACHE_PREFIX}{hash}")
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}
