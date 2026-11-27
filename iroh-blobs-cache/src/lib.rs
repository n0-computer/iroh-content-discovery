//! A size-bounded, least-recently-used cache on an iroh-blobs file store.
//!
//! [`BlobCache`] opens an [`FsStore`] with garbage collection on and keeps it
//! within a size limit:
//!
//! - Every blob the cache knows has a raw tag `cache/<hash>`, which keeps it
//!   through garbage collection. [`BlobCache::touch`] creates the tag and
//!   records the use.
//! - Over the high water mark, the least recently used blobs lose their tags
//!   until the footprint gets down to the low water mark, and the next
//!   garbage collection deletes them.
//! - [`BlobCache::hold`] keeps a blob from garbage collection and eviction
//!   while it is being fetched or read, also before it has a tag.
//! - [`BlobCache::pin`] sets a tag `pin/<name>` that eviction never drops. A
//!   hash sequence pin keeps the blobs it names too.
//!
//! The footprint is what the store occupies on disk: allocated blocks of its
//! files (partial blobs are sparse) and its database. The limit drops to keep
//! [`CacheConfig::min_free_bytes`] of the disk free.
//!
//! iroh-blobs offers no way to run garbage collection on demand, so released
//! bytes come back within [`CacheConfig::gc_interval`]. Until then they count
//! as pending and are not released again.
//!
//! The use order is written to a file in the store directory now and then and
//! on [`BlobCache::shutdown`], so it survives restarts. Tagged blobs it does
//! not list count as the oldest.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() -> n0_error::Result<()> {
//! use iroh_blobs::{Hash, HashAndFormat};
//! use iroh_blobs_cache::{BlobCache, CacheConfig};
//!
//! # let dir = tempfile::tempdir()?;
//! let cache = BlobCache::load(dir.path(), CacheConfig::new(512 * 1024 * 1024)).await?;
//! let data = b"some content";
//! let hash = Hash::new(data);
//! // Taken before the blob is stored, so that garbage collection cannot
//! // delete a partial download.
//! let hold = cache.hold(hash).await?;
//! cache.store().add_slice(data).temp_tag().await?;
//! // Records the use, and keeps the blob cached from now on.
//! cache.touch(hash).await?;
//! drop(hold);
//! // Never evicted.
//! cache.pin("my-site", HashAndFormat::raw(hash)).await?;
//! cache.shutdown().await?;
//! # Ok(())
//! # }
//! ```

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::{
    collections::{HashMap, HashSet},
    fmt, io,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// The iroh-blobs crate, whose types appear in this crate's API.
pub use iroh_blobs;
use iroh_blobs::{
    BlobFormat, Hash, HashAndFormat,
    api::{Store, TempTag, proto::BlobStatus},
    hashseq::HashSeq,
    store::{
        GcConfig,
        fs::{FsStore, options::Options},
    },
};
use n0_error::{AnyError, e, ensure, stack_error};
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
/// Passes are spaced so that measuring the store takes at most one part in
/// this many of the time.
const WALK_SHARE: u32 = 10;
/// Size up to which iroh-blobs keeps a blob inline, in its database.
///
/// The database never shrinks, so dropping these blobs frees no disk space.
const INLINE_BYTES: u64 = 16 * 1024;

/// Limits and timing of a [`BlobCache`].
///
/// Start from [`CacheConfig::new`] and change the fields you need. More
/// fields may come in later versions.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CacheConfig {
    /// Upper bound for the store's footprint on disk.
    pub max_bytes: u64,
    /// Disk space to leave free.
    ///
    /// The limit drops below [`Self::max_bytes`] when the disk fills up.
    pub min_free_bytes: u64,
    /// Share of the limit above which eviction starts.
    pub high_water: f64,
    /// Share of the limit that eviction brings the footprint down to.
    pub low_water: f64,
    /// Interval of the store's garbage collection, which deletes evicted blobs.
    pub gc_interval: Duration,
    /// Interval of eviction passes when nothing asks for one sooner.
    ///
    /// Passes come further apart when measuring the store takes long, so that
    /// measuring takes at most a tenth of the time.
    pub eviction_interval: Duration,
    /// How often the use order is written to disk.
    pub save_interval: Duration,
}

impl CacheConfig {
    /// Returns a configuration with a limit of `max_bytes`.
    ///
    /// The other fields have defaults: 1 GiB kept free, eviction from 90%
    /// down to 70%, and garbage collection every 30 seconds.
    pub fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            min_free_bytes: 1024 * 1024 * 1024,
            high_water: 0.9,
            low_water: 0.7,
            gc_interval: Duration::from_secs(30),
            eviction_interval: Duration::from_secs(10),
            save_interval: Duration::from_secs(300),
        }
    }

    /// Rejects values that would make eviction do the wrong thing or spin.
    fn validate(&self) -> Result<(), CacheError> {
        // Written so that NaN fails too.
        ensure!(
            self.low_water > 0.0 && self.low_water <= self.high_water && self.high_water <= 1.0,
            CacheError::Config {
                reason: "watermarks must satisfy 0 < low_water <= high_water <= 1"
            }
        );
        ensure!(
            !self.gc_interval.is_zero()
                && !self.eviction_interval.is_zero()
                && !self.save_interval.is_zero(),
            CacheError::Config {
                reason: "intervals must not be zero"
            }
        );
        Ok(())
    }
}

/// The state of a cache after its last eviction pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Usage {
    /// Bytes the store occupies on disk.
    pub footprint: u64,
    /// Bytes released by eviction that garbage collection has not freed yet.
    pub pending: u64,
    /// The limit in effect, less than the maximum when free disk space is low.
    pub limit: u64,
    /// Number of blobs with a cache tag.
    pub blobs: usize,
}

/// Failure of a [`BlobCache`] operation.
///
/// Every operation that talks to the store can fail with
/// [`CacheError::Store`].
#[stack_error(derive, add_meta)]
#[non_exhaustive]
pub enum CacheError {
    /// The configuration cannot work.
    #[error("invalid cache configuration: {reason}")]
    Config {
        /// What is wrong with it.
        reason: &'static str,
    },
    /// The store could not be opened, or did not carry out a request.
    #[error("blob store failed")]
    Store {
        /// The store's error.
        source: AnyError,
    },
    /// The size of the store on disk could not be measured.
    #[error("cannot measure the store on disk")]
    Footprint {
        /// The underlying IO failure.
        #[error(std_err)]
        source: io::Error,
    },
}

/// A size-bounded cache on an iroh-blobs [`FsStore`].
///
/// Cheap to clone. The eviction task runs until the last clone is dropped or
/// [`Self::shutdown`] is called. Dropping the last clone without a shutdown
/// loses the uses recorded since the use order was last written.
#[derive(Clone)]
pub struct BlobCache {
    inner: Arc<Inner>,
    task: Arc<AbortOnDropHandle<()>>,
}

// Not derived: the state lists every cached blob.
impl fmt::Debug for BlobCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobCache")
            .field("dir", &self.inner.dir)
            .field("max_bytes", &self.max_bytes())
            .field("usage", &self.usage())
            .finish_non_exhaustive()
    }
}

struct Inner {
    store: FsStore,
    dir: PathBuf,
    config: CacheConfig,
    /// [`CacheConfig::max_bytes`], changeable at runtime.
    max_bytes: AtomicU64,
    state: Mutex<State>,
    /// Wakes the eviction task early.
    wake: Notify,
    /// Lets one eviction pass run at a time.
    ///
    /// Two passes that overlap would both release the same excess.
    passes: tokio::sync::Mutex<()>,
}

#[derive(Debug, Default)]
struct State {
    /// Last use of every cached blob, in milliseconds since the Unix epoch.
    used: HashMap<Hash, u64>,
    /// Number of holds per blob.
    held: HashMap<Hash, usize>,
    /// Whether `used` changed since it was written.
    dirty: bool,
    usage: Usage,
    /// When eviction last released something.
    released_at: Option<Instant>,
    /// How long the last pass took to measure the store on disk.
    walk: Duration,
}

/// Keeps a blob from garbage collection and eviction until dropped.
#[must_use = "the blob is only kept while the Hold lives"]
pub struct Hold {
    inner: Arc<Inner>,
    hash: Hash,
    _tag: TempTag,
}

impl fmt::Debug for Hold {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hold")
            .field("hash", &self.hash)
            .finish_non_exhaustive()
    }
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
    ///
    /// The store keeps its database in `blobs.db` and its larger blobs in
    /// `data/`. The cache adds a file `recency` with the use order. Call it
    /// within a Tokio runtime.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Config`] if `config` cannot work, and
    /// [`CacheError::Store`] if the store cannot be opened or its tags cannot
    /// be read.
    pub async fn load(dir: impl AsRef<Path>, config: CacheConfig) -> Result<Self, CacheError> {
        config.validate()?;
        let dir = dir.as_ref().to_owned();
        let mut options = Options::new(&dir);
        options.gc = Some(GcConfig {
            interval: config.gc_interval,
            add_protected: None,
        });
        let store = FsStore::load_with_opts(dir.join("blobs.db"), options)
            .await
            .map_err(|source| e!(CacheError::Store, source))?;
        let used = load_recency(&dir.join(RECENCY_FILE));
        let mut state = State::default();
        // The tags decide what is cached. The file only orders it.
        let mut tags = store
            .tags()
            .list_prefix(CACHE_PREFIX)
            .await
            .map_err(store_failed)?;
        while let Some(tag) = tags.next().await {
            let tag = tag.map_err(store_failed)?;
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
            passes: tokio::sync::Mutex::new(()),
        });
        let task = n0_future::task::spawn(run(inner.clone()));
        Ok(Self {
            inner,
            task: Arc::new(AbortOnDropHandle::new(task)),
        })
    }

    /// Returns the store.
    ///
    /// Add and read blobs through it as usual. The cache owns the tags that
    /// start with `cache/` and `pin/`, so leave those to it.
    pub fn store(&self) -> &Store {
        &self.inner.store
    }

    /// Records a use of `hash`, adding it to the cache if it is new.
    ///
    /// Call it when a blob was stored or read. It also wakes the eviction
    /// task, since the store may have grown.
    pub async fn touch(&self, hash: Hash) -> Result<(), CacheError> {
        if !self.inner.renew(hash) {
            // The tag goes first. If this future is dropped on the way, the
            // blob is not listed, and the next touch sets the tag again.
            self.inner
                .store
                .tags()
                .set(cache_tag(hash), HashAndFormat::raw(hash))
                .await
                .map_err(store_failed)?;
            self.inner.insert(hash);
        }
        self.inner.wake.notify_one();
        Ok(())
    }

    /// Keeps `hash` from garbage collection and eviction.
    ///
    /// The protection lasts while the returned [`Hold`] lives. Take it before
    /// fetching a blob into the store, or garbage collection may delete a
    /// partial download, and while reading one.
    pub async fn hold(&self, hash: Hash) -> Result<Hold, CacheError> {
        let tag = self
            .inner
            .store
            .tags()
            .temp_tag(HashAndFormat::raw(hash))
            .await
            .map_err(store_failed)?;
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

    /// Pins `content` under `name`, so that eviction never drops it.
    ///
    /// A hash sequence pin keeps the blobs it names too.
    pub async fn pin(&self, name: &str, content: HashAndFormat) -> Result<(), CacheError> {
        self.inner
            .store
            .tags()
            .set(format!("{PIN_PREFIX}{name}"), content)
            .await
            .map_err(store_failed)?;
        Ok(())
    }

    /// Removes the pin `name`.
    ///
    /// Its blobs stay cached if they have been used.
    pub async fn unpin(&self, name: &str) -> Result<(), CacheError> {
        self.inner
            .store
            .tags()
            .delete(format!("{PIN_PREFIX}{name}"))
            .await
            .map_err(store_failed)?;
        Ok(())
    }

    /// Returns the upper bound for the store's footprint.
    pub fn max_bytes(&self) -> u64 {
        self.inner.max_bytes.load(Ordering::Relaxed)
    }

    /// Changes the upper bound for the store's footprint.
    ///
    /// A smaller bound starts an eviction pass.
    pub fn set_max_bytes(&self, max_bytes: u64) {
        info!(max_bytes, "blob cache limit changed");
        self.inner.max_bytes.store(max_bytes, Ordering::Relaxed);
        self.inner.wake.notify_one();
    }

    /// Drops every cached blob.
    ///
    /// Garbage collection deletes them, except pinned and held ones.
    pub async fn clear(&self) -> Result<(), CacheError> {
        info!("clearing the blob cache");
        self.inner
            .store
            .tags()
            .delete_prefix(CACHE_PREFIX)
            .await
            .map_err(store_failed)?;
        let mut state = self.inner.state.lock().expect("poisoned");
        state.used.clear();
        state.dirty = true;
        Ok(())
    }

    /// Returns the state after the last eviction pass.
    ///
    /// Before the first pass, every field is zero.
    pub fn usage(&self) -> Usage {
        self.inner.state.lock().expect("poisoned").usage
    }

    /// Runs an eviction pass now and returns the state after it.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Footprint`] if the store cannot be measured on
    /// disk, and [`CacheError::Store`] if its pins cannot be read.
    pub async fn evict(&self) -> Result<Usage, CacheError> {
        self.inner.pass().await
    }

    /// Stops the eviction task, writes the use order, and shuts the store down.
    ///
    /// The store is shut down for every clone, so their operations fail
    /// afterwards.
    pub async fn shutdown(&self) -> Result<(), CacheError> {
        self.task.abort();
        self.inner.save().await;
        self.inner.store.shutdown().await.map_err(store_failed)?;
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

    /// Measures the store and releases least recently used blobs if it is too large.
    ///
    /// Releasing starts above the high water mark and stops at the low water mark.
    async fn pass(&self) -> Result<Usage, CacheError> {
        let _pass = self.passes.lock().await;
        let walk_started = Instant::now();
        let footprint = footprint(self.dir.clone()).await?;
        let walk = walk_started.elapsed();
        let free = fs4::available_space(&self.dir).unwrap_or(u64::MAX);
        let limit = self
            .max_bytes
            .load(Ordering::Relaxed)
            .min(footprint.saturating_add(free.saturating_sub(self.config.min_free_bytes)));
        let pending = {
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
            state.usage.pending
        };
        debug!(
            footprint,
            pending,
            limit,
            walk_ms = walk.as_millis(),
            "measured the store"
        );
        let effective = footprint.saturating_sub(pending);
        let high = (limit as f64 * self.config.high_water) as u64;
        let mut released = 0;
        if effective > high {
            let need = effective.saturating_sub((limit as f64 * self.config.low_water) as u64);
            let pinned = self.pinned().await?;
            for hash in self.candidates() {
                if released >= need {
                    break;
                }
                if pinned.contains(&hash) {
                    continue;
                }
                let bytes = self.reclaimable(hash).await;
                // Garbage collection reads temp tags before named ones, so it
                // can miss a blob whose cache tag goes while a hold comes.
                if self
                    .state
                    .lock()
                    .expect("poisoned")
                    .held
                    .contains_key(&hash)
                {
                    continue;
                }
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
                    // Held while the tag went: keep it.
                    let restored = self
                        .store
                        .tags()
                        .set(cache_tag(hash), HashAndFormat::raw(hash))
                        .await;
                    if let Err(error) = restored {
                        // Unlisted, so that the next touch tags it again.
                        warn!(%error, %hash, "cannot restore cache tag");
                        let mut state = self.state.lock().expect("poisoned");
                        state.used.remove(&hash);
                        state.dirty = true;
                    }
                    continue;
                }
                released += bytes;
                debug!(%hash, bytes, "evicted");
            }
            info!(
                footprint,
                limit, released, "evicted least recently used blobs"
            );
        }
        let mut state = self.state.lock().expect("poisoned");
        if released > 0 {
            state.released_at = Some(Instant::now());
        }
        state.walk = walk;
        state.usage = Usage {
            footprint,
            pending: state.usage.pending + released,
            limit,
            blobs: state.used.len(),
        };
        Ok(state.usage)
    }

    /// Returns the listed blobs that are not held, least recently used first.
    fn candidates(&self) -> Vec<Hash> {
        let mut candidates: Vec<(u64, Hash)> = {
            let state = self.state.lock().expect("poisoned");
            state
                .used
                .iter()
                .filter(|(hash, _)| !state.held.contains_key(*hash))
                .map(|(hash, used)| (*used, *hash))
                .collect()
        };
        // Sorted outside the lock, which every touch takes: at 100k blobs
        // the sort takes about 16 ms.
        candidates.sort_unstable();
        candidates.into_iter().map(|(_, hash)| hash).collect()
    }

    /// Returns the blobs that pins keep.
    ///
    /// These are the pinned roots and the children of hash sequence pins.
    async fn pinned(&self) -> Result<HashSet<Hash>, CacheError> {
        let mut pinned = HashSet::new();
        let mut tags = self
            .store
            .tags()
            .list_prefix(PIN_PREFIX)
            .await
            .map_err(store_failed)?;
        while let Some(tag) = tags.next().await {
            let tag = tag.map_err(store_failed)?;
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

    /// Estimates the disk space that deleting `hash` frees.
    ///
    /// That is its data and outboard, or nothing for a blob inlined into the
    /// database.
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
        let error = match result {
            Ok(Ok(())) => return,
            Ok(Err(error)) => error,
            Err(error) => io::Error::other(error),
        };
        warn!(%error, "cannot save the use order");
        // Tried again at the next save.
        self.state.lock().expect("poisoned").dirty = true;
    }
}

/// The eviction task: a pass every tick, or sooner after a touch.
///
/// [`BlobCache`] aborts it when its last clone is dropped.
async fn run(inner: Arc<Inner>) {
    let mut last_save = Instant::now();
    loop {
        if let Err(error) = inner.pass().await {
            warn!(%error, "eviction pass failed");
        }
        if last_save.elapsed() >= inner.config.save_interval {
            inner.save().await;
            last_save = Instant::now();
        }
        // Walking a large store takes long, up to seconds at 100k blobs, so
        // passes come further apart then.
        let walk = inner.state.lock().expect("poisoned").walk;
        let pause = MIN_PASS_GAP.max(walk * WALK_SHARE);
        let woken = tokio::select! {
            _ = inner.wake.notified() => true,
            _ = tokio::time::sleep(inner.config.eviction_interval.max(pause)) => false,
        };
        if woken {
            tokio::time::sleep(pause).await;
        }
    }
}

/// Returns the bytes allocated by the files below `dir`.
async fn footprint(dir: PathBuf) -> Result<u64, CacheError> {
    tokio::task::spawn_blocking(move || allocated(&dir))
        .await
        .map_err(io::Error::other)
        .flatten()
        .map_err(|source| e!(CacheError::Footprint, source))
}

/// Wraps a failure of the store into [`CacheError::Store`].
#[track_caller]
fn store_failed(error: impl std::error::Error + Send + Sync + 'static) -> CacheError {
    e!(CacheError::Store, AnyError::from_std(error))
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

/// Returns the disk space a file occupies.
///
/// That is less than its length if it is sparse.
#[cfg(unix)]
fn allocated_size(metadata: &std::fs::Metadata) -> u64 {
    metadata.blocks() * 512
}

#[cfg(not(unix))]
fn allocated_size(metadata: &std::fs::Metadata) -> u64 {
    metadata.len()
}

/// The use order, as written to [`RECENCY_FILE`].
#[derive(Serialize, Deserialize)]
struct Recency {
    entries: Vec<([u8; 32], u64)>,
}

fn load_recency(path: &Path) -> HashMap<Hash, u64> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return HashMap::new(),
        Err(error) => {
            warn!(%error, "cannot read the use order; starting without it");
            return HashMap::new();
        }
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
