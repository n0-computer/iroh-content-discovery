//! Eviction against a real file store, with fast garbage collection.

use std::time::Duration;

use iroh_blobs::{Hash, HashAndFormat};
use iroh_blobs_cache::{BlobCache, CacheConfig};

const MIB: u64 = 1024 * 1024;

fn config(max_bytes: u64) -> CacheConfig {
    let mut config = CacheConfig::new(max_bytes);
    config.min_free_bytes = 0;
    config.gc_interval = Duration::from_millis(200);
    // Passes only when a test asks for one.
    config.eviction_interval = Duration::from_secs(3600);
    config
}

/// Stores a distinct 1 MiB blob and records its use.
async fn add(cache: &BlobCache, seed: u8) -> Hash {
    let data: Vec<u8> = (0..MIB as usize)
        .map(|index| (index as u8).wrapping_mul(31) ^ seed)
        .collect();
    let tag = cache
        .store()
        .blobs()
        .add_bytes(data)
        .temp_tag()
        .await
        .unwrap();
    cache.touch(tag.hash()).await.unwrap();
    tag.hash()
}

/// Waits until garbage collection deleted `hash`, or panics.
async fn wait_gone(cache: &BlobCache, hash: Hash) {
    for _ in 0..50 {
        if !cache.store().has(hash).await.unwrap() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{hash} was not collected");
}

/// Waits for two garbage collections.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(600)).await;
}

#[tokio::test]
async fn least_recently_used_blobs_go_first() {
    let dir = tempfile::tempdir().unwrap();
    let cache = BlobCache::load(dir.path(), config(10 * MIB)).await.unwrap();
    let mut blobs = Vec::new();
    for seed in 0..10 {
        blobs.push(add(&cache, seed).await);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    // The oldest becomes the newest.
    cache.touch(blobs[0]).await.unwrap();

    let usage = cache.evict().await.unwrap();
    assert!(usage.pending > 0, "{usage:?}");
    wait_gone(&cache, blobs[1]).await;
    settle().await;
    assert!(cache.store().has(blobs[0]).await.unwrap());
    assert!(cache.store().has(blobs[9]).await.unwrap());

    // Freed bytes leave the pending count, and nothing more is evicted.
    let usage = cache.evict().await.unwrap();
    assert!(usage.footprint <= 9 * MIB, "{usage:?}");
    let kept = usage.blobs;
    let usage = cache.evict().await.unwrap();
    assert_eq!(usage.blobs, kept, "{usage:?}");
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn pinned_and_held_blobs_stay() {
    let dir = tempfile::tempdir().unwrap();
    let cache = BlobCache::load(dir.path(), config(4 * MIB)).await.unwrap();
    let pinned = add(&cache, 1).await;
    let held = add(&cache, 2).await;
    let plain = add(&cache, 3).await;
    cache.pin("site", HashAndFormat::raw(pinned)).await.unwrap();
    let _hold = cache.hold(held).await.unwrap();
    // Like a download in progress: held, but not cached yet.
    let _download = cache.hold(Hash::new(b"not stored yet")).await.unwrap();
    let data = vec![9u8; MIB as usize];
    let fetching = cache
        .store()
        .blobs()
        .add_bytes(data)
        .temp_tag()
        .await
        .unwrap();
    let fetching_hash = fetching.hash();
    let _fetch_hold = cache.hold(fetching_hash).await.unwrap();
    drop(fetching);

    cache.evict().await.unwrap();
    wait_gone(&cache, plain).await;
    settle().await;
    assert!(cache.store().has(pinned).await.unwrap());
    assert!(cache.store().has(held).await.unwrap());
    assert!(cache.store().has(fetching_hash).await.unwrap());
    // Eviction skipped them rather than counting bytes GC would not free.
    for hash in [pinned, held] {
        let tag = cache
            .store()
            .tags()
            .get(format!("cache/{hash}"))
            .await
            .unwrap();
        assert!(tag.is_some(), "{hash} lost its cache tag");
    }
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn the_use_order_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let cache = BlobCache::load(dir.path(), config(64 * MIB)).await.unwrap();
    let old = add(&cache, 1).await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    let new = add(&cache, 2).await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    cache.touch(old).await.unwrap();
    cache.shutdown().await.unwrap();
    drop(cache);

    // Room for one of the two only: the one used last stays.
    let cache = BlobCache::load(dir.path(), config(2 * MIB)).await.unwrap();
    assert_eq!(cache.evict().await.unwrap().blobs, 1);
    wait_gone(&cache, new).await;
    assert!(cache.store().has(old).await.unwrap());
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_smaller_limit_evicts() {
    let dir = tempfile::tempdir().unwrap();
    let cache = BlobCache::load(dir.path(), config(64 * MIB)).await.unwrap();
    let old = add(&cache, 1).await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    let new = add(&cache, 2).await;
    assert_eq!(cache.evict().await.unwrap().blobs, 2);
    cache.set_max_bytes(2 * MIB);
    assert_eq!(cache.max_bytes(), 2 * MIB);
    assert_eq!(cache.evict().await.unwrap().blobs, 1);
    wait_gone(&cache, old).await;
    assert!(cache.store().has(new).await.unwrap());
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn clear_drops_everything_but_pins() {
    let dir = tempfile::tempdir().unwrap();
    let cache = BlobCache::load(dir.path(), config(64 * MIB)).await.unwrap();
    let pinned = add(&cache, 1).await;
    let plain = add(&cache, 2).await;
    cache.pin("keep", HashAndFormat::raw(pinned)).await.unwrap();
    cache.clear().await.unwrap();
    wait_gone(&cache, plain).await;
    assert!(cache.store().has(pinned).await.unwrap());
    assert_eq!(cache.evict().await.unwrap().blobs, 0);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn overlapping_passes_release_the_excess_once() {
    let dir = tempfile::tempdir().unwrap();
    let cache = BlobCache::load(dir.path(), config(10 * MIB)).await.unwrap();
    for seed in 0..10 {
        add(&cache, seed).await;
    }
    let (first, second) = tokio::join!(cache.evict(), cache.evict());
    first.unwrap();
    second.unwrap();
    // About 10 MiB on disk and a low water mark of 7 MiB: one pass releases
    // about 4 MiB, and the other finds nothing left to do.
    let usage = cache.usage();
    assert!(usage.pending <= 5 * MIB, "{usage:?}");
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_low_water_mark_above_the_high_one_is_not_an_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(10 * MIB);
    config.low_water = 2.0;
    let cache = BlobCache::load(dir.path(), config).await.unwrap();
    for seed in 0..10 {
        add(&cache, seed).await;
    }
    // Over the high water mark, but under the low one: nothing to release.
    assert_eq!(cache.evict().await.unwrap().blobs, 10);
    cache.shutdown().await.unwrap();
}
