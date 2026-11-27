# iroh-blobs-cache

A size-bounded, least-recently-used cache on an
[iroh-blobs](https://github.com/n0-computer/iroh-blobs) file store.

```rust
let cache = BlobCache::load(dir, CacheConfig::new(512 * 1024 * 1024)).await?;
let store = cache.store();

// Before fetching into the store: keeps a partial download from garbage collection.
let hold = cache.hold(hash).await?;
store.remote().fetch(connection, hash).complete().await?;
// Records the use, and caches the blob from now on.
cache.touch(hash).await?;
drop(hold);

// Never evicted. A hash sequence pin keeps the blobs it names too.
cache.pin("my-site", HashAndFormat::hash_seq(root)).await?;
```

- Every cached blob has a raw tag `cache/<hash>`. Above 90% of the limit,
  the least recently used lose their tags until the footprint is down to
  70%, and the store's garbage collection deletes them.
- The footprint is what the store occupies on disk: allocated blocks of its
  files (partial blobs are sparse files) and its database. The limit drops
  to keep `min_free_bytes` of the disk free.
- iroh-blobs cannot run garbage collection on demand
  ([#235](https://github.com/n0-computer/iroh-blobs/issues/235)), so released
  bytes come back within `gc_interval`. Until then they count as pending and
  are not released twice.
- The use order is written to a file in the store directory now and then and
  on `shutdown`, so it survives restarts.
- Blobs up to 16 KiB live in the store's database, which never shrinks, so
  evicting them frees no disk space.

See [the design notes](https://github.com/n0-computer/iroh-content-discovery/blob/main/iroh-link-gateway/docs/store-cache.md)
for the reasoning, measurements, and how [decdn](https://github.com/decdn/decdn)
does the same.

## License

This project is licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  https://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
