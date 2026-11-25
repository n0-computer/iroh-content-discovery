# A size-bounded cache on the iroh-blobs store

The gateway's store (`Gateway::with_store`) grows without bound. This note
collects what iroh-blobs 0.105 offers for bounding it, what others do, what
we measured, and a proposal.

## What iroh-blobs gives us

- **Tags.** Named tags (`tags().set`, `list_prefix`, `delete_prefix`) map a
  name to a `HashAndFormat` and live in the store's redb database. A raw tag
  keeps one blob. A `HashSeq` tag also keeps every blob the sequence names.
- **Temp tags.** `blobs().batch().temp_tag(..)` or `tags().temp_tag(..)` keep
  content while the handle lives, and are not persisted.
- **Garbage collection.** `GcConfig { interval, add_protected }` in the
  `FsStore` options runs a mark-and-sweep every `interval`. The mark phase
  treats temp tags, named tags and the children of `HashSeq` roots as live,
  and so does anything the `add_protected` callback adds. Blobs written while
  a run is in progress are protected for that run. GC is off by default.
- **What is missing:**
  - There is no way to run GC now: `gc_run_once` is `pub` but in a private
    module, and `Blobs::delete` is `pub(crate)`. Space is reclaimed only
    on the timer. The issue is
    [iroh-blobs#235](https://github.com/n0-computer/iroh-blobs/issues/235),
    which is still open: the maintainers prefer frequent GC.
  - There is no access time and no size accounting.
  - There is no way to drop a byte range of a blob, only whole blobs.
- **Downloads take no temp tag.** `remote().execute_get` and `fetch` write
  without protection, so a partial blob whose download stalls through a
  whole GC run is swept. Callers must hold a temp tag. #236 only documented
  this.
- The bug where a child of a `HashSeq` root was swept when its root was
  already live (#317) is fixed in 0.105.

## Measured (0.105, ext4, GC every 2 s)

| State | Files | File length | Allocated | `blobs.db` |
| --- | --- | --- | --- | --- |
| 2 MiB of a 64 MiB blob | `.data`, `.obao4`, `.sizes4`, `.bitfield` | 34.2 MiB | 2.1 MiB | 1.0 MiB |
| the whole 64 MiB blob | `.data`, `.obao4` | 65.3 MiB | 64.3 MiB | 1.0 MiB |
| plus 5000 small blobs (inline, under 16 KiB) | the same | 80.3 MiB | 73.3 MiB | 16.1 MiB |
| all tags dropped, after GC | only `blobs.db` | 16.1 MiB | 9.1 MiB | 16.1 MiB |

- **Partial blobs are sparse files.** Their length says nothing. Count
  allocated blocks (`st_blocks`) or the bitfield's `total_bytes()`.
- **The outboard adds about 0.4%** of a blob's size (64 bytes per 16 KiB).
- **Blobs up to 16 KiB are inlined into `blobs.db`.** That file never
  shrinks: freed pages are reused, but the file keeps its peak size, and
  compaction is not exposed.
- **A blob was gone 1.5 s after its tag was dropped,** within one GC interval.

## Prior art: decdn

[decdn](https://github.com/decdn/decdn) runs a size-bounded LRU cache on
`FsStore` (`crates/cache`). The parts that matter here:

- **Units.** Each raw blob has one named tag, which only exists to protect it
  from GC. Eviction drops the tag, and the periodic GC reclaims the space.
  They use no hash sequences.
- **Recency.** Kept in memory (`Hash → Instant`). After a restart every blob
  gets the same old time, so anything used since ranks newer.
- **Footprint.** The content size of complete blobs plus the bitfield bytes of
  partial ones, the latter cached for 30 s, because `observe()` on an idle
  partial blob syncs its files. Outboards and `blobs.db` are not counted. A
  `statvfs` check keeps a minimum of free disk space instead.
- **Loop.** Every second: above 90% of the budget, drop the least recently
  used tags (at most 16 per tick) until 80%. Bytes that were released but not
  yet reclaimed by GC (`pending_reclaim`) are subtracted, so the same
  headroom is not released twice while waiting for the sweep (GC every 300 s
  by default).
- **In use.** An in-memory refcount keeps blobs that are being fetched or
  served out of eviction. Pinned hashes are never evicted.
- **Their open problem** is the one in #235: space comes back only on the GC
  timer.

## Proposal for the gateway

**Unit: one blob, with a raw tag `cache/<hash>`.**
- That covers every blob the gateway stores: files, collection roots and
  their name lists.
- Collections are tagged raw, not as `HashSeq`, so a site's large file can be
  evicted on its own. A page missing a file fetches it again; that is what a
  cache does.
- Per-root eviction would keep sites whole, but one large video would then
  push out everything else of its site.

**Pins: a `HashSeq` tag `pin/<root>`.**
- Content the user installed (home screen apps on Android, "Install as app"
  on desktop) keeps all its files through GC.
- Eviction skips blobs reachable from a pin, so it does not count bytes that
  GC will never free.

**Recency.**
- An in-memory `Hash → last use`, updated when a fetch completes and when a
  response starts.
- Written to a small file in the store directory every few minutes and on
  shutdown, read on start. Blobs not in it count as oldest.
- That keeps the LRU order across restarts without writing the database on
  every request.

**Footprint.**
- What counts is what the disk holds: walk the store's data directory and sum
  allocated blocks, then add the size of `blobs.db`.
- That handles sparse partial blobs, outboards and the database without
  estimates, and costs a directory listing (two to four files per stored
  blob).
- Per-blob estimates (bitfield bytes plus 0.4%) are only used to choose how
  many blobs to release.

**Protection.**
- Every fetch holds a temp tag on its blob from the start, which the draft
  does not do yet. Every response from the store holds one until its body is
  done.
- Those blobs are also left out of eviction, so a blob is not released while
  it is read and fetched again right away.

**Loop.**
- It runs after each fetch that completes and every 10 s otherwise.
- Above the budget's high water mark (90%), it drops `cache/` tags oldest
  first until the target (70%), after subtracting what is still waiting for
  GC.
- The wide gap leaves room for the GC interval (30 s), since GC cannot be
  started on demand.

**Budget.**
- Configurable, defaulting to 512 MiB like the current offline caches.
- Also bounded by free disk space: keep at least 1 GiB, or 10% of the volume,
  free.

**What stays out of reach** without changes upstream:
- reclaiming space right away (#235: a public `gc_run_once`, or a way to wake
  GC early);
- shrinking `blobs.db`;
- evicting part of a blob.

None of these blocks the proposal. They only cost headroom.

## Once this works

The offline caches in front of the gateway become redundant and can be
removed:
- `iroh-link-embedded`'s `cache.rs` on desktop, about 850 lines;
- the cache in Android's `GatewayClient`, about 450 lines.

They exist because the gateway kept nothing. Pkarr names still revalidate in
the browser through the gateway's ETags. Offline, the gateway answers from the
kept record and the store.
