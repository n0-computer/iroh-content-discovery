# iroh-local-gateway

A localhost HTTP gateway for content-addressed files, including video. This is
workspace project four, adapted from the streaming approach in
[`iroh-examples/iroh-gateway`](https://github.com/n0-computer/iroh-examples/tree/main/iroh-gateway).

## Try it locally

With the browser extension installed and enabled on port 8080:

```sh
cargo run -p iroh-local-gateway --example demo -- /path/to/video.mp4
```

Open the printed `https://<z32>.blake3.link/` URL and leave the command running.
This starts a local DHT testnet, tracker, file provider, and HTTP gateway in one
process. The content still travels through real iroh connections and tracker
discovery, but no public DHT or relay is needed. Files are imported into a
temporary disk-backed store cleaned up on normal shutdown. The example may use
additional disk space roughly equal to the file size.

Omit the file argument for a quick text greeting. Use `--port 8081` if needed,
and set the same port in the extension. The printed direct localhost URL also
works without the extension. Ctrl-C stops the demo.

## Standalone gateway

```sh
cargo run -p iroh-local-gateway -- --listen 127.0.0.1:8080 --tracker 127.0.0.1:11223
```

Open:

```text
http://127.0.0.1:8080/blake3/<z32>
```

For a sendme/swarmie collection root, `/blake3/<z32>` automatically shows a
directory listing; see [Collections](#collections). Raw blobs continue to
stream directly from the same bare URL. Automatic collection detection is
limited to roots of at most 8 MiB; larger roots are served as raw blobs.

The same content is also served on a per-hash subdomain of `localhost`, which
browsers and curl resolve to the loopback address:

```text
http://<z32>.localhost:8080/
http://<z32>.localhost:8080/<dir>/<name>
```

Each hash then has its own browser origin, and root-relative links inside a
collection, such as `/style.css` in an HTML page, resolve within it.
Listings served this way link to `/<path>` instead of `/blake3/<z32>/<path>`.

`<z32>` is the canonical lowercase **z-base-32 encoding of the 32-byte BLAKE3
hash** (52 characters), not hex or RFC 4648 base32. In Rust:

```rust,ignore
let path = format!("/blake3/{}", z32::encode(hash.as_bytes()));
```

Only loopback HTTP listeners are accepted, including `127.0.0.1` and `::1`.
There is no TLS configuration. The iroh connection to the content peer remains
encrypted and authenticated.

## Discovery

The gateway computes `SHA-1(blake3_hash_bytes)`, queries Mainline for content
providers, and resolves the first available signed endpoint record through the
tracker. It connects to **one peer**, using normal iroh endpoint discovery,
and streams the blob using the iroh-blobs protocol. The provider must have
published its signed tracker record and announced the content infohash, as in
the workspace's `Publisher` and blobs example.

Tracker configuration uses the existing priority order:

1. `--tracker IP:PORT` (`IROH_ADDR_INDEX`) bypasses tracker discovery.
2. `--tracker-pubkey HEX` (`IROH_TRACKER_PUBKEY`) selects the signed BEP44 list.
3. `--rendezvous-hash HEX` (`IROH_TRACKER_INFOHASH`) selects the fallback hash;
   if omitted, the protocol's default rendezvous hash is used.

Use `--no-rendezvous` to disable the hash fallback. Public keys are 64 hex digits;
infohashes are 40 hex digits. `--dht-port` controls the local Mainline UDP port
(default: an available port).

## Streaming and HTTP

The gateway retrieves a bounded prefix for MIME detection and the final Bao
chunk to verify the size. It then requests the Bao chunks covering the desired
byte range and trims them to exact HTTP byte boundaries. Every returned data
chunk is verified against the requested BLAKE3 hash. No complete-file download
or persistent blob store is required. HTTP consumption drives the peer stream;
dropping the HTTP body drops the upstream request.

- `GET` returns `200` and streams the file.
- Single closed (`bytes=10-99`), open-ended (`bytes=10-`), and suffix
  (`bytes=-100`) ranges return `206`, `Content-Range`, and exact `Content-Length`.
- Unsatisfiable byte ranges return `416` with `Content-Range: bytes */SIZE`.
- Multiple ranges return streamed `multipart/byteranges` responses with a
  per-part MIME type and Content-Range, and an exact overall Content-Length.
  Unsatisfiable parts are skipped when other parts are satisfiable.
- Malformed, unknown-unit, and excessive range sets (more than 16 parts) are
  ignored, returning the full representation with `200`.
- `HEAD` returns full metadata without a response body and ignores `Range`.
- Responses include `Accept-Ranges: bytes`, a hash-derived ETag and immutable
  caching headers. `If-None-Match` and strong ETag `If-Range` are supported.
- MIME detection uses the verified prefix with the same `mime_classifier`
  library as the example gateway. Unknown binary data uses
  `application/octet-stream`; no filename extension is necessary.
- CORS permits GET/HEAD from browser pages and exposes range metadata.

## Collections

iroh-blobs collections are browsed under the root hash:

```text
http://127.0.0.1:8080/blake3/<z32>
http://127.0.0.1:8080/blake3/<z32>/<dir>/
http://127.0.0.1:8080/blake3/<z32>/<dir>/<name>
```

Collection names are treated as `/`-separated paths. A path that matches a
file name serves the file like a blob, with ranges, and a MIME type from the
file extension where known. Any other path is listed as a directory: an HTML
page with its subdirectories, its files, and a link to the parent. The bare
`/blake3/<z32>` shows the top level if the blob is detected as a collection;
`/blake3/<z32>/` always treats it as one.

Query flags:

- `?tree` on a root URL states that the blob is a collection. The gateway
  reads it directly, skipping the size probe and the detection limits, and
  returns `422` if it is not one.
- `?raw` on a file URL serves its source instead of a rendered page: text
  keeps its charset as `text/plain`, anything else becomes
  `application/octet-stream`. On a root URL it serves the underlying hash
  sequence instead of a listing, and takes precedence over `?tree`.
- `?download` saves the response instead of showing it, under the file's name
  in the collection, or under the hash for a bare blob. Listings link to it in
  a `Download` column.
- `?sizes` on a listing shows file sizes. The gateway fetches the last chunk
  of each listed file, which verifies its size, up to 16 at a time.

The gateway discovers the provider
using the **root hash** and fetches files from that same provider, so child
hashes do not need separate Mainline announcements. `/blake3/<z32>/` on a blob
that is not a collection returns `422`, and a path that is neither a file nor
a directory returns `404`.

Malformed hashes return `400`; no discovered provider returns `404`; failed
upstream operations return `502`; setup timeouts return `504`. Once HTTP headers
have been sent, transfer failures terminate the body rather than changing its
status. Discovery, connection establishment, and MIME/size probing share a
60-second budget; body reads have a 30-second inactivity timeout.

A bounded 128-entry cache reuses peer connections and MIME/size metadata for
successive video seeks. A second bounded cache retains collection manifests
and their provider connections. This version does not try alternate peers or
perform parallel downloads.

## Tests

```sh
cargo test -p iroh-local-gateway
```

The integration test uses a local Mainline testnet, a real tracker, an iroh-blobs
provider, and a TCP HTTP listener. It checks full streaming, video MIME detection,
byte-exact unaligned ranges, suffixes, multipart responses, HEAD, conditional requests, empty blobs,
invalid/missing content, and CORS without relying on public DHT or relay services.
