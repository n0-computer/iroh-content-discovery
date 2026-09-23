# iroh-local-gateway

A localhost HTTP gateway for content-addressed files, including video. This is
workspace project four, adapted from the streaming approach in
[`iroh-examples/iroh-gateway`](https://github.com/n0-computer/iroh-examples/tree/main/iroh-gateway).

## Try the full workflow

With the browser extension installed and enabled on port 8080:

```sh
cargo run -p iroh-local-gateway --example demo -- /path/to/video.mp4
```

Open the printed `https://<public-key>.pkarr.link/` URL and leave the command
running. By default this uses the **public Mainline DHT**, discovers public
address-index trackers, and starts the file provider, Pkarr publisher, and HTTP
gateway in one process. The provider and gateway use normal iroh discovery and
relays. The full browser flow is:

```text
https://<public-key>.pkarr.link/
  -> http://127.0.0.1:8080/pkarr/<public-key>/
  -> https://<hash>.blake3.link/
  -> http://127.0.0.1:8080/blake3/<hash>
```

The gateway resolves the signed HTTPS record on Mainline and redirects;
the extension intercepts both domains. The content then travels through real
iroh connections and tracker discovery. Public mode needs working outbound UDP
and an available address-index tracker. Pass `--tracker IP:PORT` (or set
`IROH_ADDR_INDEX`) to select one explicitly if discovery fails. Startup can
take a minute. The Pkarr packet is republished every ten minutes, and the blob
provider must stay running. Files are imported into a
temporary disk-backed store cleaned up on normal shutdown. The example may use
additional disk space roughly equal to the file size.

Omit the file argument for a quick text greeting. Use `--port 8081` if needed,
and set the same port in the extension. The printed localhost blob URL also
works without the extension; following the Pkarr route's redirect needs the
extension to intercept `blake3.link`. Stop any other gateway on that port first.
Ctrl-C stops the demo.

For an isolated run without public DHT or relay services, opt in explicitly:

```sh
cargo run -p iroh-local-gateway --example demo -- --local-testnet
```

This starts a local DHT and tracker and uses in-memory iroh address discovery.
Links from this mode work only through this demo's gateway.

## Standalone gateway

```sh
cargo run -p iroh-local-gateway -- --listen 127.0.0.1:8080 --tracker 127.0.0.1:11223
```

Open:

```text
http://127.0.0.1:8080/blake3/<z32>
```

For a sendme/swarmie collection root, `/blake3/<z32>` automatically shows a
directory index. Fetch a named file at `/blake3/<z32>/path/to/file`.
The gateway discovers the provider using the **root hash**, reads the
collection, and streams the selected child from that same provider. Child
hashes do not need separate Mainline announcements. Raw blobs continue to
stream directly from the same bare URL. Automatic collection detection is
limited to roots of at most 8 MiB; larger roots are served as raw blobs.

`<z32>` is the canonical lowercase **z-base-32 encoding of the 32-byte BLAKE3
hash** (52 characters), not hex or RFC 4648 base32. In Rust:

```rust,ignore
let path = format!("/blake3/{}", z32::encode(hash.as_bytes()));
```

Only loopback HTTP listeners are accepted, including `127.0.0.1` and `::1`.
There is no TLS configuration. The iroh connection to the content peer remains
encrypted and authenticated.

## Pkarr redirects

Publish a test redirect and keep it alive with the included example:

```sh
cargo run -p iroh-local-gateway --example pkarr-publish -- example.com
```

Leave it running alongside your gateway. After publication succeeds, open the
printed `https://<public-key>.pkarr.link/` URL with the extension enabled, or
use the printed localhost URL directly. This publishes to the public Mainline
DHT; the target is a hostname, without `https://` or a path. The example does
not start the gateway. For a content-addressed target, replace `example.com`
with `<hash>.blake3.link` and keep the content provider running too.

By default each run generates a temporary identity. To reuse a public key:

```sh
cargo run -p iroh-local-gateway --example pkarr-publish -- example.com --key-file /tmp/pkarr-test.key
```

The example creates the file if missing (mode `0600` on Unix), or reads its
32-byte secret key. Restart with the same key file and a different hostname to
update the destination. Stop the old publisher before changing the target.
It republishes every ten minutes, retries transient failures after thirty
seconds, and exits on Ctrl-C or a sequence conflict. Stopping does not delete
the record immediately; the DHT eventually expires it without republication.

`/pkarr/<public-key>` and `/pkarr/<public-key>/path?query` resolve a Pkarr
public key (canonical lowercase z-base-32) through the same Mainline node.
On a cache miss, the gateway uses the first verified BEP44 item returned by
Mainline; it does not wait for the full lookup to finish. `n0-mainline` verifies
the signature, and `simple-dns` decodes the value's apex `HTTPS` records; no
`pkarr` client or additional DHT implementation is used. It selects the supported
target with the lowest priority and redirects to `https://<target>/path?query`.
Paths and queries retain their original percent encoding; the bare key uses `/`.
Service-mode port parameters are supported. Targets must be conventional DNS
hostnames; root targets, bare public keys, and records requiring mandatory SVCB
parameters or no-default-alpn are not supported. A/AAAA records alone do not
define a redirect target.

Responses use `307 Temporary Redirect` and `Cache-Control: no-store`, so a new
signed record can change the destination. The gateway caches successful signed packets
in memory by public key (up to 1024 entries), for the minimum answer TTL capped at
30 seconds. Cache hits do not extend expiry; zero-TTL records and errors are not
cached. Paths and queries are applied separately on each request. Cache misses
perform a DHT lookup with a 60-second timeout. The first response can be an older
signed version; this favors latency over searching for the newest available version. Invalid keys return `400`, missing packets `404`,
packets without a supported HTTPS target `422`, invalid packets or failed
lookups `502`, and lookup timeouts `504`.

All target hostnames are treated alike, including `<hash>.blake3.link`. The
browser extension can intercept that destination using its existing rules.
This route can be used directly on localhost; the extension also routes
`https://<public-key>.pkarr.link/path?query` here automatically.

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

Blobs are served under `/blake3/<z32>`. iroh-blobs collections can be browsed
under `/tree/<z32>`:

```text
http://127.0.0.1:8080/tree/<z32>
http://127.0.0.1:8080/tree/<z32>/<dir>/
http://127.0.0.1:8080/tree/<z32>/<dir>/<name>
```

Collection names are treated as `/`-separated paths. A path that matches a
file name serves the file like `/blake3/`, with ranges and MIME detection. Any
other path is listed as a directory: an HTML page with its subdirectories,
its files, and a link to the parent. Add `?sizes` to also show file
sizes; the gateway then fetches the last chunk of each listed file, which
verifies its size, up to 16 at a time. Files are fetched from the peer that
provided the collection, so only the collection hash needs to be announced.
`/tree/` on a blob that is not a collection returns `422`, and a path that is
neither a file nor a directory returns `404`.

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
