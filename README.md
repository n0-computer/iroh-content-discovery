# iroh Mainline endpoint discovery

Discover iroh endpoints through Mainline. Mainline maps an application-defined
infohash to a compact IPv4 socket; the address index maps that socket to opaque
bytes containing a signed [`EndpointId`][eid]. This works for blobs, gossip
peers, and other iroh protocols.

The address index itself is generic:

```text
SocketAddrV4 → opaque bytes
```

A publisher first asks a server for a short-lived token. The server returns the
packet's observed public IPv4 socket and a stateless MAC bound to that socket. A
put carrying the token must arrive from the same socket; the server then derives
the map key from the packet source and stores the bytes under its own receipt
time and TTL. Reads are direct and public. Requests are padded to 1200 bytes and
shorter ones are dropped, so a response can never be larger than the request
that caused it, and every response fits one unfragmented datagram. The server
neither parses nor validates the value it stores.

Index traffic shares the UDP socket of the caller's Mainline node. Mainline
announcements use their implied source port, so the compact peer address and the
index key describe the same UDP mapping. `iroh-mainline-endpoint-discovery` binds
no socket of its own and owns no DHT node. A publisher behind a shared CGNAT
address therefore cannot claim another publisher's port, because neither the
announcement nor the index write accepts a caller-supplied one.

Index datagrams start with `00 61 64 64 72 69 64 78` (`\0addridx`). The leading
zero byte cannot begin a Mainline KRPC message, whose outer value is a bencoded
dictionary starting with `d`, so both protocols can share a socket.

The iroh discovery layer stores a signed endpoint record in those opaque bytes.
A resolver checks the signature, takes the endpoint ID, and dials it through
iroh's normal discovery. The `host:port` in the DHT and the index is a
rendezvous key, never an iroh address.

`Resolver::resolve_stream` yields endpoint IDs as peer batches and index lookups
complete, translating up to 16 peers at a time so one slow lookup cannot hold up
the rest. An iroh-blobs downloader can start on the first provider while
discovery continues. `Resolver::resolve_continuously` begins a new lookup
whenever the consumer asks for more, and may yield an endpoint it has yielded
before. `Resolver::resolve` collects a sorted list when a caller wants one.

All of this needs direct UDP access to a server. Mainline needs the same, so a
node that can use Mainline at all already meets the requirement.

The repository contains four Rust workspace crates and a browser extension:

- `udp-addr-index-proto`: versioned token, put, and get UDP messages; no iroh
  dependency
- `udp-addr-index`: an embeddable address index server, and the `udp-addr-index` binary
- `iroh-mainline-endpoint-discovery`: the `AddrIndex`, `Publisher` and
  `Resolver` APIs; the publisher takes an endpoint secret key and a Mainline
  node that the caller owns
- `iroh-local-gateway`: localhost HTTP streaming, MIME detection, and byte ranges
- `iroh-link-extension`: redirects hash and key subdomains to the gateway in
  Chrome, Brave and Firefox

```sh
cargo run -p udp-addr-index -- --dht-port 11223 \
  --rendezvous-hash b86c3d910e1a67ec9ba8a69a95bd7f8b08be923b
cargo run -p iroh-mainline-endpoint-discovery --example blobs
cargo run -p iroh-mainline-endpoint-discovery --example spoof
```

To expose index metrics for Prometheus, pass `--metrics-listen 127.0.0.1:9090`
to the `udp-addr-index` command and scrape `http://127.0.0.1:9090/metrics`.
The listener is disabled unless requested. Bind it to a trusted interface;
the endpoint has no authentication.

MIT or Apache-2.0, at your option.

[eid]: https://docs.rs/iroh/latest/iroh/struct.PublicKey.html

## Finding servers

Servers share a Mainline node's UDP socket and can announce themselves under the
rendezvous infohash `b86c3d910e1a67ec9ba8a69a95bd7f8b08be923b`, which is the
SHA-1 of `iroh-addr-index servers v1`. An announcement uses the implied source
port, renews every ten minutes, and retries after thirty seconds on failure.

`Server::attach(dht)` serves and announces until the returned handle is dropped.
`Server::attach_with_rendezvous(dht, Some(hash))` picks a different hash, and
`None` serves without announcing at all. The CLI announces only when you pass
`--rendezvous-hash HEX`: use the hash above for default discovery, and omit the
flag when clients reach you through an explicit address or a signed list. The
CLI binds all IPv4 interfaces at `--dht-port`, 11223 by default, so allow
inbound UDP there. Mainline can choose the port but not the interface.

`AddrIndex::discover(dht)` finds servers with `get_peers` and then uses the same
DHT socket for index requests. It refreshes on use after ten minutes, keeps at
most two candidates, and gives discovery thirty seconds. Finding none is an
error. Announcements are untrusted, so the candidates may be unreachable or
dishonest; the signed endpoint records they serve are validated either way.

The examples discover servers by default. Set `IROH_ADDR_INDEX=ip:port` to pin
one instead, or call `AddrIndex::udp(dht, server)` in code. Mainline bootstrap
nodes are still required, since no server address is hardcoded. The
`udp-addr-index` binary exits if its service or announcement task stops, and
shuts down on Ctrl-C or SIGTERM.

### Signed bootstrap list (BEP44)

An explicit server and the two discovery sources are configured independently.
The trusted BEP44 key is tried first, and the rendezvous hash only when the
signed list yields nothing. Each lookup has a thirty-second deadline.

```rust,ignore
let index = AddrIndex::discover_with_config(dht, DiscoveryConfig {
    server: None, // Some("203.0.113.1:6881".parse()?) bypasses discovery
    public_key: Some(public_key),
    rendezvous_hash: Some(rendezvous_hash),
}).await?;
```

Each field is optional. An explicit `server` takes precedence over the key and
the hash, and skips discovery entirely. Either discovery field can be `None` to
turn that source off. `AddrIndex::discover` uses the default rendezvous hash
with no key, and `AddrIndex::discover_with_authority` uses a key with that hash
as fallback. A server announces under a custom hash with
`Server::attach_with_rendezvous(dht, Some(hash))`.

Discovery keeps at most two servers, and a signed list holds one or two
addresses. A signed result is never topped up with rendezvous candidates.
Rendezvous candidates stay untrusted, and even a signed address only means the
authority vouches for that server, not that it answered. Falling back means no
signed address was found, not that a listed server failed a request.

The authority publishes a list with the `ServerList` helper:

```rust,ignore
let list = ServerList::new(vec!["203.0.113.1:6881".parse()?])?;
let item = list.sign(&signing_key, sequence)?;
dht.put_mutable(item, None).await?;
```

The signing key stays with the authority; clients need only the public key.
Raise the nonnegative sequence number whenever the list changes, and republish
the signed item periodically so the DHT keeps it. The salt is
`iroh-addr-index servers v1`. The value is a version byte `1` followed by up to
two compact IPv4 sockets, four address bytes and a big-endian port each. An
empty list withdraws every signed candidate and allows the fallback.

`n0-mainline` verifies BEP44 signatures. An `AddrIndex` keeps the highest valid
list it has seen across refreshes, including when a lookup times out, and
rejects lower sequences for its lifetime. That memory does not survive a
restart, so a fresh client can still be handed an older signed list. Lists carry
no wall-clock expiry.

### Running the BEP44 republisher

Run the list publisher separately from the servers it names:

```sh
# Set IROH_INDEX_LIST_SECRET to a 64-hex-digit Ed25519 secret seed.
cargo run -p iroh-mainline-endpoint-discovery --bin iroh-index-list -- \
  --sequence 1 --server 203.0.113.1:6881 203.0.113.2:6881
```

The process consumes and removes `IROH_INDEX_LIST_SECRET` before any runtime
thread starts, signs the list once, and zeroizes the secret buffers and the
signing key. It logs the public key so you can configure clients, and the
renewal task keeps only the signed item. Removing the variable here does not
remove it from the shell or service configuration that launched the process.

Publication starts immediately and repeats every ten minutes. A transient error
retries after thirty seconds, and each attempt times out after thirty. A
sequence conflict or a DHT shutdown stops the task. To change the list, restart
with new addresses and a higher sequence. Omit `--server` to publish an empty
list. Ctrl-C stops renewal.

Embedded applications can run `republish_server_list(dht, signed_item)` as a
task of their own; dropping that future stops renewal without stopping the DHT.

## Local HTTP content gateway

The fourth workspace project, [`iroh-local-gateway`](iroh-local-gateway/README.md),
serves `http://127.0.0.1:8080/blake3/<z32>`. It finds a provider through Mainline
and the address index, then streams Bao-verified bytes with MIME detection
and HTTP range support for video seeking. Collection roots automatically show
a directory listing at `/blake3/<z32>`, and `/blake3/<z32>/path/to/file`
streams a file from the same provider. The same content is served on
`http://<z32>.blake3.localhost:8080/`, giving each hash its own browser
origin, and Pkarr keys resolve at `/pkarr/<key>` and
`http://<key>.pkarr.localhost:8080/`.

Query flags:

- `?tree` serves a known collection without detecting it first.
- `?download` saves a file under its collection name, or a root as its raw
  bytes.
- `?sizes` adds file sizes to a listing.

```sh
cargo run -p iroh-local-gateway -- --index-server 127.0.0.1:11223
```

To serve content, the `provide` example adds a file or directory as blobs plus
a collection, announces every hash, and publishes a Pkarr name for the
collection, printing both link URLs:

```sh
cargo run -p iroh-mainline-endpoint-discovery --example provide -- ./site
```

It reads `PKARR_SECRET` (64 hex digits) to keep the same name across runs, and
prints a generated one if unset. Use `--no-pkarr` to publish hashes only.

The gateway also accepts the BEP44 public key and rendezvous hash discovery
options. See its README for configuration and HTTP behavior.

## Browser extension

The fifth project, [`iroh-link-extension`](iroh-link-extension/README.md),
rewrites `https://<z32>.blake3.net/<path>` to
`http://<z32>.blake3.localhost:<port>/<path>`, and
`https://<key>.pkarr.net/<path>` to `http://<key>.pkarr.localhost:<port>/<path>`,
in Chrome, Brave and Firefox. Load that directory
unpacked from the browser's extensions page with Developer mode enabled. The
popup configures the local gateway port (default 8080) and enables/disables
rewrites. The apex `blake3.net` site is unaffected.
