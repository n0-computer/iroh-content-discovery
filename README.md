# iroh Mainline endpoint discovery

Discover iroh endpoints through Mainline. Mainline maps an application-defined
infohash to a compact IPv4 socket; the address index maps that socket to opaque
bytes containing a signed [`EndpointId`][eid]. This works for blobs, gossip
peers, and other iroh protocols.

The address index itself is generic:

```text
SocketAddrV4 → opaque bytes
```

A publisher first asks a replica for a short-lived token. The replica returns
the packet's observed public IPv4 socket and a stateless MAC bound to that
socket. A put carrying the token must arrive from the same socket; the replica
then derives the map key from the packet source and stores the bytes using its
own receipt time and TTL. Reads are direct and public; requests are padded to
1200 bytes and shorter requests are dropped to prevent response amplification.
Responses are bounded to one non-fragmented UDP datagram. The replica neither parses nor
validates the value.

The index exchange is sent and received through the same UDP socket owned by
the caller's Mainline node. Mainline announcements use their implied source
port, so the compact peer address and the address-index key describe the same
UDP mapping. `iroh-mainline-endpoint-discovery` does not bind another socket or
own another DHT node. In particular, a publisher sharing a CGNAT public IP
cannot claim another publisher's port: neither the Mainline announcement nor
the index write accepts a caller-supplied port.

Address-index datagrams start with `00 61 64 64 72 69 64 78`
(`\0addridx`). The zero byte cannot begin a Mainline KRPC message, whose outer
value must be a bencoded dictionary beginning with `d`.

The iroh discovery layer stores a signed endpoint record in those opaque bytes.
A resolver validates the signature, extracts the endpoint ID, and dials it
normally using iroh's regular discovery mechanisms. The DHT/index `host:port`
is only a rendezvous key and is never used as an iroh address.
`Resolver::resolve_stream` yields endpoint IDs as peer batches and index
lookups complete. It translates up to 16 peers concurrently, so a slow index
lookup does not block another result. An iroh-blobs downloader can try providers while discovery
continues. `Resolver::resolve_continuously` starts another lookup when the
consumer asks for more. It can yield the same endpoint again. `Resolver::resolve` still
collects a sorted list when needed.

This requires direct UDP access to the replica. Mainline has the same direct
UDP requirement, so nodes that can use this discovery mechanism already meet
that constraint.

The repository contains four Rust workspace crates and a browser extension:

- `udp-address-records-proto`: versioned token, put, and get UDP messages; no iroh
  dependency
- `udp-address-records`: embeddable replica server and the `udp-address-records` binary
- `iroh-mainline-endpoint-discovery`: reusable `Directory`, `Publisher`, and
  `Resolver` APIs; the publisher takes an endpoint secret key and an externally
  managed Mainline DHT node
- `iroh-local-gateway`: localhost HTTP streaming, MIME detection, and byte ranges
- `blake3-link-extension`: Chrome/Brave redirects from hash subdomains to the gateway

```sh
cargo run -p udp-address-records -- --udp-port 11223 \
  --rendezvous-hash b86c3d910e1a67ec9ba8a69a95bd7f8b08be923b
cargo run -p iroh-mainline-endpoint-discovery --example blobs
cargo run -p iroh-mainline-endpoint-discovery --example spoof
```

To expose index metrics for Prometheus, pass `--metrics-listen 127.0.0.1:9090`
to the `udp-address-records` command and scrape `http://127.0.0.1:9090/metrics`.
The listener is disabled unless requested. Bind it to a trusted interface;
the endpoint has no authentication.

MIT or Apache-2.0, at your option.

[eid]: https://docs.rs/iroh/latest/iroh/struct.PublicKey.html

## Finding replicas

Replicas share a Mainline node's UDP socket and can announce under the rendezvous
infohash `b86c3d910e1a67ec9ba8a69a95bd7f8b08be923b` (SHA-1 of
`iroh-addr-index replicas v1`). Announcements use the implied source port and
renew every ten minutes; failures retry after thirty seconds.

`Server::attach(dht)` serves and announces until its returned handle is dropped.
`Server::attach_with_rendezvous(dht, Some(hash))` selects a hash, while `None`
serves without announcing. The CLI makes no announcement unless
`--rendezvous-hash HEX` is supplied; pass the hash above for default rendezvous
discovery, or omit the flag when using an explicit address or signed list.
The CLI runs a Mainline server on all IPv4 interfaces at `--udp-port` (11223 by
default). Allow inbound UDP on that port. Mainline currently only supports
choosing the bind port, not a specific local interface address.

`Directory::discover(dht)` finds replicas with `get_peers` and uses that same DHT
socket for index requests. It refreshes on use after ten minutes, caps the list
at two candidates, and gives discovery thirty seconds. No replicas is an error;
announcements are untrusted and can include unavailable or dishonest servers.
Signed endpoint records are still validated by the discovery layer.

The examples discover replicas by default. Set `IROH_ADDR_INDEX=ip:port` to use
an explicit replica instead, or use `Directory::udp(dht, replica)` in code.
Mainline bootstrap nodes are still needed; tracker addresses are not hardcoded.
The `udp-address-records` binary exits if its service or announcement task stops,
and shuts down on Ctrl-C or SIGTERM.

### Signed bootstrap list (BEP44)

Applications can configure an explicit tracker and both discovery sources independently. The trusted BEP44 key
is tried first; the rendezvous hash is queried only when no signed addresses
are available. Each lookup has a thirty-second deadline.

```rust,ignore
let directory = Directory::discover_with_config(dht, DiscoveryConfig {
    tracker: None, // Some("203.0.113.1:6881".parse()?) bypasses discovery
    public_key: Some(public_key),
    rendezvous_hash: Some(rendezvous_hash),
}).await?;
```

Each field is optional. An explicit `tracker` takes precedence over the public
key and hash and bypasses discovery. Either discovery field can be `None` to
disable that source. `Directory::discover` uses
the default rendezvous hash without a key. `Directory::discover_with_authority`
uses the supplied key with the default hash as fallback. A replica can announce
under a custom hash with `Server::attach_with_rendezvous(dht, Some(hash))`.

Discovery selects at most **two trackers**, and a signed list can contain one
or two addresses. A signed result is never padded with rendezvous candidates.
Rendezvous candidates remain untrusted; a signed address does not guarantee
availability. Fallback currently means no signed addresses were found, not
that a listed tracker failed an application request.

The authority publishes a list using the library's `TrackerList` helper:

```rust,ignore
let list = TrackerList::new(vec!["203.0.113.1:6881".parse()?])?;
let item = list.sign(&signing_key, sequence)?;
dht.put_mutable(item, None).await?;
```

Keep the signing key with the list operator; clients need only its public key.
Increase the nonnegative sequence number whenever the list changes and
periodically republish the signed item to keep it available in the DHT.
The fixed legacy salt is `iroh-addr-index replicas v1` (preserved across the
crate rename for compatibility). The value is version byte `1`
followed by up to two compact IPv4 sockets (four IP bytes, two big-endian port
bytes). An empty list withdraws all signed candidates and permits fallback.

`n0-mainline` verifies BEP44 signatures. A directory retains the highest observed
valid list across refreshes, including when a lookup times out, and rejects
lower sequences for its lifetime. This cache is not persisted across restarts;
a fresh client can still receive an older signed list. Lists have no additional
wall-clock expiry.

### Running the BEP44 republisher

Run the list publisher separately from the individual tracker servers:

```sh
# Set IROH_TRACKER_LIST_SECRET to a 64-hex-digit Ed25519 secret seed.
cargo run -p iroh-mainline-endpoint-discovery --bin iroh-tracker-list -- \
  --sequence 1 --tracker 203.0.113.1:6881 203.0.113.2:6881
```

The process consumes and removes `IROH_TRACKER_LIST_SECRET` before starting
runtime threads, signs the list once, and zeroizes its owned secret buffers and
signing key. It logs the public key for configuring clients. Only the signed
record is retained by the renewal task. This does not remove the variable from
the launching shell or service configuration.

Publication starts immediately and repeats every ten minutes. Transient errors
retry after thirty seconds; publication attempts time out after thirty seconds.
A sequence conflict or DHT shutdown stops the task. To change the list, restart
with the new addresses and a higher sequence. Omit `--tracker` to publish an
empty list. Ctrl-C stops renewal.

Embedded applications can run `republish_tracker_list(dht, signed_item)` as a
separate async task; dropping that future stops renewal without stopping the DHT.

## Local HTTP content gateway

The fourth workspace project, [`iroh-local-gateway`](iroh-local-gateway/README.md),
serves `http://127.0.0.1:8080/blake3/<z32>`. It discovers one content peer through
Mainline and the tracker, then streams Bao-verified bytes with MIME detection
and HTTP range support for video seeking. Collection roots automatically show
a directory listing at `/blake3/<z32>`, and `/blake3/<z32>/path/to/file`
streams a file from the same provider. The same content is served on
`http://<z32>.localhost:8080/`, giving each hash its own browser origin.

```sh
cargo run -p iroh-local-gateway -- --tracker 127.0.0.1:11223
```

The gateway also accepts the BEP44 public key and rendezvous hash discovery
options. See its README for configuration and HTTP behavior.

## Browser extension

The fifth project, [`blake3-link-extension`](blake3-link-extension/README.md),
rewrites `https://<z32>.blake3.link/<path>` to
`http://<z32>.localhost:<port>/<path>` in Chrome and Brave. Load that directory
unpacked from the browser's extensions page with Developer mode enabled. The
popup configures the local gateway port (default 8080) and enables/disables
rewrites. The apex `blake3.link` site is unaffected.
