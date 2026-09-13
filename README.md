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

This requires direct UDP access to the replica. Mainline has the same direct
UDP requirement, so nodes that can use this discovery mechanism already meet
that constraint.

The repository is a workspace containing:

- `iroh-addr-index-proto`: versioned token, put, and get UDP messages; no iroh
  dependency
- `iroh-addr-index`: embeddable replica server and the `iroh-addr-index` binary
- `iroh-mainline-endpoint-discovery`: reusable `Directory`, `Publisher`, and
  `Resolver` APIs; the publisher takes an endpoint secret key and an externally
  managed Mainline DHT node

```sh
cargo run -p iroh-addr-index -- --udp-bind 0.0.0.0:11223
IROH_ADDR_INDEX=<public-replica-ip:port> \
  cargo run -p iroh-mainline-endpoint-discovery --example blobs
IROH_ADDR_INDEX=<public-replica-ip:port> \
  cargo run -p iroh-mainline-endpoint-discovery --example spoof
```

MIT or Apache-2.0, at your option.

[eid]: https://docs.rs/iroh/latest/iroh/struct.PublicKey.html
