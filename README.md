# iroh Mainline endpoint discovery

Discover iroh endpoints through Mainline. Mainline maps an application-defined
infohash to a compact IPv4 socket; the address index maps that socket to a
signed [`EndpointId`][eid]. This works for blobs, gossip peers, and other iroh
protocols.

The address index does **not** store content or peer hashes. It answers: given
a compact `ip:port`, which [`EndpointId`][eid] currently claims that mapping?

A signed row says that an endpoint listens on these `host:port`s using these
ALPNs, and identifies the indexes a replica may build from it. The service is
the reverse `addr → EndpointId` index and ignores rows that do not permit that
index. Publishers must announce at least one ALPN. Replicas cannot forge a
victim's signed row, and state size is O(live endpoints).

A signature does not prove the endpoint **owns** the socket. Confirm with a
short iroh connection to that port as the claimed endpoint, offering the
announced ALPNs ([`confirm_socket`][probe]). Endpoints that do not already run a
connectable protocol can announce and accept `/iroh-addr-index/probe/0`
([`ProbeAccept`][probe-accept]).

Transport is one postcard UDP datagram ≤ 1200 bytes. By default, a replica
requires publish datagrams to originate from an IP listed in the record.

The repository is a workspace containing:

- `iroh-addr-index-proto`: signed records and versioned UDP messages
- `iroh-addr-index`: embeddable replica server and the `iroh-addr-index` binary
- `iroh-mainline-endpoint-discovery`: reusable `Directory`, `Publisher`, and
  `Resolver` APIs that take externally managed iroh endpoints and Mainline DHT
  nodes

```sh
cargo run -p iroh-addr-index -- serve --udp-bind 0.0.0.0:11223
cargo run -p iroh-addr-index -- publish \
  --udp <replica-ip:port> --dht-addr 203.0.113.9:6881 \
  --alpn /my-protocol/1 <blake3-or-infohash-hex>
cargo run -p iroh-addr-index -- lookup \
  --udp <replica-ip:port> 203.0.113.9:6881
cargo run -p iroh-mainline-endpoint-discovery --example blobs
cargo run -p iroh-mainline-endpoint-discovery --example spoof
```

MIT or Apache-2.0, at your option.

[eid]: https://docs.rs/iroh/latest/iroh/struct.PublicKey.html
[probe]: https://docs.rs/iroh-addr-index/latest/iroh_addr_index/fn.confirm_socket.html
[probe-accept]: https://docs.rs/iroh-addr-index/latest/iroh_addr_index/struct.ProbeAccept.html
