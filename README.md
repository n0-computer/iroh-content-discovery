# iroh-endpoint-tracker

Addr → [`EndpointId`][eid] directory. Join Mainline `get_peers` contacts to
iroh identities.

Content discovery stays on the DHT (`announce_peer` / `get_peers` on a SHA-1
of the BLAKE3). This service does **not** store content hashes. It answers:
given a compact `ip:port`, which [`EndpointId`][eid]s currently claim that
mapping.

A signed row is: this eid is listening on these `host:port`s using these
ALPNs, and which indexes a replica may build from that (eid→addr, reverse
addr→eid, ALPN peer lists). This directory is the reverse index and ignores
rows that do not permit it. Publishers must announce at least one ALPN; the
list is otherwise open. Replicas cannot forge a victim.
State size is O(live endpoints).

A signature does not prove the eid **owns** the socket. Confirm with a short
iroh connect to that port as the claimed eid, offering the announced ALPNs
([`confirm_socket`][probe]). Endpoints that do not already run a connectable
protocol can announce and accept `/iroh-addr-index/probe/0`
([`ProbeAccept`][probe-accept]).

Transport: one postcard UDP datagram ≤ 1200 bytes. By default, a replica
requires publish datagrams to originate from an IP listed in the record.

```sh
cargo run --bin iroh-endpoint-tracker -- serve --udp-bind 0.0.0.0:11223
cargo run --bin iroh-endpoint-tracker -- publish \
  --udp <tracker-ip:port> --dht-addr 203.0.113.9:6881 \
  --alpn /my-protocol/1 <blake3-or-infohash-hex>
cargo run --bin iroh-endpoint-tracker -- lookup --udp <tracker-ip:port> 203.0.113.9:6881
cargo run --example blobs
cargo run --example spoof
```

MIT or Apache-2.0, at your option.

[eid]: https://docs.rs/iroh/latest/iroh/struct.PublicKey.html
[probe]: https://docs.rs/iroh-endpoint-tracker/latest/iroh_endpoint_tracker/fn.confirm_socket.html
[probe-accept]: https://docs.rs/iroh-endpoint-tracker/latest/iroh_endpoint_tracker/struct.ProbeAccept.html
