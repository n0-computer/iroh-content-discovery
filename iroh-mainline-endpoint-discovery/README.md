# iroh-mainline-endpoint-discovery

Find iroh endpoints through the Mainline DHT. Mainline maps an infohash to
compact IPv4 `host:port` pairs; an [address index][udp-addr-index] maps each
`host:port` to a signed record containing an [`EndpointId`][eid]. Providers of a
blob, members of a gossip topic, or anything else you can name with an infohash
can find each other this way. For the why, see the blog post
[Iroh global content discovery][blog].

The crate provides three APIs:

- `AddrIndex` finds address index servers and talks to them over the caller's
  Mainline socket.
- `Publisher` stores our signed endpoint record and announces infohashes.
- `Resolver` turns an infohash into a stream of candidate `EndpointId`s.

It binds no socket and owns no DHT node: you pass in an [`n0-mainline`][mainline]
node, and index traffic shares its UDP socket.

## Records

The value stored in the index is an endpoint record signed with the endpoint's
secret key, so only the owner of the key can write one. A resolver checks the
signature, takes the endpoint ID, and dials it through iroh's normal address
lookup. The `host:port` in the DHT and the index is just a rendezvous key, never
an iroh address.

The record is also signed for the socket the server observed, and a reader
discards a record it finds under any other socket. Reads are public, so anyone
can copy a record. Without that binding they could store it in their own slot,
which the server accepts because they really do receive there, and every
resolver would then return that endpoint as a provider for content it does not
serve.

What you get back is still only a list of *candidates* that might serve the
content. Check them before trusting them, for example with an iroh-blobs size
request, as the gateway does.

## Resolving

`Resolver::resolve_stream` yields endpoint IDs as peer batches and index lookups
complete, translating up to 16 peers at a time so one slow lookup cannot hold up
the rest. An iroh-blobs downloader can start on the first provider while
discovery continues. The stream is lazy: the Mainline lookup starts only when
the first item is requested, so providers you already know can be chained in
front of it. `Resolver::resolve_continuously` starts a new lookup whenever the
consumer asks for more, and may yield an endpoint again.
`Resolver::resolve` collects a sorted list.

Mainline servers using n0-mainline 0.8 keep at most one peer per IP address for
each infohash. Publishers behind the same public IPv4 address can replace each
other's announcements even when they use different ports. Discovery therefore
does not guarantee finding every publisher behind a shared NAT.

## Publishing

A `Publisher` keeps its index record and its announcements on separate
schedules. Each infohash is announced every ten minutes, with jitter. The record
is republished only while there are infohashes to announce: when it is older
than thirty minutes (servers keep it for an hour), when discovery finds other
servers, and when Mainline reports a different public address for us. It goes
to all servers and counts once one stored it, since readers ask them all; a
failure is retried after thirty seconds. Many infohashes share one record, so
announcing them costs one index publication.

An announcement goes ahead only if a server holds our record for the address
Mainline sees, since readers could not resolve it otherwise. After an outage of
thirty seconds or more, announcements resume after a random delay of up to a
minute, so they do not all restart at once.

## Finding servers

```rust,ignore
let index = AddrIndex::builder(dht)
    .list_key(public_key)             // trusted Pkarr list, tried first
    .rendezvous_hash(rendezvous_hash) // untrusted fallback
    // .server("203.0.113.1:60125".parse()?) bypasses discovery
    .build()
    .await?;
```

Explicit servers are used as given and skip discovery. Otherwise the trusted
Pkarr list is tried first, and the rendezvous hash only when the list yields
nothing. Each lookup has a thirty-second deadline. Discovery repeats in the
background every ten minutes; a failed refresh keeps the previous servers and
retries after thirty seconds, so lookups never wait for it. At most two servers
are kept.

The builder starts empty. `n0_defaults()` uses the list maintained by n0,
`DEFAULT_INDEX_LIST_KEY` (`z6rb8uoy1pwuckhw8qx8i4qseczujxw4qakpe7xng3yi68wpyrqo`),
and caches lookups for `DEFAULT_LOOKUP_CACHE_TTL` (five minutes). Sockets
without a record are remembered for at most thirty seconds, failed lookups not
at all, and concurrent lookups of one socket share a request. With no server,
key, or hash, `build` returns `NoServers`. `AddrIndex::udp(dht, server)` and
`AddrIndex::discover(dht)` are shorthands for one explicit server and for
`builder(dht).n0_defaults()`. Set `IROH_ADDR_INDEX=ip:port` to pin one server in
the examples and the gateway.

Rendezvous candidates come from `get_peers` on a hash servers announce under, by
default `b86c3d910e1a67ec9ba8a69a95bd7f8b08be923b`. They are untrusted and may be
unreachable or dishonest; the records they serve are validated either way. A
signed result is never topped up with rendezvous candidates, and even a signed
address only means the list's owner vouches for that server, not that it
answered.

### Curated server list (Pkarr)

The list's owner publishes it with `ServerList`:

```rust,ignore
let list = ServerList::new(vec!["203.0.113.1:6881".parse()?])?;
let item = list.sign(&signing_key, sequence)?;
dht.put_mutable(item, None).await?;
```

Clients need only the public key. The value is a standard Pkarr DNS packet in an
**unsalted** BEP 44 item, with one IPv4 socket per apex `IN TXT` record. Only the
apex matching the signer's z-base-32 key is read. Malformed sockets or more than
two entries invalidate the list. An empty apex TXT record (`@ 300 IN TXT ""`), or
none at all, withdraws the list and allows the fallback. Sequences are Unix
timestamps in microseconds. An `AddrIndex` rejects lists older than the newest it
has seen, but that memory does not survive a restart, and lists carry no expiry.

You can also publish the list with **iroh-share**: create a standalone name,
open its advanced DNS record editor, replace its records with lines like the
ones below, and keep the daemon running to renew it.

```dns
@ 300 IN TXT "203.0.113.1:60125"
@ 300 IN TXT "198.51.100.2:33445"
```

Or run the standalone republisher, separately from the servers it names:

```sh
# IROH_INDEX_LIST_SECRET is a 64-hex-digit Ed25519 secret seed.
cargo run -p iroh-mainline-endpoint-discovery --features cli \
  --bin iroh-index-list -- \
  --server 203.0.113.1:6881 203.0.113.2:6881
```

It takes `IROH_INDEX_LIST_SECRET` out of its environment before any thread
starts, signs the list once, zeroizes the secret, and logs the public key for
your clients. It republishes every ten minutes and retries transient errors
after thirty seconds. To change the list, restart with new addresses; omit
`--server` to publish an empty one. An explicit `--sequence` must exceed the
previous one. Embedded applications can run
`republish_server_list(dht, signed_item)` as a task of their own.

The command sits behind the `cli` feature, which is off by default, so the
library never pulls in `clap` or `tracing-subscriber`.

## Examples

```sh
# Publish and resolve a blob in one process, through Mainline and the index.
cargo run -p iroh-mainline-endpoint-discovery --example blobs
# Show that another socket cannot replace a publisher's record.
cargo run -p iroh-mainline-endpoint-discovery --example spoof
# Join an iroh-gossip topic by name, without a ticket.
cargo run -p iroh-mainline-endpoint-discovery --example gossip -- my-topic
# Share a directory and publish a Pkarr name for it.
cargo run -p iroh-mainline-endpoint-discovery --example provide -- ./site
# Publish a named blob, then resolve and download it from a separate client.
cargo run -p iroh-mainline-endpoint-discovery --example pkarr-publish-resolve -- --once
```

In the `gossip` example the topic string is hashed into a gossip topic id and an
infohash. Every member announces itself under that infohash, and members that
find each other join the same topic. Anyone who knows the string can join.

`provide` adds a file or directory as blobs plus a collection, announces every
hash, and publishes a Pkarr name for the collection. It reads `PKARR_SECRET`
(64 hex digits) to keep the same name across runs; `--no-pkarr` publishes hashes
only.

`pkarr-publish-resolve` generates a keypair, serves a blob, publishes its
endpoint record, content announcement and Pkarr name, then resolves the name
from a separate DHT client and downloads and verifies the blob. Use
`--key-file ./site.key` to keep the name and `--data '...'` to change the
content. Without `--once` it keeps serving and republishing until Ctrl-C.

All examples need UDP access to Mainline and a working address index. Set
`IROH_ADDR_INDEX=ip:port` to use a particular one.

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

[blog]: https://www.iroh.computer/blog/iroh-global-content-discovery
[eid]: https://docs.rs/iroh/latest/iroh/struct.PublicKey.html
[mainline]: https://docs.rs/n0-mainline
[udp-addr-index]: https://docs.rs/udp-addr-index
