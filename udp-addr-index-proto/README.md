# udp-addr-index-proto

Wire types and encoding for a small UDP address index: `SocketAddrV4 → opaque
bytes`. No iroh dependency; the application decides what the bytes mean.

## Protocol

Every packet starts with the eight bytes `\0addridx`, followed by a postcard
encoded `Request::V1` or `Response::V1`. The leading zero keeps it distinct from
Mainline KRPC traffic, so both can share a UDP socket. Packets fit in 1200 bytes;
values can be up to 1024 bytes. Every request carries a client-chosen `u64` `tx`,
which the reply echoes so concurrent requests can be matched up.

To write, do a quick handshake:

1. Send `Prepare { tx, padding }`, with 24 padding bytes. The server replies
   with `Prepared { tx, addr, token }`: your public IPv4 socket as it sees it,
   plus a short-lived 16-byte token. The padding keeps this reply no larger
   than the request.
2. Send `Put { tx, token, value }` from that same socket. The server checks the
   token, stores the bytes under your observed source address, and replies with
   `Stored { tx, addr }`. You don't choose the key: the packet's source does.

To read, just send `Get { tx, addr }`. No token or prepare step needed. The reply
is `Value { tx, addr, value }`, with `Some(bytes)` for a live entry or `None` for
a miss. Reads are public.

`Get` packets are padded with trailing zeros after the postcard payload to
exactly 1200 bytes. The server drops shorter requests, so a spoofed request
can't trigger a larger reply. Receivers ignore the padding bytes.

Tokens prove you can receive packets at the source IP and port; they don't
identify you or validate your data. The server treats values as opaque bytes
and controls their expiry. Unknown protocol versions and invalid requests are
silently dropped, so callers need to handle timeouts.

## Server discovery

Servers announce on Mainline under `b86c3d910e1a67ec9ba8a69a95bd7f8b08be923b`
(SHA-1 of `iroh-addr-index servers v1`), using their implied source port.
Clients call `get_peers` on that hash to find candidate replicas. Mainline and
index traffic share the same UDP socket, so the announced port serves both.
An announcement is just a candidate, not a guarantee of availability or trust.

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
