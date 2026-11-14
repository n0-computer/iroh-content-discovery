# iroh content discovery

Global content discovery for [iroh], using the BitTorrent [Mainline] DHT.

You publish a blob, and anyone who knows its BLAKE3 hash can find you and
download it, without a tracker, a server you run, or anyone's permission. On
top of that, `https://<hash>.blake3.net` links let you browse content-addressed
websites, and `https://<key>.pkarr.net` names let you point to them.

The background is in the blog post [Iroh global content discovery][blog].

## How it works

Mainline is the most successful permissionless content discovery system out
there, so we use it as is. A provider announces `SHA-1(blake3_hash)` on
Mainline. A reader asks Mainline for that infohash and gets back a list of
`host:port` pairs.

But iroh dials endpoints by `EndpointId`, not by address, and most of those
addresses are behind a NAT anyway. So we need one tiny extra piece: an
**address index** that maps a verified `host:port` to a few bytes. Providers
store a signed record with their `EndpointId` there, over the same UDP socket
they use for Mainline. Readers translate the `host:port` pairs into candidate
endpoints, dial them with iroh's normal hole punching, and verify every byte
against the BLAKE3 hash. A wrong answer anywhere along the way can waste time,
but it can't make you accept the wrong content.

The address index doesn't depend on iroh at all. In the long run we would love
for Mainline to do this job itself.

## Browsing the content-addressed web

1. Install the [gateway][gateway-readme]. There are installers for macOS,
   Windows and Linux on the [releases page][releases], or build it from source.
2. Install the [iroh link browser extension][extension-readme] from the
   [Chrome Web Store][chrome-store] for Chrome or Brave. The Firefox version is
   awaiting review; until then you can [load it as a temporary
   add-on][firefox].

The extension rewrites `blake3.net` and `pkarr.net` links to the gateway on
`localhost:45475`, which finds the content and streams it to the browser. Try
[some static content][demo-blake3], or [a pkarr name][demo-pkarr] currently
pointing to it.

To publish your own content and names, use [iroh-share], a daemon that keeps
your content and names announced. Run it on a box that's on all the time.

## What's in here

- [`iroh-mainline-endpoint-discovery`][discovery]: publish and resolve iroh
  endpoints through Mainline and the address index. Start here if you want to
  use this from your own iroh application.
- [`udp-addr-index`][server] and [`udp-addr-index-proto`][proto]: the address
  index server and its wire protocol.
- [`iroh-link-gateway`][gateway-readme]: the local HTTP gateway.
- [`iroh-link-extension`][extension-readme]: the browser extension.
- [`iroh-link-android`][android-readme]: an Android browser with the gateway
  built in.

To see the whole thing work in one process, discovery and all:

```sh
cargo run -p iroh-mainline-endpoint-discovery --example blobs
```

## Status

This is experimental. It works, but it's far from perfect: Mainline traffic is
unencrypted and easy to block, Pkarr names use Ed25519, which is not post-quantum
secure, and most importantly, there is **no privacy**. If you share content,
anybody can look up your IP address.

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

[iroh]: https://www.iroh.computer
[Mainline]: https://www.bittorrent.org/beps/bep_0005.html
[blog]: https://www.iroh.computer/blog/iroh-global-content-discovery
[releases]: https://github.com/n0-computer/iroh-content-discovery/releases/latest
[chrome-store]: https://chromewebstore.google.com/detail/iroh-link/aajlbmaphckgbinhnifpiggcmdfnofcd
[firefox]: iroh-link-extension/README.md#install-in-firefox
[demo-blake3]: https://y7rmokt6h5mryuauw83em4u1br6tqrukaw3ngtde7zp8p3bg6hto.blake3.net/
[demo-pkarr]: https://5ti57aszf7kaicsncb4wgigkf9bju39kofiz8dthwdujkmz85u8y.pkarr.net/
[iroh-share]: https://github.com/n0-computer/iroh-share
[discovery]: iroh-mainline-endpoint-discovery/README.md
[server]: udp-addr-index/README.md
[proto]: udp-addr-index-proto/README.md
[gateway-readme]: iroh-link-gateway/README.md
[android-readme]: iroh-link-android/README.md
[extension-readme]: iroh-link-extension/README.md
