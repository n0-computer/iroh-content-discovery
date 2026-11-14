# iroh link for Android

A small Android browser for `https://<hash>.blake3.net` and
`https://<key>.pkarr.net` links. It runs the [iroh link gateway] inside the
app, so you need no desktop gateway and no browser extension.

The app has tabs, back and forward, history, and bookmarks. Pages run in a
full Chromium WebView with JavaScript, local storage, IndexedDB, and
WebAssembly. Each hash and each key gets its own origin, which is a secure
context.

## How it works

On start, the app runs the Rust gateway on `127.0.0.1:45475`. The WebView loads
a link the same way the desktop extension rewrites it:

```text
https://<hash>.blake3.net/path   ->  http://<hash>.blake3.localhost:45475/path
https://<key>.pkarr.net/path     ->  http://<key>.pkarr.localhost:45475/path
```

Chromium resolves `*.localhost` to the loopback address and treats it as a
secure context. The address bar, history, bookmarks, and shared links always
show the public `https://` form. A page can also use absolute `blake3.net` or
`pkarr.net` URLs for images, scripts, or frames. The app sends those requests
to the gateway as well.

The port is part of every page origin. If it changed, sites would lose their
local storage. For this reason the port is fixed. If another app holds the
port, the app shows an error and loads no pages.

The WebView's own HTTP cache holds only about 20 MB and cannot be made
larger, so it never keeps large files such as WebAssembly modules. The app
puts its own cache of up to 512 MB in front of the gateway:

- `blake3.net` content is immutable. The app serves it from disk without a
  request.
- `pkarr.net` content can change. The app revalidates it with `If-None-Match`,
  so an unchanged file costs a `304`. If the gateway cannot reach the content,
  the app serves the cached copy.

The gateway's ETag is the BLAKE3 hash of the file, so each file is stored once.
If a transfer breaks, the app resumes it with `Range` and `If-Range` and keeps
trying for 90 seconds. Requests with a `Range` header, as media elements send
them, go to the gateway directly. Android can clear the cache when storage runs
low, and you can clear it in the app's system settings.

The gateway finds index servers when the app starts, which takes a few
seconds. A banner shows the progress. Requests made in that time wait for the
gateway.

## Open links in the app

The app registers for `*.blake3.net` and `*.pkarr.net`. Android 12 and later
open web links in the default browser unless you approve the domains. To
approve them, open the menu, select **Open links by default**, then
**Add link**, and select both domains.

Without approval, you can share a link to the app, or paste it into the
address bar.

## Build

You need the Android SDK and NDK, a JDK 17 or later, and [cargo-ndk]:

```sh
rustup target add aarch64-linux-android x86_64-linux-android
cargo install cargo-ndk
```

Build the native library for `arm64-v8a` (phones) and `x86_64` (emulator),
then the app:

```sh
./build-native.sh
./gradlew testDebugUnitTest assembleDebug
```

For a signed release build, put a `keystore.properties` file next to this
README:

```properties
storeFile=release.jks
storePassword=...
keyAlias=iroh-link
keyPassword=...
```

Then run `./gradlew assembleRelease`. Without the file, the release APK is
unsigned. Keep the keystore safe: Android installs an update only if it has the
same signature.

Debuggable builds enable WebView remote debugging, so you can inspect pages at
`chrome://inspect`. Gateway logs go to logcat with the tag `iroh-link`, and page
console messages use the tag `iroh-link-web`.

## Try it with iroh in WebAssembly

The `publish-dir` example of the gateway publishes a directory as a website.
It announces the content on Mainline and points a Pkarr name at it. For
example, you can publish the [browser-echo] example from iroh-examples, which
runs iroh compiled to WebAssembly:

```sh
# In iroh-examples/browser-echo:
cargo build --target wasm32-unknown-unknown --release
wasm-bindgen target/wasm32-unknown-unknown/release/browser_echo.wasm \
    --out-dir public/wasm --weak-refs --target web
cargo run --features cli -- accept

# In this repository:
cargo run -p iroh-link-gateway --example publish-dir -- \
    ../iroh-examples/browser-echo/public --key-file echo.key
```

The `wasm-bindgen` CLI version must match the crate's `wasm-bindgen`
dependency. Open the printed `pkarr.net` link in the app, and append
`?connect=<endpoint-id>&payload=hello` with the ID that the CLI prints. The page
connects to the CLI, and the CLI echoes the payload. You can also connect from
the CLI to the endpoint ID that the page shows.

[iroh link gateway]: ../iroh-link-gateway/README.md
[cargo-ndk]: https://github.com/bbqsrc/cargo-ndk
[browser-echo]: https://github.com/n0-computer/iroh-examples/tree/main/browser-echo
