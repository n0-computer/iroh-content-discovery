# blake3.link Local Gateway extension

Workspace project five: a Manifest V3 extension for desktop Chrome, Brave, and
Firefox.
It sends content links to a local HTTP gateway without contacting the domain's
web server:

```text
https://<z32>.blake3.link/
    -> http://127.0.0.1:8080/blake3/<z32>
https://<z32>.blake3.link/path/to/file
    -> http://127.0.0.1:8080/blake3/<z32>/path/to/file
```

The hash is a 52-character lowercase z-base-32 encoded BLAKE3 digest, matching
the gateway route. A collection root shows an index at the bare URL. Query
strings are retained. Paths within collections are
rewritten too. The apex `https://blake3.link/` stays untouched so it
can host instructions or an extension download page. Other hosts, nested subdomains, and localhost requests are not matched.
The extension captures a single alphanumeric label; the gateway validates its
length, alphabet, and canonical encoding, returning 400 for invalid hashes.

## Install in Chrome or Brave

1. Open `chrome://extensions` in Chrome or `brave://extensions` in Brave.
2. Enable **Developer mode**.
3. Click **Load unpacked**, then choose this `blake3-link-extension` directory
   (the directory containing `manifest.json`). No build step is required.
4. Open the extension popup, set the gateway port (default **8080**), leave
   local redirects enabled, and click **Save**.
5. Start the gateway and open a content link:

   ```sh
   cargo run -p iroh-local-gateway -- --tracker 127.0.0.1:11223
   ```

Use your actual tracker address or the gateway's public-key/infohash discovery
options. A peer must be serving and announcing the requested root hash.

After editing the extension files, click its **Reload** button on the extensions
page. If previously loaded with different host permissions, approve the new
permissions or remove and load it again.

## Install in Firefox

Firefox 140 or later is required.

1. Open `about:debugging#/runtime/this-firefox`.
2. Click **Load Temporary Add-on**, then choose `manifest.json` in this
   directory.
3. Open the extension popup and click **Save**. Firefox asks for access to
   `*.blake3.link` sites; allow it, or redirects stay inactive. The popup
   shows a hint while access is missing.

Temporary add-ons are removed when Firefox restarts. For a permanent install,
Firefox needs a signed package, for example from `npx web-ext sign
--channel=unlisted` with addons.mozilla.org API credentials. Firefox Developer
Edition and Nightly can instead install an unsigned package when
`xpinstall.signatures.required` is set to `false` in `about:config`.

`npx web-ext lint` checks the manifest for Firefox. Its warning that
`background.service_worker` is ignored is expected: Chrome uses the service
worker and Firefox uses `background.scripts`, both pointing to the same file.

## Settings and behavior

The popup configures the localhost port and can disable all redirects. Saved
settings and dynamic rules survive browser restarts. There is no always-running
background process; the service worker only initializes rules at installation
or update. Rule matching and redirection happen in the browser network stack.
The browser's address bar changes to the localhost URL.

Firefox lets users withhold host access to `*.blake3.link`. The popup requests
it when saving with redirects enabled.

Permissions are limited to `*.blake3.link`, local extension settings, and
request redirection. The extension needs no access to browsing history or all
websites. The local gateway must be running; the extension does not start it.

## Tests

```sh
node --test blake3-link-extension/rules.test.js
```

The tests check hash-subdomain routing, query preservation, lookalike-host
rejection, port validation, and disabling. They model matching and are not a
replacement for loading the extension in the browser.
