import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { DEFAULT_SETTINGS, HOST_ORIGINS, RULE_IDS, makeRules, validateSettings } from "./rules.js";

test("runtime permission requests cover both domains declared in the manifest", () => {
  const manifest = JSON.parse(readFileSync(new URL("./manifest.json", import.meta.url), "utf8"));
  assert.deepEqual(HOST_ORIGINS, manifest.host_permissions);
  for (const domain of ["blake3.net", "pkarr.net"]) {
    for (const scheme of ["http", "https"]) {
      assert.ok(HOST_ORIGINS.includes(`${scheme}://*.${domain}/*`));
    }
  }
});

// Model URL matching and transformations; Chrome/Brave enforce the actual rules.
function redirect(input, port = DEFAULT_SETTINGS.port) {
  const rules = makeRules({ port, enabled: true }).sort((a, b) => b.priority - a.priority);
  for (const { condition, action } of rules) {
    const regex = new RegExp(condition.regexFilter, condition.isUrlFilterCaseSensitive ? "" : "i");
    if (!regex.test(input)) continue;
    if (action.redirect.regexSubstitution) {
      return input.replace(regex, action.redirect.regexSubstitution.replace(/\\([0-9])/g, "$$$1"));
    }
    const url = new URL(input);
    const { scheme, host, port } = action.redirect.transform;
    url.protocol = `${scheme}:`;
    url.hostname = host;
    url.port = port;
    return url.href;
  }
  return null;
}

test("hash subdomains rewrite to a local per-hash origin", () => {
  const hash = "y".repeat(52);
  assert.equal(redirect(`https://${hash}.blake3.net/?download=1#time`), `http://${hash}.blake3.localhost:45475/?download=1#time`);
  assert.equal(redirect(`http://${hash}.blake3.net:80/`, 12345), `http://${hash}.blake3.localhost:12345/`);
  assert.equal(redirect(`https://${hash}.blake3.net/site/index.html?x=1`), `http://${hash}.blake3.localhost:45475/site/index.html?x=1`);
});

test("single labels are forwarded for gateway validation", () => {
  for (const domain of ["blake3", "pkarr"]) {
    for (const label of ["www", "docs", "not-z32", "invalid_label", "y".repeat(51), "y".repeat(53)]) {
      assert.equal(
        redirect(`https://${label}.${domain}.net/path?x=1`),
        `http://${label}.${domain}.localhost:45475/path?x=1`,
      );
    }
  }
});

test("apex, lookalikes, nested subdomains, and localhost are untouched", () => {
  const hash = "y".repeat(52);
  for (const url of [
    "https://blake3.net/", `https://blake3.net/${hash}`,
    `https://${hash}.blake3.net.evil/`, `https://evil/?host=${hash}.blake3.net`,
    `https://prefix.${hash}.blake3.net/`, 
 "http://127.0.0.1:8080/path",
    `https://${hash}.blake3.net@evil/`,
    "https://pkarr.net/", `https://pkarr.net/${hash}`,
    `https://${hash}.pkarr.net.evil/`, `https://evil/?host=${hash}.pkarr.net`,
    `https://prefix.${hash}.pkarr.net/`, `https://${hash}.pkarr.net@evil/`,
    `http://127.0.0.1:8080/pkarr/${hash}/`,
  ]) {
    assert.equal(redirect(url), null, url);
  }
});

test("public-key subdomains rewrite to a local per-key origin", () => {
  const key = "y".repeat(52);
  assert.equal(redirect(`https://${key}.pkarr.net/`), `http://${key}.pkarr.localhost:45475/`);
  assert.equal(redirect(`http://${key}.pkarr.net:80/`, 12345), `http://${key}.pkarr.localhost:12345/`);
  assert.equal(redirect(`https://${key}.pkarr.net/?q=%2F#section`), `http://${key}.pkarr.localhost:45475/?q=%2F#section`);
  assert.equal(redirect(`https://${key}.pkarr.net/a%2Fb/file%20name?x=1#part`), `http://${key}.pkarr.localhost:45475/a%2Fb/file%20name?x=1#part`);
});

test("ports are bounded, settings are optional only through defaults, disable removes rules", () => {
  for (const port of [0, -1, 65536, 1.5, NaN, "8080"]) {
    assert.throws(() => validateSettings({ port, enabled: true }));
  }
  assert.deepEqual(makeRules({ port: 8080, enabled: false }), []);
  // Rule 3 is retired, but stays in RULE_IDS so upgrades remove it.
  assert.deepEqual(makeRules(DEFAULT_SETTINGS).map(({ id }) => id), [1, 2]);
  assert.deepEqual(RULE_IDS, [1, 2, 3]);
});
