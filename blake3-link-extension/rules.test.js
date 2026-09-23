import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { DEFAULT_SETTINGS, HOST_ORIGINS, RULE_IDS, makeRules, validateSettings } from "./rules.js";

test("runtime permission requests cover both domains declared in the manifest", () => {
  const manifest = JSON.parse(readFileSync(new URL("./manifest.json", import.meta.url), "utf8"));
  assert.deepEqual(HOST_ORIGINS, manifest.host_permissions);
  for (const domain of ["blake3.link", "pkarr.link"]) {
    for (const scheme of ["http", "https"]) {
      assert.ok(HOST_ORIGINS.includes(`${scheme}://*.${domain}/*`));
    }
  }
});

// Model URL matching and transformations; Chrome/Brave enforce the actual rules.
function redirect(input, port = 8080) {
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

test("hash subdomains rewrite to the local blob route", () => {
  const hash = "y".repeat(52);
  assert.equal(redirect(`https://${hash}.blake3.link/?download=1#time`), `http://127.0.0.1:8080/blake3/${hash}?download=1#time`);
  assert.equal(redirect(`http://${hash}.blake3.link:80/`, 12345), `http://127.0.0.1:12345/blake3/${hash}`);
  assert.equal(redirect(`https://${hash}.blake3.link/site/index.html?x=1`), `http://127.0.0.1:8080/blake3/${hash}/site/index.html?x=1`);
});

test("apex, lookalikes, nested subdomains, and localhost are untouched", () => {
  const hash = "y".repeat(52);
  for (const url of [
    "https://blake3.link/", `https://blake3.link/${hash}`,
    `https://${hash}.blake3.link.evil/`, `https://evil/?host=${hash}.blake3.link`,
    `https://prefix.${hash}.blake3.link/`, 
 "http://127.0.0.1:8080/path",
    `https://${hash}.blake3.link@evil/`,
    "https://pkarr.link/", `https://pkarr.link/${hash}`,
    `https://${hash}.pkarr.link.evil/`, `https://evil/?host=${hash}.pkarr.link`,
    `https://prefix.${hash}.pkarr.link/`, `https://${hash}.pkarr.link@evil/`,
    `http://127.0.0.1:8080/pkarr/${hash}/`,
  ]) {
    assert.equal(redirect(url), null, url);
  }
});

test("public-key subdomains rewrite to the local Pkarr route", () => {
  const key = "y".repeat(52);
  assert.equal(redirect(`https://${key}.pkarr.link/`), `http://127.0.0.1:8080/pkarr/${key}/`);
  assert.equal(redirect(`http://${key}.pkarr.link:80/`, 12345), `http://127.0.0.1:12345/pkarr/${key}/`);
  assert.equal(redirect(`https://${key}.pkarr.link/?q=%2F#section`), `http://127.0.0.1:8080/pkarr/${key}/?q=%2F#section`);
  assert.equal(redirect(`https://${key}.pkarr.link/a%2Fb/file%20name?x=1#part`), `http://127.0.0.1:8080/pkarr/${key}/a%2Fb/file%20name?x=1#part`);
});

test("ports are bounded, settings are optional only through defaults, disable removes rules", () => {
  for (const port of [0, -1, 65536, 1.5, NaN, "8080"]) {
    assert.throws(() => validateSettings({ port, enabled: true }));
  }
  assert.deepEqual(makeRules({ port: 8080, enabled: false }), []);
  assert.deepEqual(makeRules(DEFAULT_SETTINGS).map(({ id }) => id), RULE_IDS);
  assert.equal(RULE_IDS.length, 3);
});
