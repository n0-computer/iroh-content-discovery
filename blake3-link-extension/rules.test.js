import assert from "node:assert/strict";
import test from "node:test";
import { DEFAULT_SETTINGS, makeRules, validateSettings } from "./rules.js";

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

test("hash subdomains rewrite to a local per-hash origin", () => {
  const hash = "y".repeat(52);
  assert.equal(redirect(`https://${hash}.blake3.link/?download=1#time`), `http://${hash}.localhost:8080/?download=1#time`);
  assert.equal(redirect(`http://${hash}.blake3.link:80/`, 12345), `http://${hash}.localhost:12345/`);
  assert.equal(redirect(`https://${hash}.blake3.link/site/index.html?x=1`), `http://${hash}.localhost:8080/site/index.html?x=1`);
});

test("apex, lookalikes, nested subdomains, and localhost are untouched", () => {
  const hash = "y".repeat(52);
  for (const url of [
    "https://blake3.link/", `https://blake3.link/${hash}`,
    `https://${hash}.blake3.link.evil/`, `https://evil/?host=${hash}.blake3.link`,
    `https://prefix.${hash}.blake3.link/`, 
 "http://127.0.0.1:8080/path",
    `https://${hash}.blake3.link@evil/`,
  ]) {
    assert.equal(redirect(url), null, url);
  }
});

test("ports are bounded, settings are optional only through defaults, disable removes rules", () => {
  for (const port of [0, -1, 65536, 1.5, NaN, "8080"]) {
    assert.throws(() => validateSettings({ port, enabled: true }));
  }
  assert.deepEqual(makeRules({ port: 8080, enabled: false }), []);
  assert.equal(makeRules(DEFAULT_SETTINGS).length, 1);
});
