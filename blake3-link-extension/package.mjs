// Build unsigned, browser-specific ZIPs using Node.js and the zip utility.
import { execFileSync } from "node:child_process";
import { copyFile, mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(fileURLToPath(import.meta.url));
const platform = process.argv[2] ?? "all";
if (!["chrome", "firefox", "all"].includes(platform) || process.argv.length > 3) {
  console.error("Usage: node package.mjs [chrome|firefox|all]");
  process.exit(1);
}

const source = JSON.parse(await readFile(join(root, "manifest.json"), "utf8"));
if (!/^\d+(?:\.\d+){0,3}$/.test(source.version)) {
  throw new Error("Expected a numeric extension version in manifest.json");
}
const files = ["manifest.json", "background.js", "rules.js", "popup.html", "popup.js", "popup.css"];
const output = join(root, "dist");
await mkdir(output, { recursive: true });
for (const browser of platform === "all" ? ["chrome", "firefox"] : [platform]) {
  const staging = await mkdtemp(join(tmpdir(), "iroh-extension-"));
  const archive = join(output, `iroh-link-${source.version}-${browser}.zip`);
  const temporaryArchive = `${archive}.tmp.zip`;
  try {
    const manifest = structuredClone(source);
    if (browser === "chrome") {
      delete manifest.background.scripts;
      delete manifest.browser_specific_settings;
      manifest.minimum_chrome_version = "121";
    } else {
      delete manifest.background.service_worker;
    }
    await writeFile(join(staging, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
    for (const file of files.slice(1)) {
      await copyFile(join(root, file), join(staging, file));
    }
    // Build a fresh archive so removed files cannot survive a previous build.
    await rm(temporaryArchive, { force: true });
    execFileSync("zip", ["-X", "-q", temporaryArchive, ...files], { cwd: staging });
    await rename(temporaryArchive, archive);
    console.log(archive);
  } finally {
    await rm(staging, { recursive: true, force: true });
    await rm(temporaryArchive, { force: true });
  }
}
