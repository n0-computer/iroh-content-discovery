"""Stage gateway binaries and local, unsigned browser-extension files."""
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tomllib
import zipfile

ROOT = Path(__file__).resolve().parents[2]
FILES = ['background.js', 'rules.js', 'popup.html', 'popup.js', 'popup.css', 'LICENSE-APACHE', 'LICENSE-MIT']

def checksum(path):
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_name(path.name + '.sha256').write_text(f'{digest}  {path.name}\n')

def extensions(destination):
    source = ROOT / 'iroh-link-extension'
    for browser in ['chrome', 'firefox']:
        folder = destination / browser
        folder.mkdir(parents=True, exist_ok=True)
        manifest = json.loads((source / 'manifest.json').read_text())
        if browser == 'chrome':
            del manifest['background']['scripts']
            del manifest['browser_specific_settings']
            manifest['minimum_chrome_version'] = '121'
        else:
            del manifest['background']['service_worker']
        (folder / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        for name in FILES:
            shutil.copy2(source / name, folder / name)
    # Unsigned XPI for Developer Edition/Nightly/ESR; release Firefox needs signing.
    with zipfile.ZipFile(destination / 'iroh-link-firefox-unsigned.xpi', 'w', zipfile.ZIP_DEFLATED) as archive:
        for file in sorted((destination / 'firefox').iterdir()):
            archive.write(file, file.name)
    shutil.copy2(ROOT / 'packaging/gateway/extensions.html', destination / 'Install extensions.html')

def stage(target):
    destination = ROOT / 'dist/gateway' / target
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True)
    suffix = '.exe' if 'windows' in target else ''
    for binary in ['iroh-local-gateway', 'iroh-gateway-background']:
        shutil.copy2(ROOT / 'target' / target / 'release' / (binary + suffix), destination)
    for name in ['README.md', 'LICENSE-APACHE', 'LICENSE-MIT']:
        shutil.copy2(ROOT / 'iroh-local-gateway' / name, destination)
    extensions(destination / 'extensions')
    return destination

def version():
    return tomllib.loads((ROOT / 'Cargo.toml').read_text())['workspace']['package']['version']

if __name__ == '__main__':
    print(stage(sys.argv[1]))
