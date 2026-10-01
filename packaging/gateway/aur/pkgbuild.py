"""Write the AUR PKGBUILD for the Linux archives of the current version."""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from stage import ROOT, version

TEMPLATE = pathlib.Path(__file__).with_name('PKGBUILD.in')
ARCHIVES = {'X86_64': 'x64', 'AARCH64': 'arm64'}

def render(checksums):
    """Fill in the version and the SHA-256 checksum of each architecture's archive."""
    text = TEMPLATE.read_text().replace('@PKGVER@', version())
    for architecture, checksum in checksums.items():
        text = text.replace(f'@SHA256_{architecture}@', checksum)
    assert '@' not in text.replace('hello@n0.computer', ''), 'unfilled placeholder'
    return text

def checksums(dist):
    """Read the checksums that build.py wrote beside the archives."""
    return {architecture: (dist / f'iroh-link-gateway-{version()}-linux-{suffix}.tar.gz.sha256').read_text().split()[0]
            for architecture, suffix in ARCHIVES.items()}

if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    output.mkdir(parents=True, exist_ok=True)
    (output / 'PKGBUILD').write_text(render(checksums(ROOT / 'dist')))
    print(output / 'PKGBUILD')
