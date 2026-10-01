"""Build and install the AUR package from the local x86_64 archive in an Arch Linux container."""
import pathlib
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from stage import ROOT, version
from pkgbuild import render

logs = ROOT / 'installer-test-logs'
logs.mkdir(exist_ok=True)
archive = ROOT / 'dist' / f'iroh-link-gateway-{version()}-linux-x64.tar.gz'
# makepkg uses a source file already beside the PKGBUILD instead of downloading
# it, so this builds the package from the archive the release will publish.
SCRIPT = """set -e
pacman -Syu --noconfirm --needed base-devel namcap >/dev/null
useradd --create-home builder
cp -r /aur /home/builder/aur && chown -R builder /home/builder/aur
cd /home/builder/aur
su builder -c 'makepkg --printsrcinfo > .SRCINFO && makepkg --nodeps --noconfirm'
namcap PKGBUILD *.pkg.tar.zst
pacman -U --noconfirm iroh-link-gateway-bin-*.pkg.tar.zst
iroh-link-gateway --help >/dev/null
test -f /usr/lib/systemd/system/iroh-link-gateway.service
test -f /usr/lib/systemd/user/iroh-link-gateway.service
test -f /usr/share/iroh-link-gateway/extensions/chrome/manifest.json
test -f /usr/share/licenses/iroh-link-gateway-bin/LICENSE-MIT
pacman -R --noconfirm iroh-link-gateway-bin
! test -e /usr/bin/iroh-link-gateway
"""

if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='iroh-link-gateway-aur-') as temporary:
        work = pathlib.Path(temporary)
        # Only the x86_64 archive exists here, and Arch Linux images are x86_64 only.
        sums = {'X86_64': (archive.parent / (archive.name + '.sha256')).read_text().split()[0], 'AARCH64': 'SKIP'}
        (work / 'PKGBUILD').write_text(render(sums))
        shutil.copy2(archive, work)
        runtime = shutil.which('docker') or shutil.which('podman')
        result = subprocess.run([runtime, 'run', '--rm', '-v', f'{work}:/aur:ro', 'archlinux:latest', 'bash', '-c', SCRIPT],
                                capture_output=True, text=True, timeout=900)
        (logs / 'aur.log').write_text(result.stdout + result.stderr)
        assert result.returncode == 0, result.stdout + result.stderr
        print('PASS: AUR package builds, passes namcap, installs, and removes')
