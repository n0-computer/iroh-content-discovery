"""Install, upgrade, and uninstall only on an ephemeral macOS CI runner."""
import os
from pathlib import Path
import socket
import subprocess

if os.environ.get('CI') != 'true':
    raise SystemExit('Run this installation test only on an ephemeral CI runner')
root = Path(__file__).resolve().parents[3]
home = Path.home()
app = home / 'Applications/Iroh Gateway.app'
extensions = home / 'Applications/Iroh Gateway Extensions'
state = home / 'Library/Application Support/iroh-local-gateway'
agent = home / 'Library/LaunchAgents/computer.n0.iroh-local-gateway.plist'
if app.exists() or agent.exists():
    raise SystemExit('Refusing to replace an existing installation')
logs = root / 'installer-test-logs'
logs.mkdir(exist_ok=True)
state.mkdir(parents=True, exist_ok=True)
settings = '["--listen","127.0.0.1:18080","--index-server","127.0.0.1:9"]'
(state / 'arguments.json').write_text(settings)
package = next((root / 'dist').glob('*-macos-arm64.pkg'))

def install(label):
    result = subprocess.run(['/usr/sbin/installer', '-pkg', str(package), '-target', 'CurrentUserHomeDirectory'],
                            capture_output=True, text=True, timeout=180)
    (logs / f'{label}.log').write_text(result.stdout + result.stderr)
    assert result.returncode == 0, result.stdout + result.stderr

def status():
    subprocess.run([str(app / 'Contents/MacOS/iroh-gateway-background'), 'status'], check=True, timeout=30)
    assert (state / 'ready').exists()
    with socket.create_connection(('127.0.0.1', 18080), timeout=5):
        pass

try:
    install('install')
    status()
    assert agent.is_file()
    assert app.stat().st_uid == os.getuid()
    for name in ['chrome/manifest.json', 'firefox/manifest.json', 'iroh-link-firefox-unsigned.xpi', 'Install extensions.html']:
        assert (extensions / name).is_file(), name
    install('upgrade')
    status()
    assert (state / 'arguments.json').read_text() == settings
    result = subprocess.run(['/bin/sh', str(home / 'Applications/Uninstall Iroh Gateway.command'), '--yes'],
                            capture_output=True, text=True, timeout=90)
    (logs / 'uninstall.log').write_text(result.stdout + result.stderr)
    assert result.returncode == 0, result.stdout + result.stderr
    assert not app.exists() and not agent.exists() and not extensions.exists()
    assert not (state / 'ready').exists()
    assert (state / 'arguments.json').read_text() == settings
    print('PASS: per-user package, LaunchAgent, local extensions, upgrade, uninstall, settings retention')
finally:
    for name in ['gateway.log', 'launcher.log']:
        path = state / name
        if path.exists():
            (logs / name).write_bytes(path.read_bytes())
