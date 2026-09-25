$ErrorActionPreference = 'Stop'
if ($env:CI -ne 'true') { throw 'Run this installation test only on an ephemeral CI runner' }
$root = (Resolve-Path "$PSScriptRoot/../../..").Path
$installer = (Get-ChildItem "$root/dist/*-windows-x64-setup.exe" | Select-Object -First 1).FullName
$app = "$env:LOCALAPPDATA\Programs\Iroh Gateway"
$state = "$env:LOCALAPPDATA\iroh-local-gateway"
$logs = "$root/installer-test-logs"
if (Test-Path $app) { throw 'Refusing to replace an existing installation' }
New-Item -ItemType Directory -Force $logs, $state | Out-Null
# Avoid public discovery dependencies in the installation test.
$settings = '["--listen","127.0.0.1:18080","--index-server","127.0.0.1:9"]'
Set-Content "$state/arguments.json" $settings -Encoding ascii
function Install($label) {
    $process = Start-Process $installer -ArgumentList @('/VERYSILENT', '/SUPPRESSMSGBOXES', '/NORESTART', "/LOG=`"$logs/$label.log`"") -PassThru
    if (!$process.WaitForExit(120000)) { throw 'Installer timed out' }
    if ($process.ExitCode -ne 0) { throw "Installer failed: $($process.ExitCode)" }
}
function Status {
    $process = Start-Process "$app/iroh-gateway-background.exe" -ArgumentList 'status' -PassThru -Wait
    if ($process.ExitCode -ne 0) { throw 'Gateway is not running' }
    if (!(Test-Path "$state/ready")) { throw 'Gateway has no bound listener' }
    $client = New-Object System.Net.Sockets.TcpClient
    try { $client.Connect('127.0.0.1', 18080) } finally { $client.Dispose() }
}
try {
    Install 'install'
    Status
    foreach ($name in @('iroh-local-gateway.exe','iroh-gateway-background.exe','extensions/chrome/manifest.json','extensions/firefox/manifest.json','extensions/iroh-link-firefox-unsigned.xpi','extensions/Install extensions.html')) {
        if (!(Test-Path "$app/$name")) { throw "Missing $name" }
    }
    $startup = (Get-ItemProperty 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Run').'Iroh Gateway'
    if ($startup -ne "`"$app\iroh-gateway-background.exe`"") { throw "Wrong login command: $startup" }
    Install 'upgrade'
    Status
    if ((Get-Content "$state/arguments.json" -Raw).Trim() -ne $settings) { throw 'Upgrade changed arguments' }
    $process = Start-Process "$app/unins000.exe" -ArgumentList @('/VERYSILENT','/SUPPRESSMSGBOXES','/NORESTART',"/LOG=`"$logs/uninstall.log`"") -PassThru
    if (!$process.WaitForExit(120000)) { throw 'Uninstall timed out' }
    if ($process.ExitCode -ne 0) { throw 'Uninstall failed' }
    if ((Test-Path "$app/iroh-local-gateway.exe") -or (Test-Path "$state/ready")) { throw 'Uninstall left gateway running or installed' }
    if (Get-ItemProperty 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Run' -Name 'Iroh Gateway' -ErrorAction SilentlyContinue) { throw 'Login entry remains' }
    if ((Get-Content "$state/arguments.json" -Raw).Trim() -ne $settings) { throw 'Uninstall removed settings' }
    Write-Host 'PASS: install, login registration, local extension files, upgrade, stop, uninstall, settings retention'
} finally {
    foreach ($name in @('gateway.log','launcher.log')) {
        if (Test-Path "$state/$name") { Copy-Item "$state/$name" $logs }
    }
}
