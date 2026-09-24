$ErrorActionPreference = 'Stop'
$root = (Resolve-Path "$PSScriptRoot/../../..").Path
$version = python -c "import tomllib; print(tomllib.load(open(__import__('sys').argv[1],'rb'))['workspace']['package']['version'])" "$root/Cargo.toml"
if ($LASTEXITCODE -ne 0) { throw 'Could not read workspace version' }
$compiler = "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe"
if (!(Test-Path $compiler)) {
    choco install innosetup --version=6.4.3 --yes --no-progress
    if ($LASTEXITCODE -ne 0) { throw 'Inno Setup installation failed' }
}
if (!(Test-Path $compiler)) { throw 'Inno Setup compiler not found' }
& $compiler "/DAppVersion=$version" "/DBuildDir=$root\dist\gateway\x86_64-pc-windows-msvc" "$PSScriptRoot\gateway.iss"
if ($LASTEXITCODE -ne 0) { throw 'Installer compilation failed' }
$installer = Get-Item "$root/dist/iroh-local-gateway-$version-windows-x64-setup.exe"
$hash = (Get-FileHash $installer.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
Set-Content -Path "$($installer.FullName).sha256" -Value "$hash  $($installer.Name)" -Encoding ascii
