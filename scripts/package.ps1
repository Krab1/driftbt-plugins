<#
  Builds a plugin in release mode and zips it the way Drift's installer expects: plugin.json and the program at the
  root of the zip. Prints the zip's path and sha256 (both go into Drift's registry/index.json).

    scripts\package.ps1                  # tracker-sim
    scripts\package.ps1 -Plugin tracker-sim
#>
param([string]$Plugin = "tracker-sim")
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
cargo build --release -p $Plugin
if ($LASTEXITCODE -ne 0) { throw "build failed" }
$manifest = Get-Content "crates\$Plugin\plugin.json" -Raw | ConvertFrom-Json
if ($manifest.id -ne $Plugin) { throw "plugin.json id '$($manifest.id)' must equal the folder name '$Plugin'" }
$exe = $manifest.exec[0]
$stage = "dist\stage\$Plugin"
Remove-Item $stage -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force $stage | Out-Null
Copy-Item "target\release\$exe" $stage
Copy-Item "crates\$Plugin\plugin.json" $stage
$zip = "dist\$Plugin-$($manifest.version).zip"
Remove-Item $zip -Force -ErrorAction SilentlyContinue
Compress-Archive -Path "$stage\*" -DestinationPath $zip
$sha = (Get-FileHash $zip -Algorithm SHA256).Hash.ToLower()
Write-Host "zip:    $PWD\$zip"
Write-Host "sha256: $sha"
Write-Host "size:   $((Get-Item $zip).Length) bytes"
