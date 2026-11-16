$ErrorActionPreference = 'Stop'

$project = $PSScriptRoot
$config = Join-Path $project 'examples\arduino.toml'

if (-not (Test-Path -LiteralPath $config)) {
    throw "Configuration not found: $config"
}

Push-Location $project

try {
    Write-Host 'Starting lab-server with Arduino configuration...'
    Write-Host "Config: $config"
    Write-Host 'Press Ctrl+C to stop.'

    cargo run --release -- --config $config

    if ($LASTEXITCODE -ne 0) {
        throw "lab-server exited with code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}
