# Auth0 Migration Dashboard - startup script
# Run from the migration-dashboard folder: .\start.ps1

$ErrorActionPreference = 'Stop'
$dashDir = $PSScriptRoot

# Check that dependencies are installed
if (-not (Test-Path "$dashDir\node_modules")) {
    Write-Host "Installing dependencies..." -ForegroundColor Yellow
    Push-Location $dashDir
    npm install
    Pop-Location
}

# Copy env template if .env doesn't exist
$envFile = "$dashDir\.env"
if (-not (Test-Path $envFile)) {
    Copy-Item "$dashDir\.env.example" $envFile
    Write-Host ".env created from template (edit if needed)" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "  Starting Auth0 Migration Dashboard" -ForegroundColor Cyan
Write-Host "  Open: http://localhost:3001" -ForegroundColor Green
Write-Host "  Press Ctrl+C to stop" -ForegroundColor Gray
Write-Host ""

Push-Location $dashDir
node server.js
Pop-Location
