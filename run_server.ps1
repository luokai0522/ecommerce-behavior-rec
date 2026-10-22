# Start the recommend service (Windows).
#
# Usage (from anywhere):
#   powershell -ExecutionPolicy Bypass -File .\run_server.ps1
#   powershell -ExecutionPolicy Bypass -File .\run_server.ps1 -Port 9000
#
# Requires: flask, pandas, pyarrow
#   python -m pip install flask pandas pyarrow -i https://pypi.tuna.tsinghua.edu.cn/simple

param([int]$Port = 8000)

$ErrorActionPreference = "Continue"
try { [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false) } catch {}

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

$py = "D:\develop\Python\python.exe"
$env:PYTHONIOENCODING = "utf-8"
$env:PORT = "$Port"

if (-not (Test-Path (Join-Path $root "data\processed\recs_als"))) {
    Write-Host "[WARN] data\processed\recs_als not found - run: run_job.ps1 -Job als" -ForegroundColor Yellow
}
if (-not (Test-Path (Join-Path $root "data\processed\user_tag"))) {
    Write-Host "[WARN] data\processed\user_tag not found - run: run_job.ps1 -Job profile" -ForegroundColor Yellow
}

Write-Host "starting service on http://127.0.0.1:$Port"
Write-Host "  GET /api/recommend/123?top=10"
Write-Host "  GET /api/profile/123"
Write-Host "  GET /api/stats"
Write-Host "press Ctrl+C to stop"
Write-Host ""

& $py (Join-Path $root "server\app.py")
