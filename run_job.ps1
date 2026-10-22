# Local Spark job runner (Windows).
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File .\run_job.ps1 -Job clean
#   powershell -ExecutionPolicy Bypass -File .\run_job.ps1 -Job profile
#
# Notes:
#  * Output goes outside the project (DSH workspace ACL blocks chmod),
#    results are copied back to data\processed\ afterwards.
#  * All paths below are machine-specific.

param([string]$Job = "clean")

$ErrorActionPreference = "Continue"
try { [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false) } catch {}

$env:JAVA_HOME        = "C:\Java\jdk-17\jdk-17.0.20.1+1"
$env:SPARK_HOME       = "D:\develop\Python\Lib\site-packages\pyspark"
$env:HADOOP_HOME      = "C:\hadoop"
$env:PYSPARK_PYTHON   = "D:\develop\Python\python.exe"
$env:PYTHONIOENCODING = "utf-8"
$env:Path = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:SPARK_HOME\bin;$env:Path"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path

# ---- writable output root ----
$candidates = @(
    (Join-Path $env:TEMP "spark_out"),
    (Join-Path $env:LOCALAPPDATA "spark_out"),
    "D:\spark_out"
)
$outRoot = $null
foreach ($c in $candidates) {
    try {
        New-Item -ItemType Directory -Force -Path $c -ErrorAction Stop | Out-Null
        $probe = Join-Path $c ".probe"
        Set-Content -Path $probe -Value "ok" -ErrorAction Stop
        Remove-Item $probe -Force -ErrorAction SilentlyContinue
        $outRoot = $c
        break
    } catch { }
}
if (-not $outRoot) { Write-Host "[ERROR] no writable output dir" -ForegroundColor Red; exit 1 }

switch ($Job) {
    "clean" {
        $script  = Join-Path $root "spark_jobs\clean.py"
        $args2   = @("--input", (Join-Path $root "data\sample\sample_100k.csv"),
                     "--output", (Join-Path $outRoot "ods"))
        $dest    = Join-Path $root "data\processed\ods"
    }
    "profile" {
        $script  = Join-Path $root "spark_jobs\user_profile.py"
        $args2   = @("--input", (Join-Path $root "data\processed\ods"),
                     "--output", (Join-Path $outRoot "user_tag"))
        $dest    = Join-Path $root "data\processed\user_tag"
    }
    "itemcf" {
        $script  = Join-Path $root "spark_jobs\itemcf.py"
        $args2   = @("--input", (Join-Path $root "data\processed\ods"),
                     "--output", (Join-Path $outRoot "recs_itemcf"),
                     "--eval-users", "2000")
        $dest    = Join-Path $root "data\processed\recs_itemcf"
    }
    default {
        Write-Host "[ERROR] unknown job: $Job (use clean|profile|itemcf)" -ForegroundColor Red
        exit 1
    }
}

$logFile = Join-Path $root "run_$Job.log"
$sparkSubmit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"

Write-Host "job   : $Job"
Write-Host "script: $script"
Write-Host "log   : $logFile"
Write-Host "starting ..."
Write-Host ""

& $sparkSubmit --master "local[4]" --conf "spark.sql.shuffle.partitions=8" $script @args2 *> $logFile
$code = $LASTEXITCODE

Write-Host ""
Write-Host "---- exit code: $code ----"
Get-Content $logFile -Encoding utf8 | Where-Object { $_ -match "^\[(clean|profile|itemcf)\]|^\[热门基线\]|^  【|^    - " }

if ($code -eq 0) {
    Write-Host ""
    Write-Host "copying results into the project ..."
    $destParent = Split-Path $dest
    if (-not (Test-Path $destParent)) { New-Item -ItemType Directory -Force -Path $destParent | Out-Null }
    if (Test-Path $dest) { Remove-Item -Recurse -Force $dest -ErrorAction SilentlyContinue }
    try {
        Copy-Item -Recurse -Force (Join-Path $outRoot (Split-Path $dest -Leaf)) $dest -ErrorAction Stop
        Write-Host "[OK] copied to $dest" -ForegroundColor Green
    } catch {
        Write-Host "[WARN] copy failed, results stay in $outRoot" -ForegroundColor Yellow
    }
} else {
    Write-Host ""
    Write-Host "[FAILED] last 25 lines:" -ForegroundColor Red
    Get-Content $logFile -Tail 25 -Encoding utf8
}
