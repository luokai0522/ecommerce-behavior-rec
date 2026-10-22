# Local runner for the Spark cleaning job (Windows).
# Usage (from anywhere):
#   powershell -ExecutionPolicy Bypass -File "<path>\run_clean.ps1"
#
# Why the output goes outside the project:
#   The project sits inside the DSH workspace, whose ACL blocks chmod
#   (winutils "ChangeFileModeByMask error (5)"). The script picks the
#   first writable location outside the workspace and copies results back.

$ErrorActionPreference = "Continue"

try { [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false) } catch {}

# ---- Environment (rebuilt every run) ----
$env:JAVA_HOME        = "C:\Java\jdk-17\jdk-17.0.20.1+1"
$env:SPARK_HOME       = "D:\develop\Python\Lib\site-packages\pyspark"
$env:HADOOP_HOME      = "C:\hadoop"
$env:PYSPARK_PYTHON   = "D:\develop\Python\python.exe"
$env:PYTHONIOENCODING = "utf-8"
$env:Path = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:SPARK_HOME\bin;$env:Path"

$root      = Split-Path -Parent $MyInvocation.MyCommand.Path
$inputFile = Join-Path $root "data\sample\sample_100k.csv"
$finalDir  = Join-Path $root "data\processed\ods"
$logFile   = Join-Path $root "run.log"

if (-not (Test-Path $inputFile)) {
    Write-Host "[ERROR] input not found: $inputFile" -ForegroundColor Red
    exit 1
}

# ---- pick a writable output directory outside the workspace ----
$candidates = @(
    (Join-Path $env:TEMP "spark_out\ods"),
    (Join-Path $env:LOCALAPPDATA "spark_out\ods"),
    "D:\spark_out\ods",
    "E:\spark_out\ods"
)
$outputDir = $null
foreach ($c in $candidates) {
    try {
        New-Item -ItemType Directory -Force -Path $c -ErrorAction Stop | Out-Null
        $probe = Join-Path $c ".probe"
        Set-Content -Path $probe -Value "ok" -ErrorAction Stop
        Remove-Item $probe -Force -ErrorAction SilentlyContinue
        $outputDir = $c
        break
    } catch {
        Write-Host "[skip] not writable: $c"
    }
}
if (-not $outputDir) {
    Write-Host "[ERROR] no writable output directory found" -ForegroundColor Red
    exit 1
}

if (Test-Path $outputDir) { Remove-Item -Recurse -Force $outputDir -ErrorAction SilentlyContinue }
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

Write-Host "input : $inputFile"
Write-Host "output: $outputDir"
Write-Host "log   : $logFile"
Write-Host "starting (first launch takes 20-40s) ..."
Write-Host ""

$sparkSubmit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"
$jobScript   = Join-Path $root "spark_jobs\clean.py"

& $sparkSubmit --master "local[4]" `
    --conf "spark.sql.shuffle.partitions=8" `
    $jobScript --input $inputFile --output $outputDir *> $logFile

$code = $LASTEXITCODE
Write-Host ""
Write-Host "---- exit code: $code ----"
Write-Host "---- job output ----"
Get-Content $logFile -Encoding utf8 | Where-Object { $_ -match "^\[clean\]" }

if ($code -eq 0) {
    Write-Host ""
    Write-Host "[OK] partitions produced:" -ForegroundColor Green
    Get-ChildItem $outputDir | Select-Object -First 12 Name
    Write-Host ""
    Write-Host "copying results into the project ..."
    if (Test-Path $finalDir) { Remove-Item -Recurse -Force $finalDir -ErrorAction SilentlyContinue }
    New-Item -ItemType Directory -Force -Path (Split-Path $finalDir) | Out-Null
    try {
        Copy-Item -Recurse -Force $outputDir $finalDir -ErrorAction Stop
        Write-Host "[OK] copied to $finalDir"
    } catch {
        Write-Host "[WARN] copy failed, results stay at $outputDir" -ForegroundColor Yellow
    }
} else {
    Write-Host ""
    Write-Host "[FAILED] last 25 lines of log:" -ForegroundColor Red
    Get-Content $logFile -Tail 25 -Encoding utf8
}
