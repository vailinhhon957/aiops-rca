param(
    [ValidateSet("balanced", "normal-priority")]
    [string]$Mode = "balanced",

    [string]$AnomalyDataRoot = "D:\HOCTAP\2025-2026\HK2\DACN\microservices-demo\data_anomaly_balanced_v3",

    [string]$RcaDataRoot = "D:\HOCTAP\2025-2026\HK2\DACN\microservices-demo\data_rca_balanced_v3",

    [ValidateSet("auto", "cpu", "cuda")]
    [string]$RcaDevice = "auto",

    [switch]$SkipRca
)

$ErrorActionPreference = "Stop"

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptRoot
$pythonExe = "python"

$anomalyScript = Join-Path $scriptRoot "scripts\27_train_anomaly.py"
$rcaScript = Join-Path $scriptRoot "scripts\28_train_rca.py"

if (-not (Test-Path $anomalyScript)) {
    throw "Missing anomaly training script: $anomalyScript"
}

if (-not (Test-Path $rcaScript)) {
    throw "Missing RCA training script: $rcaScript"
}

switch ($Mode) {
    "balanced" {
        $optimizeFor = "anomaly"
        $anomalyOutputDir = Join-Path $AnomalyDataRoot "models\anomaly_gbrt_balanced"
    }
    "normal-priority" {
        $optimizeFor = "normal"
        $anomalyOutputDir = Join-Path $AnomalyDataRoot "models\anomaly_gbrt_normal_priority"
    }
    default {
        throw "Unsupported mode: $Mode"
    }
}

function Resolve-RcaDevice {
    param([string]$Preferred)
    if ($Preferred -ne "auto") {
        return $Preferred
    }
    try {
        $detected = & $pythonExe -c "import torch; print('cuda' if torch.cuda.is_available() else 'cpu')" 2>$null
        $detected = ($detected | Out-String).Trim().ToLower()
        if ($detected -in @("cuda", "cpu")) {
            return $detected
        }
    }
    catch {
    }
    return "cpu"
}

$resolvedRcaDevice = Resolve-RcaDevice -Preferred $RcaDevice
$rcaOutputDir = Join-Path $RcaDataRoot "models\rca_gat_like_$resolvedRcaDevice"

Write-Host "Training current best stack..." -ForegroundColor Cyan
Write-Host "Mode: $Mode" -ForegroundColor Yellow
Write-Host "Anomaly optimize-for: $optimizeFor" -ForegroundColor Yellow
Write-Host "RCA device: $resolvedRcaDevice" -ForegroundColor Yellow

Write-Host ""
Write-Host "==> Training anomaly model" -ForegroundColor Green
& $pythonExe $anomalyScript `
    --data-root $AnomalyDataRoot `
    --model-kind gbrt `
    --optimize-for $optimizeFor `
    --output-dir $anomalyOutputDir

if ($LASTEXITCODE -ne 0) {
    throw "Anomaly training failed with exit code $LASTEXITCODE"
}

if (-not $SkipRca) {
    Write-Host ""
    Write-Host "==> Training RCA model" -ForegroundColor Green
    & $pythonExe $rcaScript `
        --data-root $RcaDataRoot `
        --device $resolvedRcaDevice `
        --output-dir $rcaOutputDir

    if ($LASTEXITCODE -ne 0) {
        throw "RCA training failed with exit code $LASTEXITCODE"
    }
}

Write-Host ""
Write-Host "Done." -ForegroundColor Cyan
Write-Host "Anomaly artifacts: $anomalyOutputDir"
if (-not $SkipRca) {
    Write-Host "RCA artifacts: $rcaOutputDir"
}
