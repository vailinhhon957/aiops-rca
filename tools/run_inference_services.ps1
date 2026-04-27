param(
    [string]$AnomalyArtifactDir = "D:\HOCTAP\2025-2026\HK2\DACN\microservices-demo\data_anomaly_balanced_v3\models\anomaly_gbrt_balanced",
    [string]$RcaArtifactDir = "D:\HOCTAP\2025-2026\HK2\DACN\microservices-demo\data_rca_balanced_v3\models\rca_gat_like_cuda",
    [ValidateSet("cpu", "cuda")]
    [string]$RcaDevice = "cuda"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$anomalyScript = Join-Path $PSScriptRoot "run_anomaly_service.ps1"
$rcaScript = Join-Path $PSScriptRoot "run_rca_service.ps1"

if (-not (Test-Path $anomalyScript)) {
    throw "Missing anomaly runner: $anomalyScript"
}
if (-not (Test-Path $rcaScript)) {
    throw "Missing RCA runner: $rcaScript"
}

Write-Host "Opening anomaly and RCA service windows..." -ForegroundColor Cyan

$anomalyArgs = @(
    "-NoExit",
    "-ExecutionPolicy", "Bypass",
    "-File", $anomalyScript,
    "-ArtifactDir", $AnomalyArtifactDir
)

$rcaArgs = @(
    "-NoExit",
    "-ExecutionPolicy", "Bypass",
    "-File", $rcaScript,
    "-ArtifactDir", $RcaArtifactDir,
    "-Device", $RcaDevice
)

Start-Process powershell -WorkingDirectory $repoRoot -ArgumentList $anomalyArgs | Out-Null
Start-Process powershell -WorkingDirectory $repoRoot -ArgumentList $rcaArgs | Out-Null

Write-Host "Anomaly service target: http://127.0.0.1:8000" -ForegroundColor Yellow
Write-Host "RCA service target: http://127.0.0.1:8001" -ForegroundColor Yellow
