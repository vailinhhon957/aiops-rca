param(
    [string]$ArtifactDir = "D:\HOCTAP\2025-2026\HK2\DACN\microservices-demo\data_rca_balanced_v3\models\rca_gat_like_cuda",
    [ValidateSet("cpu", "cuda")]
    [string]$Device = "cuda",
    [string]$BindHost = "0.0.0.0",
    [int]$Port = 8001
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path $ArtifactDir)) {
    throw "Missing RCA artifact directory: $ArtifactDir"
}

Set-Location $repoRoot
$env:AIOPS_RCA_ARTIFACT_DIR = $ArtifactDir
$env:AIOPS_RCA_DEVICE = $Device

Write-Host "Starting RCA inference service..." -ForegroundColor Cyan
Write-Host "Repo root: $repoRoot" -ForegroundColor Yellow
Write-Host "Artifact dir: $ArtifactDir" -ForegroundColor Yellow
Write-Host "Device: $Device" -ForegroundColor Yellow
Write-Host "URL: http://127.0.0.1:$Port" -ForegroundColor Yellow

python -m uvicorn aiops_framework.inference.rca_service.app:app --host $BindHost --port $Port
