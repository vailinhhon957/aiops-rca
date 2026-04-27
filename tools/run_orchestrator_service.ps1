param(
    [string]$AnomalyBaseUrl = "http://127.0.0.1:8000",
    [string]$RcaBaseUrl = "http://127.0.0.1:8001",
    [string]$BindHost = "0.0.0.0",
    [int]$Port = 8002
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
$env:AIOPS_ANOMALY_BASE_URL = $AnomalyBaseUrl
$env:AIOPS_RCA_BASE_URL = $RcaBaseUrl

Write-Host "Starting inference orchestrator..." -ForegroundColor Cyan
Write-Host "Repo root: $repoRoot" -ForegroundColor Yellow
Write-Host "Anomaly URL: $AnomalyBaseUrl" -ForegroundColor Yellow
Write-Host "RCA URL: $RcaBaseUrl" -ForegroundColor Yellow
Write-Host "Orchestrator URL: http://127.0.0.1:$Port" -ForegroundColor Yellow

python -m uvicorn aiops_framework.inference.orchestrator.app:app --host $BindHost --port $Port
