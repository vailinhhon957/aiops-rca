param(
    [string]$PythonExe = "python",
    [string]$AnomalyMetadata = "",
    [string]$RcaMetadata = "",
    [string]$AnomalySnapshot = "",
    [string]$RcaSnapshot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent (Split-Path -Parent $ScriptDir)

if (-not $AnomalyMetadata) {
    $AnomalyMetadata = Join-Path $RepoRoot "pipeline\rca_data_pipeline\templates\balanced_manifests\v2\anomaly_balanced_v2_metadata.csv"
}
if (-not $RcaMetadata) {
    $RcaMetadata = Join-Path $RepoRoot "pipeline\rca_data_pipeline\templates\balanced_manifests\v2\rca_balanced_v2_metadata.csv"
}
if (-not $AnomalySnapshot) {
    $AnomalySnapshot = Join-Path $RepoRoot "data_anomaly_balanced_v2"
}
if (-not $RcaSnapshot) {
    $RcaSnapshot = Join-Path $RepoRoot "data_rca_balanced_v2"
}

$BatchScript = Join-Path $RepoRoot "pipeline\rca_data_pipeline\scripts\13_run_batch_dataset.py"
$WorkingDataRoot = Join-Path $RepoRoot "data"

function Remove-TreeRobust {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PathToRemove
    )

    if (-not (Test-Path $PathToRemove)) {
        return
    }

    Get-ChildItem -LiteralPath $PathToRemove -Recurse -Force -ErrorAction SilentlyContinue | ForEach-Object {
        try {
            $_.IsReadOnly = $false
        } catch {
        }
    }

    Remove-Item -LiteralPath $PathToRemove -Recurse -Force -ErrorAction Stop
}

function Invoke-RebuildAndSnapshot {
    param(
        [string]$Label,
        [string]$MetadataFile,
        [string]$SnapshotDir
    )

    Write-Host ""
    Write-Host "=== Rebuild $Label ===" -ForegroundColor Cyan
    Write-Host "metadata: $MetadataFile"

    & $PythonExe $BatchScript `
        --metadata-file $MetadataFile `
        --mode rebuild-only `
        --clean

    if (-not (Test-Path $WorkingDataRoot)) {
        throw "Working data directory not found: $WorkingDataRoot"
    }

    if (Test-Path $SnapshotDir) {
        Remove-TreeRobust -PathToRemove $SnapshotDir
    }

    Copy-Item -LiteralPath $WorkingDataRoot -Destination $SnapshotDir -Recurse -Force
    Write-Host "saved snapshot: $SnapshotDir" -ForegroundColor Green
}

Invoke-RebuildAndSnapshot -Label "anomaly balanced v2" -MetadataFile $AnomalyMetadata -SnapshotDir $AnomalySnapshot
Invoke-RebuildAndSnapshot -Label "RCA balanced v2" -MetadataFile $RcaMetadata -SnapshotDir $RcaSnapshot

Write-Host ""
Write-Host "Done." -ForegroundColor Green
Write-Host "Anomaly v2 snapshot: $AnomalySnapshot"
Write-Host "RCA v2 snapshot:     $RcaSnapshot"
Write-Host "Current working data/ now matches the last rebuild (RCA v2)."
