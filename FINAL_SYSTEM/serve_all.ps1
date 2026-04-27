$ErrorActionPreference = "Stop"
$python = "D:\doanchuyennganh_aiops\doan_env\Scripts\python.exe"

Write-Host "Starting Anomaly API on port 8000..." -ForegroundColor Cyan
Start-Process -NoNewWindow -FilePath $python -ArgumentList "tools\serve_anomaly_api.py --artifact-dir .\artifacts\anomaly_final --host 127.0.0.1 --port 8000"

Start-Sleep -Seconds 2

Write-Host "Starting RCA API on port 8100..." -ForegroundColor Cyan
Start-Process -NoNewWindow -FilePath $python -ArgumentList "rca\tools\serve_gat_rca_api.py --artifact-dir .\rca\artifacts\rca_final --graph-path .\rca\dataset\graph_dataset_final.json --metadata-path .\rca\dataset\graph_metadata_final.json --host 127.0.0.1 --port 8100"

Write-Host "Both APIs starting. Health check in 5s..." -ForegroundColor Green
Start-Sleep -Seconds 5
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8100/health
