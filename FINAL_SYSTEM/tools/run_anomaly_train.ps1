$ErrorActionPreference = "Stop"
$python = "D:\doanchuyennganh_aiops\doan_env\Scripts\python.exe"

Write-Host "Training anomaly final model..." -ForegroundColor Cyan
& $python .\tools\train_supervised_anomaly.py `
  --csv-path .\dataset\anomaly_final.csv `
  --output-dir .\artifacts\anomaly_final `
  --model-type hgb `
  --max-depth 6 `
  --learning-rate 0.02 `
  --max-iter 1200 `
  --min-samples-leaf 5 `
  --l2-regularization 0.2 `
  --class-weight balanced
Write-Host "Done." -ForegroundColor Green
