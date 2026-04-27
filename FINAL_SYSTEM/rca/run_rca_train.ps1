$ErrorActionPreference = "Stop"
$python = "D:\doanchuyennganh_aiops\doan_env\Scripts\python.exe"

Write-Host "Training RCA final model..." -ForegroundColor Cyan
& $python .\tools\train_gat_rca.py `
  --graph-path .\dataset\graph_dataset_final.json `
  --metadata-path .\dataset\graph_metadata_final.json `
  --train-runs-path .\dataset\train_runs_final.txt `
  --val-runs-path .\dataset\val_runs_final.txt `
  --test-runs-path .\dataset\test_runs_final.txt `
  --output-dir .\artifacts\rca_final `
  --edge-drop-rate 0.05 `
  --lr 0.0005 `
  --dropout 0.20 `
  --hidden-dim 96 `
  --heads 4
Write-Host "Done." -ForegroundColor Green
