$ErrorActionPreference = "Stop"

Write-Host "Step 1: Converting Data29.3new RCA graphs -> JSON dataset..." -ForegroundColor Cyan
D:\doanchuyennganh_aiops\doan_env\Scripts\python.exe .\tools\convert_data29new_rca_to_graph_json.py

Write-Host "Step 2: Training GAT RCA on Data29.3new..." -ForegroundColor Cyan
D:\doanchuyennganh_aiops\doan_env\Scripts\python.exe .\tools\train_gat_rca.py `
  --graph-path .\dataset\graph_dataset_data29new.json `
  --metadata-path .\dataset\graph_metadata_data29new.json `
  --train-runs-path .\dataset\data29new_train_runs.txt `
  --val-runs-path .\dataset\data29new_val_runs.txt `
  --test-runs-path .\dataset\data29new_test_runs.txt `
  --output-dir .\artifacts\gat_rca_data29new `
  --edge-drop-rate 0.05

Write-Host "Done." -ForegroundColor Green
