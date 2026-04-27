# FINAL_SYSTEM

Bo final da duoc don gon va chot theo cau hinh:
- `Anomaly = anomaly_final`
- `RCA = rca_final`

## Thu muc dang dung

### Anomaly
- dataset: `dataset/anomaly_final.csv`
- artifact: `artifacts/anomaly_final/`
- train: `tools/run_anomaly_train.ps1`
- serve: `tools/serve_anomaly_api.py`

### RCA
- dataset: `rca/dataset/graph_dataset_final.json`
- metadata: `rca/dataset/graph_metadata_final.json`
- splits:
  - `rca/dataset/train_runs_final.txt`
  - `rca/dataset/val_runs_final.txt`
  - `rca/dataset/test_runs_final.txt`
- artifact: `rca/artifacts/rca_final/`
- train: `rca/run_rca_train.ps1`
- serve: `rca/tools/serve_gat_rca_api.py`

## Lenh dung hang ngay

### Train anomaly
```powershell
cd D:\doanchuyennganh_aiops\FINAL_SYSTEM
.\tools\run_anomaly_train.ps1
```

### Train RCA
```powershell
cd D:\doanchuyennganh_aiops\FINAL_SYSTEM\rca
.\run_rca_train.ps1
```

### Chay ca hai API
```powershell
cd D:\doanchuyennganh_aiops\FINAL_SYSTEM
.\serve_all.ps1
```

## Metric final

### Anomaly
- balanced_accuracy: `0.7143`
- specificity: `0.7760`
- recall_anomaly: `0.6525`
- f1_anomaly: `0.6774`
- roc_auc: `0.7994`

### RCA
- top1_acc: `0.7500`
- top3_acc: `0.8750`
- mrr: `0.8326`

## Ghi chu
- Anomaly final la HGB balanced tren bo Data3.4.
- RCA final la GAT tuned tren bo du lieu RCA da chot.

