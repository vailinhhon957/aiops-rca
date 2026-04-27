# RCA Data Pipeline

Bo script nay dung de chuan hoa trace Jaeger thanh dataset cho:

- anomaly detection
- root cause analysis (RCA) o muc service graph

Pipeline ho tro hai luong:

- Tao run moi cho thu nghiem sau nay
- Import dataset cu tu `D:\HOCTAP\2025-2026\HK2\DACN\dataset`

## Tai lieu thiet ke mo rong

- Xem `GENERALIZATION_DESIGN.md` neu ban muon nang cap schema/model de tong quat hoa sang nhieu he microservice.
- Xem `FAULT_SCENARIO_CATALOG.md` neu ban muon thiet ke fault scenarios moi va thu them data co nhan sach.
- Xem `README_for_data_collection.md` neu ban muon checklist thuc chien cho tung buoi thu data.

## Cau truc du lieu dau ra

```text
data/
  raw/
    <run_id>/
      run_meta.json
      events.jsonl
      windows/
        traces_0001.json
  interim/
    spans/
      spans_<run_id>.parquet|csv
    traces/
      trace_table_<run_id>.parquet|csv
  processed/
    anomaly/
      trace_features.parquet|csv
      window_features.parquet|csv
      trace_features_labeled.parquet|csv
      window_features_labeled.parquet|csv
      trace_labels.parquet|csv
      window_labels.parquet|csv
    rca/
      graph_index.parquet|csv
      graph_payloads/
        graph_<graph_id>.json
      graph_tensors/
        graph_<graph_id>.pt
  splits/
    train_runs.txt
    val_runs.txt
    test_runs.txt
```

## Schema chinh

### `run_meta.json`

```json
{
  "run_id": "run_20260325_001",
  "scenario_name": "payment_latency_1000ms",
  "trace_file": "payment_latency_1000ms.json",
  "label": 1,
  "fault_type": "latency",
  "root_cause_service": "paymentservice",
  "source_service": "frontend",
  "start_time": "2026-03-25T10:00:00Z",
  "end_time": "2026-03-25T10:15:00Z",
  "notes": "fault active"
}
```

### `spans_<run_id>`

- `run_id`
- `window_id`
- `trace_id`
- `span_id`
- `parent_span_id`
- `service_name`
- `operation_name`
- `start_time_us`
- `duration_us`
- `start_time_ms`
- `end_time_ms`
- `duration_ms`
- `status_code`
- `span_kind`
- `is_error`

### `trace_features`

- `system_id`
- `run_id`
- `window_id`
- `trace_id`
- `trace_duration_ms`
- `span_count`
- `service_count`
- `service_role_count`
- `error_span_count`
- `error_service_count`
- `max_span_duration_ms`
- `mean_span_duration_ms`
- `std_span_duration_ms`
- `critical_path_est_ms`
- `fanout_count`
- `entrypoint_ratio`
- `stateful_service_ratio`
- `edge_span_ratio`
- `backend_span_ratio`
- `async_span_ratio`
- `stateful_span_ratio`
- `client_span_ratio`
- `server_span_ratio`
- `grpc_span_ratio`
- `http_span_ratio`
- `db_span_ratio`
- `cache_span_ratio`
- `mq_span_ratio`
- `internal_span_ratio`
- `latency_zscore`
- `duration_ratio_to_run_baseline`
- `is_anomaly`

### `window_features`

- `system_id`
- `run_id`
- `window_id`
- `trace_count`
- `service_count`
- `service_role_count`
- `avg_trace_duration_ms`
- `p95_trace_duration_ms`
- `max_trace_duration_ms`
- `error_trace_ratio`
- `error_span_ratio`
- `request_fanout_mean`
- `critical_path_mean_ms`
- `latency_cv`
- `mean_span_count`
- `call_type_diversity`
- `entrypoint_trace_ratio`
- `stateful_trace_ratio`
- `is_anomaly`

### `graph_index`

- `graph_id`
- `run_id`
- `window_id`
- `root_cause_service`
- `num_nodes`
- `num_edges`
- `node_feature_names`
- `graph_path`

## Thu tu chay script

1. `01_start_run.py`
2. `02_import_legacy_dataset.py`
3. `03_parse_traces.py`
4. `04_clean_spans.py`
5. `05_build_trace_features.py`
6. `05b_build_window_features.py`
7. `06_label_anomaly.py`
8. `07_build_service_graphs.py`
9. `08_export_graph_dataset.py`
10. `09_make_splits.py`

## Goi y chay nhanh voi dataset hien tai

```bash
python pipeline/rca_data_pipeline/scripts/02_import_legacy_dataset.py
python pipeline/rca_data_pipeline/scripts/03_parse_traces.py
python pipeline/rca_data_pipeline/scripts/04_clean_spans.py
python pipeline/rca_data_pipeline/scripts/05_build_trace_features.py
python pipeline/rca_data_pipeline/scripts/05b_build_window_features.py
python pipeline/rca_data_pipeline/scripts/06_label_anomaly.py
python pipeline/rca_data_pipeline/scripts/07_build_service_graphs.py
python pipeline/rca_data_pipeline/scripts/08_export_graph_dataset.py
python pipeline/rca_data_pipeline/scripts/09_make_splits.py
```

## Ghi chu

- Neu may chua co `pyarrow`, script se tu dong fallback sang `csv`.
- Neu may chua co `torch`, `08_export_graph_dataset.py` se xuat `json` va bo qua file `.pt`.
- Thu muc `templates/` chua san cac file mau de tao metadata cho dot thu du lieu moi.
- Script `10_generate_metadata_rows.py` co the sinh san CSV metadata skeleton tu scenario catalog.
