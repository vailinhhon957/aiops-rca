# README for Data Collection

Tai lieu nay la checklist thuc chien de thu trace va tao metadata dung format cho pipeline RCA.

No di cung cac template trong thu muc `templates/`.

## 1. Muc tieu cua moi buoi thu data

Moi buoi thu data nen chi nham vao:

- 1 load profile
- 1 nhom scenario
- 1 batch `run_id` ro rang

Vi du:

- buoi 1: `normal_low_load`, `normal_medium_load`
- buoi 2: `payment_latency_injection`
- buoi 3: `checkout_pod_kill`, `recommendation_pod_kill`

## 2. File ban can truoc khi bat dau

- `templates/scenario_catalog_online_boutique.csv`
- `templates/metadata_rich_template.csv`
- `templates/run_meta_template.json`
- `templates/events_template.jsonl`
- script `scripts/10_generate_metadata_rows.py`

## 3. Checklist truoc khi chay

1. Xac dinh scenario can thu.
2. Xac dinh so run moi scenario.
3. Xac dinh load profile:
- `low`
- `medium`
- `high`
4. Xac dinh split tag:
- `train`
- `val`
- `test`
5. Dam bao app dang on dinh.
6. Dam bao tracing dang bat.
7. Dam bao load generator dang san sang.
8. Dam bao co thu muc de luu Jaeger JSON export.

## 4. Tao skeleton metadata truoc khi chay

Tu repo root:

```powershell
python pipeline\rca_data_pipeline\scripts\10_generate_metadata_rows.py `
  --runs-per-scenario 5 `
  --split-tags train,val,test `
  --output pipeline\rca_data_pipeline\templates\generated_metadata_batch.csv
```

Script nay sinh san:

- `run_id`
- `trace_file`
- `fault_family`
- `fault_type`
- `fault_target_service`
- `fault_target_role`
- `split_tag`

Neu ban muon sinh mot batch manh hon, da dang hon cho anomaly + RCA, dung:

```powershell
python pipeline\rca_data_pipeline\scripts\16_generate_stronger_metadata_batch.py `
  --variant-profile strong `
  --train-runs 2 `
  --val-runs 1 `
  --test-runs 1 `
  --output pipeline\rca_data_pipeline\templates\stronger_metadata_batch.csv
```

Script nay tu sinh them:

- nhieu run moi scenario hon
- bien the `severity/load_profile`
- split can bang theo tung bien the
- tham so runtime moi run:
  - `warmup_seconds`
  - `cooldown_seconds`
  - `fault_duration_seconds`
  - `query_limit`
  - tham so injector nhu `cpu_limit_m`, `memory_limit_mib`, `latency_delay_seconds`

`11_collect_run.py` va `12_export_jaeger_run.py` da doc cac cot override nay, nen khi ban chay batch thi fault va trace export se da dang hon ma khong can sua tay tung lenh.

Sau khi rebuild voi pipeline moi, trace/window features cua cac run co fault se duoc tach thanh:

- `pre`
- `active`
- `recovery`

Mac dinh anomaly label se la:

- `active = 1` voi run `label = 1`
- `pre/recovery = 0`
- hard negative (`label = 0`) se giu `0` o moi phase

Neu ban muon bo sung them data `normal + hard negative`, dung:

```powershell
python pipeline\rca_data_pipeline\scripts\17_generate_normal_hard_negative_batch.py `
  --output pipeline\rca_data_pipeline\templates\normal_hard_negative_batch.csv
```

Ban chi can dien them:

- `start_time`
- `fault_start_time`
- `fault_end_time`
- `end_time`
- `target_pod`
- `target_container`
- cac thong so thong ke sau khi export trace

Neu ban muon auto-dien cac truong suy ra duoc an toan, dung:

```powershell
python pipeline\rca_data_pipeline\scripts\14_autofill_metadata_fields.py `
  --metadata-file pipeline\rca_data_pipeline\templates\batch1_fill_only.csv `
  --namespace default `
  --fill-target-pods
```

Script nay se:

- tu dien `target_container = fault_target_service` neu dang trong
- voi `pod-kill`, co the lay `target_pod` hien tai tu cluster neu ban bat `--fill-target-pods`

Script nay khong tu dien:

- `start_time`
- `fault_start_time`
- `fault_end_time`
- `end_time`

vi cac moc nay phai gan voi run that.

Neu ban muon dung mot file batch giong `13_run_batch_dataset.py`, dung:

```powershell
python pipeline\rca_data_pipeline\scripts\15_run_metadata_autofill_batch.py `
  --metadata-file pipeline\rca_data_pipeline\templates\batch1_fill_only.csv `
  --split-tags train `
  --show-missing
```

Script nay ho tro:

- chon theo `run-id`
- chon theo `split-tag`
- auto-dien metadata an toan cho ca nhom run
- in ra cac truong con thieu sau khi dien xong

## 4b. Thu ban tu dong mot run

Neu ban khong muon bam tay tung lenh, co the dung:

```powershell
python pipeline\rca_data_pipeline\scripts\11_collect_run.py `
  --run-id ob_kill_checkout_001 `
  --metadata-file pipeline\rca_data_pipeline\templates\batch1_fill_only.csv `
  --namespace default
```

Script nay hien ho tro tot cho:

- `none`
- `pod-kill`
- `replica-drop`
- `cpu-stress`
- `memory-stress`
- `latency-injection`
- `timeout`
- `http-500`

No se:

- tat built-in `loadgenerator`
- bat `simple-loadgen`
- warm-up
- ghi `start_time`
- inject fault
- ghi `fault_start_time`
- doi hoi phuc
- ghi `fault_end_time`
- cooldown
- ghi `end_time`
- cap nhat lai file CSV metadata

Hien tai cac fault moi duoc tao theo huong Kubernetes-native:

- `cpu-stress`: rollout voi CPU limit rat thap
- `memory-stress`: rollout voi memory limit rat thap
- `latency-injection`: doi selector cua service sang delay proxy pod
- `timeout`: doi selector cua service sang blackhole proxy pod
- `http-500`: rollout frontend voi env misconfiguration

## 4c. Export trace tu Jaeger theo run metadata

Sau khi `11_collect_run.py` chay xong, ban co the export trace bang:

```powershell
python pipeline\rca_data_pipeline\scripts\12_export_jaeger_run.py `
  --run-id ob_kill_checkout_001 `
  --metadata-file pipeline\rca_data_pipeline\templates\batch1_fill_only.csv `
  --jaeger-url http://127.0.0.1:16686
```

Script nay se:

- doc `start_time` va `end_time` cua run
- query Jaeger API theo `source_service`
- luu JSON ra `dataset/raw/collected/<trace_file>`
- cap nhat them cac thong so:
  - `trace_count`
  - `span_count_total`
  - `avg_spans_per_trace`
  - `unique_service_count`
  - `unique_services`

Dieu kien:

- Jaeger API phai truy cap duoc o `http://127.0.0.1:16686`
- run do da co `start_time` va `end_time`

## 4d. Chay ca batch bang mot lenh

Neu ban muon gom:

- collect run
- export Jaeger
- rebuild dataset

thanh mot entrypoint duy nhat, dung:

```powershell
python pipeline\rca_data_pipeline\scripts\13_run_batch_dataset.py `
  --metadata-file pipeline\rca_data_pipeline\templates\batch1_fill_only.csv `
  --namespace default `
  --jaeger-url http://127.0.0.1:16686 `
  --mode all
```

Hoac tren Windows dung wrapper:

```powershell
.\pipeline\rca_data_pipeline\run_batch1_supported.ps1
```

Script nay hien tai chi tu dong collect duoc cac fault type da co injector:

- `none`
- `pod-kill`
- `replica-drop`
- `cpu-stress`
- `memory-stress`
- `latency-injection`
- `timeout`
- `http-500`

Neu ban chi muon chay mot phan cua batch:

```powershell
python pipeline\rca_data_pipeline\scripts\13_run_batch_dataset.py `
  --metadata-file pipeline\rca_data_pipeline\templates\batch1_fill_only.csv `
  --namespace default `
  --jaeger-url http://127.0.0.1:16686 `
  --mode all `
  --run-ids ob_norm_low_001,ob_norm_mid_001,ob_kill_checkout_001
```

Neu ban chi muon rebuild dataset tu trace da co:

```powershell
python pipeline\rca_data_pipeline\scripts\13_run_batch_dataset.py `
  --metadata-file pipeline\rca_data_pipeline\templates\batch1_fill_only.csv `
  --mode rebuild-only
```

Neu ban muon rebuild sach hoan toan, xoa local dataset roots truoc khi build:

```powershell
python pipeline\rca_data_pipeline\scripts\13_run_batch_dataset.py `
  --metadata-file pipeline\rca_data_pipeline\templates\batch1_fill_only.csv `
  --mode rebuild-only `
  --clean
```

`--clean` chi xoa cac thu muc local cua pipeline:

- `data/raw`
- `data/interim`
- `data/processed`
- `data/splits`

Neu ban muon vua collect run moi vua rebuild sach trong mot lan:

```powershell
python pipeline\rca_data_pipeline\scripts\13_run_batch_dataset.py `
  --metadata-file pipeline\rca_data_pipeline\templates\batch1_fill_only.csv `
  --namespace default `
  --jaeger-url http://127.0.0.1:16686 `
  --mode all `
  --run-ids ob_lat_pay_001 `
  --clean
```

## 5. Checklist trong luc chay 1 run

1. Bat load generator.
2. Cho warm-up 30-60 giay.
3. Ghi `start_time`.
4. Bat fault injection.
5. Ghi `fault_start_time`.
6. Giu fault trong 2-5 phut.
7. Tat fault.
8. Ghi `fault_end_time`.
9. Thu load them 30-60 giay neu can.
10. Ghi `end_time`.
11. Export trace JSON tu Jaeger.
12. Dat ten file trung voi `trace_file`.

## 6. Quy uoc dat ten run

Nen dung:

- `<scenario_id>_<index>`

Vi du:

- `ob_lat_pay_001`
- `ob_lat_pay_002`
- `ob_kill_checkout_001`

Neu can severity trong ten:

- `ob_lat_pay_low_001`
- `ob_lat_pay_med_001`
- `ob_lat_pay_high_001`

## 7. Checklist sau khi ket thuc mot buoi thu

1. Kiem tra moi `trace_file` deu ton tai.
2. Kiem tra moi `run_id` la duy nhat.
3. Kiem tra `label`, `fault_family`, `fault_type` dung voi scenario.
4. Kiem tra `fault_target_service` va `root_cause_service` khop nhau.
5. Kiem tra `split_tag` da phan bo hop ly.
6. Kiem tra `start_time <= fault_start_time <= fault_end_time <= end_time`.
7. Kiem tra note day du cho run loi.

## 8. Merge vao metadata chinh

Sau khi dien xong file CSV skeleton:

1. append no vao file metadata ban dang quan ly
2. dat cac Jaeger JSON vao thu muc raw phu hop
3. chay lai pipeline:

```powershell
python pipeline\rca_data_pipeline\scripts\02_import_legacy_dataset.py
python pipeline\rca_data_pipeline\scripts\03_parse_traces.py
python pipeline\rca_data_pipeline\scripts\04_clean_spans.py
python pipeline\rca_data_pipeline\scripts\05_build_trace_features.py
python pipeline\rca_data_pipeline\scripts\05b_build_window_features.py
python pipeline\rca_data_pipeline\scripts\06_label_anomaly.py
python pipeline\rca_data_pipeline\scripts\07_build_service_graphs.py
python pipeline\rca_data_pipeline\scripts\08_export_graph_dataset.py
python pipeline\rca_data_pipeline\scripts\09_make_splits.py
```

## 9. Loi thuong gap

- 1 run co nhieu fault chong nhau
- ten `trace_file` khong khop `run_id`
- quen ghi `fault_start_time` va `fault_end_time`
- `root_cause_service` khong khop `fault_target_service`
- split tag mat can bang

## 10. Khuyen nghi thuc te

- Thu normal truoc.
- Thu tung fault rieng.
- Moi severity thu thanh 3-5 run.
- Giu notes ngan gon nhung ro ly do va thong so fault.
