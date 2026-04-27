# Fault Scenario Catalog

Tai lieu nay mo ta bo nhan va fault scenarios mo rong de thu them du lieu cho:

- anomaly detection
- RCA node classification

No duoc thiet ke cho `online-boutique` truoc, nhung co the map sang he microservice khac thong qua:

- `system_id`
- `fault_family`
- `fault_type`
- `fault_target_role`

## 1. Muc tieu dataset mo rong

Dataset moi nen dat muc tieu toi thieu:

- 8-12 fault scenarios
- 5-10 run moi scenario
- 3 muc do severity cho fault quan trong
- 10-20 run normal lam baseline

## 2. Taxonomy nhan de xuat

### 2.1 Label cap run

- `label`
  - `0`: normal
  - `1`: anomalous
- `fault_family`
  - `none`
  - `resource`
  - `latency`
  - `availability`
  - `application`
  - `deployment`
- `fault_type`
  - `none`
  - `cpu-stress`
  - `memory-stress`
  - `latency-injection`
  - `timeout`
  - `pod-kill`
  - `replica-drop`
  - `grpc-error`
  - `http-500`
  - `bad-config`
  - `bad-rollout`
- `fault_target_service`
- `fault_target_role`
- `severity`
  - `low`
  - `medium`
  - `high`

### 2.2 Label cap graph/window

- `is_anomaly`
- `fault_family`
- `fault_target_service`
- `fault_target_role`
- `root_cause_node_index`

## 3. Scenario de xuat cho Online Boutique

### 3.1 Normal baselines

1. `normal_low_load`
- fault_family: `none`
- target: `none`
- muc tieu: baseline latency va topology o tai thap

2. `normal_medium_load`
- fault_family: `none`
- target: `none`
- muc tieu: baseline o tai vua

3. `normal_high_load`
- fault_family: `none`
- target: `none`
- muc tieu: tach anomaly do fault voi latency do tai cao

### 3.2 Resource faults

4. `recommendation_cpu_stress`
- fault_family: `resource`
- fault_type: `cpu-stress`
- target_service: `recommendationservice`
- target_role: `recommendation`
- severity: `low|medium|high`

5. `cart_memory_stress`
- fault_family: `resource`
- fault_type: `memory-stress`
- target_service: `cartservice`
- target_role: `cart`
- severity: `low|medium|high`

### 3.3 Latency faults

6. `payment_latency_injection`
- fault_family: `latency`
- fault_type: `latency-injection`
- target_service: `paymentservice`
- target_role: `payment`
- severity:
  - low: `200ms`
  - medium: `500ms`
  - high: `1000ms`

7. `catalog_latency_injection`
- fault_family: `latency`
- fault_type: `latency-injection`
- target_service: `productcatalogservice`
- target_role: `catalog`
- severity:
  - low: `100ms`
  - medium: `300ms`
  - high: `700ms`

8. `currency_timeout`
- fault_family: `latency`
- fault_type: `timeout`
- target_service: `currencyservice`
- target_role: `currency`
- severity:
  - low: timeout rate 10%
  - medium: timeout rate 30%
  - high: timeout rate 60%

### 3.4 Availability faults

9. `checkout_pod_kill`
- fault_family: `availability`
- fault_type: `pod-kill`
- target_service: `checkoutservice`
- target_role: `checkout`

10. `recommendation_pod_kill`
- fault_family: `availability`
- fault_type: `pod-kill`
- target_service: `recommendationservice`
- target_role: `recommendation`

11. `catalog_replica_drop`
- fault_family: `availability`
- fault_type: `replica-drop`
- target_service: `productcatalogservice`
- target_role: `catalog`
- severity:
  - low: replicas -> 2 to 1
  - medium: replicas -> 2 to 0 trong 20s
  - high: replicas -> 0 trong 60s

### 3.5 Application / deployment faults

12. `payment_grpc_error`
- fault_family: `application`
- fault_type: `grpc-error`
- target_service: `paymentservice`
- target_role: `payment`

13. `frontend_http_500`
- fault_family: `application`
- fault_type: `http-500`
- target_service: `frontend`
- target_role: `entrypoint`

14. `frontend_bad_config`
- fault_family: `deployment`
- fault_type: `bad-config`
- target_service: `frontend`
- target_role: `entrypoint`

15. `payment_bad_rollout`
- fault_family: `deployment`
- fault_type: `bad-rollout`
- target_service: `paymentservice`
- target_role: `payment`

## 4. Khuyen nghi so run moi scenario

Muc tieu toi thieu:

- normal: 15-20 run
- moi scenario fault: 5 run
- moi severity level: 3-5 run

Muc tieu dep hon cho paper:

- 20 normal run
- 8-10 fault scenarios
- 5-10 run moi scenario
- it nhat 50-80 fault runs

## 5. Nguyen tac thu du lieu

1. Moi run chi co 1 fault chinh.
2. Moi run chi co 1 target service chinh.
3. Ghi ro `fault_start_time` va `fault_end_time`.
4. Khong chong nhieu fault trong cung run.
5. Giu load profile nhat quan.
6. Co baseline normal cung load level.

## 6. Metadata can thu cho moi run

Moi run can co:

- `run_id`
- `system_id`
- `scenario_name`
- `label`
- `fault_family`
- `fault_type`
- `fault_target_service`
- `fault_target_role`
- `severity`
- `source_service`
- `load_profile`
- `start_time`
- `fault_start_time`
- `fault_end_time`
- `end_time`
- `trace_file`
- `notes`

## 7. Quy trinh thu du lieu de xuat

1. Chay `normal_low_load`, `normal_medium_load`, `normal_high_load`.
2. Chon 1 scenario fault.
3. Chay load on dinh truoc 1-2 phut.
4. Bat fault trong 2-5 phut.
5. Thu trace toan bo giai doan.
6. Export Jaeger JSON.
7. Dien metadata vao CSV.
8. Chay pipeline import/parse/build features.

## 8. File template di kem

Trong thu muc `templates/` co:

- `scenario_catalog_online_boutique.csv`
- `metadata_rich_template.csv`
- `run_meta_template.json`
- `scenario_label_template.json`
- `events_template.jsonl`

Ban nen copy cac file do thanh file moi cho tung dot thu nghiem.
