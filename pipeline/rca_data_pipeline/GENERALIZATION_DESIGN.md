# Cross-System Generalization Design

Tai lieu nay thiet ke lai schema du lieu va bai toan model de he thong RCA co the:

- hoc tren nhieu he microservice khac nhau
- giam phu thuoc vao ten service cu the
- giu duoc tinh thuc dung voi pipeline traces -> anomaly -> RCA -> recovery

No duoc viet de mo rong truc tiep tu pipeline hien tai trong thu muc `pipeline/rca_data_pipeline`.

## 1. Van de cua schema hien tai

Pipeline hien tai hoat dong tot cho mot he duy nhat, nhung kho tong quat vi:

- feature trace-level dang co cot theo ten service co dinh
  - `frontend_avg_ms`
  - `paymentservice_avg_ms`
  - `recommendationservice_avg_ms`
- nhan RCA dang la service name cua mot app cu the
- split train/test moi dung tot trong cung mot topology

He qua:

- model de hoc meo theo service name
- kho chuyen sang app microservice khac
- kho viet paper neu muon claim kha nang generalization

## 2. Muc tieu thiet ke moi

Schema moi can phuc vu 3 muc tieu:

1. Service-name-agnostic
- khong khoa model vao ten nhu `paymentservice`

2. Topology-aware
- van giu thong tin graph goi giua cac service

3. Multi-system ready
- mot pipeline co the nap du lieu tu nhieu he
  - `online-boutique`
  - `sock-shop`
  - `train-ticket`

## 3. Nguyen tac thiet ke

### 3.1 Tach metadata he thong khoi feature hoc may

- Ten he
- Ten service goc
- version deploy
- namespace

chi nen duoc luu de audit, khong nen dua truc tiep vao feature chinh.

### 3.2 Dung feature tuong doi hon la tuyet doi

Vi du:

- thay vi chi dung `avg_latency_ms = 120`
- them:
  - `latency_ratio_to_baseline`
  - `latency_zscore_within_run`
  - `request_share`
  - `error_rate_delta`

### 3.3 Dung nhan RCA theo node hien tai trong graph

Thay vi class co dinh:

- `checkoutservice`
- `cartservice`
- `recommendationservice`

ta doi thanh:

- `root_cause_node_index`
- `root_cause_role`
- `fault_family`

Khi do cung mot model co the du doan node loi trong graph cua nhieu he khac nhau.

## 4. Schema du lieu moi

## 4.1 Cau truc thu muc de xuat

```text
data_v2/
  raw/
    <system_id>/
      <run_id>/
        run_meta.json
        service_catalog.json
        events.jsonl
        windows/
          traces_0001.json
          traces_0002.json
  normalized/
    runs.parquet
    services.parquet
    spans.parquet
    windows.parquet
    traces.parquet
  processed/
    anomaly/
      window_features.parquet
      trace_features.parquet
      labels.parquet
    rca/
      graph_index.parquet
      graph_payloads/
      graph_tensors/
  splits/
    by_run/
    by_system/
    cross_system/
```

## 4.2 `run_meta.json`

```json
{
  "run_id": "ob_run_20260325_001",
  "system_id": "online-boutique",
  "system_family": "ecommerce",
  "topology_version": "ob_v0_10_5",
  "environment": "kubernetes-aws",
  "namespace": "app",
  "scenario_name": "payment_latency_1000ms",
  "is_fault": 1,
  "fault_family": "latency",
  "fault_type": "downstream_latency",
  "fault_target_service": "paymentservice",
  "fault_target_role": "payment",
  "load_profile": {
    "profile_name": "medium",
    "rps_target": 10,
    "concurrency": 20
  },
  "start_time": "2026-03-25T10:00:00Z",
  "end_time": "2026-03-25T10:10:00Z"
}
```

Cot moi quan trong:

- `system_id`
- `system_family`
- `topology_version`
- `fault_target_role`

`fault_target_role` la cau noi giua nhieu he thong khac nhau.

Vi du:

- `paymentservice` trong app A va `payment-api` trong app B deu map ve role `payment`

## 4.3 `service_catalog.json`

Moi run can co bang map service sang role chuan:

```json
[
  {
    "service_name": "frontend",
    "service_role": "entrypoint",
    "service_tier": "edge",
    "criticality": "high"
  },
  {
    "service_name": "paymentservice",
    "service_role": "payment",
    "service_tier": "backend",
    "criticality": "high"
  }
]
```

Day la thanh phan quan trong nhat de generalize.

Thay vi hoc truc tiep service name, model co the hoc theo:

- `service_role`
- `service_tier`
- `criticality`

## 4.4 `normalized/services.parquet`

Moi dong = mot service trong mot he thong

- `system_id`
- `topology_version`
- `service_name`
- `service_role`
- `service_tier`
- `criticality`
- `is_entrypoint`
- `is_stateful`

## 4.5 `normalized/spans.parquet`

Mo rong tu schema hien tai:

- `system_id`
- `run_id`
- `window_id`
- `trace_id`
- `span_id`
- `parent_span_id`
- `service_name`
- `service_role`
- `operation_name`
- `call_type`
- `start_time_ms`
- `end_time_ms`
- `duration_ms`
- `status_code`
- `is_error`

`call_type` co the la:

- `http`
- `grpc`
- `db`
- `cache`
- `mq`
- `internal`

## 4.6 `normalized/windows.parquet`

Moi dong = mot time window

- `system_id`
- `run_id`
- `window_id`
- `window_start_ms`
- `window_end_ms`
- `is_fault_window`
- `fault_family`
- `fault_target_service`
- `fault_target_role`
- `load_profile_name`

Dung bang nay de anomaly va RCA deu co anchor theo time window.

## 4.7 `processed/anomaly/window_features.parquet`

De generalize tot hon, anomaly nen hoc o muc `window`, khong chi trace.

Moi dong = mot window.

- `system_id`
- `run_id`
- `window_id`
- `trace_count`
- `service_count`
- `avg_trace_duration_ms`
- `p95_trace_duration_ms`
- `error_trace_ratio`
- `error_span_ratio`
- `request_fanout_mean`
- `critical_path_mean_ms`
- `latency_cv`
- `topology_change_score`
- `is_anomaly`

Feature nen la generic, khong co ten service co dinh.

## 4.8 `processed/anomaly/trace_features.parquet`

Van giu trace-level, nhung bo cot hard-code theo service name.

Feature de xuat:

- `system_id`
- `run_id`
- `window_id`
- `trace_id`
- `trace_duration_ms`
- `span_count`
- `service_count`
- `error_span_count`
- `error_service_count`
- `max_span_duration_ms`
- `mean_span_duration_ms`
- `std_span_duration_ms`
- `critical_path_est_ms`
- `fanout_count`
- `entrypoint_ratio`
- `backend_ratio`
- `stateful_service_ratio`
- `client_span_ratio`
- `server_span_ratio`
- `db_span_ratio`
- `cache_span_ratio`
- `latency_zscore`
- `duration_ratio_to_run_baseline`

Khong con:

- `frontend_avg_ms`
- `checkoutservice_avg_ms`
- ...

## 4.9 `processed/rca/graph_payload`

Moi graph = 1 window bat thuong.

### Node features de xuat

Moi node nen co:

- `avg_latency_ms`
- `p95_latency_ms`
- `error_rate`
- `request_count`
- `request_share`
- `in_degree`
- `out_degree`
- `betweenness_approx`
- `latency_ratio_to_node_baseline`
- `error_ratio_to_node_baseline`
- `tier_id`
- `criticality_id`
- `role_id`
- `is_entrypoint`
- `is_stateful`

### Edge features de xuat

- `call_count`
- `mean_edge_latency_ms`
- `error_rate`
- `edge_type_id`

### Labels

Khong nen chi luu `root_cause_service`.

Nen luu:

- `root_cause_node_index`
- `root_cause_service_name`
- `root_cause_role`
- `fault_family`

`root_cause_node_index` la nhan chinh cho node classification.

## 5. Bai toan model moi

He thong nen tach thanh 2 bai toan.

## 5.1 Stage 1: Anomaly Detection

### Input

- `window_features.parquet`
hoac
- graph embedding cua tung window

### Output

- `anomaly_score`
- `is_anomaly`

### Model goi y

Ban dau:

- `IsolationForest`
- `XGBoost`
- `MLP`

Nang cap:

- Temporal autoencoder
- Transformer/TCN tren chuoi window features

### Vi sao stage nay de generalize

Vi no hoc pattern bat thuong tong quat:

- tang latency
- tang error
- thay doi fanout
- thay doi topo goi

khong can biet app cu the la app nao.

## 5.2 Stage 2: RCA as Node Classification

### Input

- graph cua window bat thuong

### Output

- score tren tung node
- top-1 root cause node
- top-3 root cause node

### Model de xuat

`GATv2` hoac `GraphSAGE` node classifier.

#### Dau vao cua moi node

- metric dong
- topo graph
- role embedding
- tier embedding
- criticality embedding

#### Dau ra

- xac suat node la root cause

Day la cach bieu dien tot hon so voi multiclass classification theo ten service.

## 6. Cach de model generalize giua nhieu he

## 6.1 Khong dua service name raw vao embedding chinh

Service name van duoc luu de audit, nhung khong nen dung lam feature hoc may.

Neu can text feature, chi nen dung:

- hash bucket cua role
- role taxonomy
- tier taxonomy

## 6.2 Dung role taxonomy chung

Can tao bang map role dung cho nhieu he:

- `entrypoint`
- `checkout`
- `cart`
- `payment`
- `recommendation`
- `catalog`
- `currency`
- `shipping`
- `notification`
- `auth`
- `inventory`
- `database`
- `cache`

Khong phai he nao cung co du tat ca role, dieu do van on.

## 6.3 Dung relative normalization

Moi node nen co them baseline rieng:

- mean trong normal windows
- std trong normal windows

Feature suy bien:

- `latency_zscore`
- `error_rate_delta`
- `request_share_delta`

No giup model hoc "service nay bat thuong so voi chinh no", khong phai "service nay co 120ms".

## 6.4 Split dung cho paper

Can co 3 che do split:

### In-system

- train/test trong cung mot he

### Cross-scenario

- train tren mot so fault
- test tren fault unseen trong cung he

### Cross-system

- train tren `online-boutique`
- test tren `sock-shop`

Neu lam duoc cross-system la diem rat manh.

## 7. De xuat migration tu pipeline hien tai

Khong can bo toan bo pipeline cu.

### Buoc 1

Giu nguyen:

- parse spans
- clean spans
- build graph payload

### Buoc 2

Them:

- `system_id`
- `service_role`
- `service_tier`
- `criticality`

vao span va run metadata.

### Buoc 3

Loai bo feature hard-code theo ten service trong `trace_features`.

### Buoc 4

Them `window_features.parquet`.

### Buoc 5

Doi nhan RCA:

- tu `root_cause_service`
- sang `root_cause_node_index` + `root_cause_role`

## 8. Mapping de xuat cho Online Boutique

Bang nay la diem bat dau tot.

| service_name | service_role | service_tier | criticality |
| --- | --- | --- | --- |
| frontend | entrypoint | edge | high |
| checkoutservice | checkout | backend | high |
| cartservice | cart | backend | high |
| paymentservice | payment | backend | high |
| productcatalogservice | catalog | backend | high |
| recommendationservice | recommendation | backend | medium |
| currencyservice | currency | backend | medium |
| shippingservice | shipping | backend | medium |
| emailservice | notification | async | low |
| redis-cart | cache | stateful | high |

Neu doi sang app khac, ban chi can tao bang role mapping moi.

## 9. Thiet ke output JSON chuan cho inference

```json
{
  "system_id": "online-boutique",
  "run_id": "ob_run_20260325_001",
  "window_id": "window_0007",
  "anomaly_score": 0.93,
  "is_anomaly": 1,
  "root_cause_top1": {
    "node_index": 3,
    "service_name": "paymentservice",
    "service_role": "payment",
    "confidence": 0.81
  },
  "root_cause_top3": [
    {"node_index": 3, "service_role": "payment", "confidence": 0.81},
    {"node_index": 2, "service_role": "checkout", "confidence": 0.11},
    {"node_index": 5, "service_role": "catalog", "confidence": 0.05}
  ],
  "fault_family_pred": "latency",
  "recommended_action": "restart_pod"
}
```

JSON nay van phu hop voi decision/recovery module hien tai.

## 10. Lo trinh thuc hien de xuat

### Phase 1

Nang cap schema:

- them `system_id`
- them `service_catalog`
- them `service_role`

### Phase 2

Refactor feature engineering:

- bo cot service-specific
- them feature generic + relative

### Phase 3

Refactor RCA label:

- node classification
- them `root_cause_node_index`

### Phase 4

Them app thu hai:

- `sock-shop` hoac `train-ticket`

### Phase 5

Danh gia:

- in-system
- cross-scenario
- cross-system

## 11. Ket luan

Huong thiet ke moi nen duoc tom lai nhu sau:

- anomaly la bai toan generic tren window/trace features
- RCA la bai toan node classification tren service graph
- metadata can luu service name de audit, nhung model nen hoc tu role, tier, criticality va thong tin tuong doi
- nhan nen doi tu service-name class sang node-level class

Day la huong thiet ke phu hop hon neu ban muon he thong:

- dung duoc voi nhieu app microservice
- co claim generalization manh hon
- de nang cap thanh bai bao hon
