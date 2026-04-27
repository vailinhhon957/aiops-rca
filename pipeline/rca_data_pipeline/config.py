from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = REPO_ROOT.parent
WORKSPACE_ROOT = PROJECT_ROOT.parent
PIPELINE_ROOT = REPO_ROOT / "pipeline" / "rca_data_pipeline"

LEGACY_DATASET_ROOT = PROJECT_ROOT / "dataset"
if not LEGACY_DATASET_ROOT.exists():
    LEGACY_DATASET_ROOT = WORKSPACE_ROOT / "dataset"

DATA_ROOT = REPO_ROOT / "data"
RAW_ROOT = DATA_ROOT / "raw"
INTERIM_ROOT = DATA_ROOT / "interim"
PROCESSED_ROOT = DATA_ROOT / "processed"
SPLITS_ROOT = DATA_ROOT / "splits"

SPANS_ROOT = INTERIM_ROOT / "spans"
TRACE_TABLE_ROOT = INTERIM_ROOT / "traces"
ANOMALY_ROOT = PROCESSED_ROOT / "anomaly"
RCA_ROOT = PROCESSED_ROOT / "rca"
GRAPH_PAYLOAD_ROOT = RCA_ROOT / "graph_payloads"
GRAPH_TENSOR_ROOT = RCA_ROOT / "graph_tensors"

DEFAULT_WINDOW_ID = "window_0001"
DEFAULT_SYSTEM_ID = "online-boutique"
DEFAULT_SYSTEM_FAMILY = "ecommerce"
DEFAULT_TOPOLOGY_VERSION = "online-boutique-v0.10.5"
DEFAULT_SERVICE_CATALOG = PIPELINE_ROOT / "service_catalog_online_boutique.json"
RANDOM_SEED = 42

CORE_SERVICES = [
    "frontend",
    "checkoutservice",
    "currencyservice",
    "paymentservice",
    "productcatalogservice",
    "recommendationservice",
    "emailservice",
    "cartservice",
    "shippingservice",
    "adservice",
]

REQUIRED_METADATA_COLUMNS = [
    "run_id",
    "trace_file",
    "label",
    "fault_type",
    "root_cause_service",
]
