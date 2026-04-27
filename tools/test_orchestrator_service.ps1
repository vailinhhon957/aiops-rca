param(
    [string]$BaseUrl = "http://127.0.0.1:8002",
    [string]$GraphTensorPath = "D:\HOCTAP\2025-2026\HK2\DACN\microservices-demo\data_rca_balanced_v3\processed\rca\graph_tensors\ob_cpu_reco_light_001__traces_0001__active.pt"
)

$script = @"
import json
from pathlib import Path
import torch

pt = Path(r"$GraphTensorPath")
obj = torch.load(pt, map_location="cpu")
edge_rows = obj["edge_index"].t().tolist() if obj["edge_index"].numel() > 0 else []

payload = {
    "window": {
        "features": {
            "trace_count": 100,
            "service_count": 6,
            "service_role_count": 5,
            "avg_trace_duration_ms": 120,
            "p95_trace_duration_ms": 350,
            "max_trace_duration_ms": 800,
            "error_trace_ratio": 0.01,
            "error_span_ratio": 0.001,
            "request_fanout_mean": 4,
            "critical_path_mean_ms": 110,
            "latency_cv": 1.2,
            "mean_span_count": 18,
            "call_type_diversity": 3,
            "entrypoint_trace_ratio": 1,
            "stateful_trace_ratio": 0
        },
        "metadata": {"run_id": "demo_run", "window_id": "demo_window"}
    },
    "graph": {
        "graph_id": str(obj["graph_id"]),
        "node_names": list(obj["node_names"]),
        "node_roles": list(obj.get("node_roles", [])),
        "x": obj["x"].tolist(),
        "edge_index": edge_rows,
        "top_k": 3,
        "metadata": {"run_id": str(obj["run_id"])}
    }
}
print(json.dumps(payload, ensure_ascii=False))
"@

$body = $script | python -
Invoke-RestMethod -Uri "$BaseUrl/analyze/pipeline" -Method Post -ContentType "application/json" -Body $body
