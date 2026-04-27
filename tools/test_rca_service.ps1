param(
    [string]$BaseUrl = "http://127.0.0.1:8001",
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
    "graph_id": str(obj["graph_id"]),
    "node_names": list(obj["node_names"]),
    "node_roles": list(obj.get("node_roles", [])),
    "x": obj["x"].tolist(),
    "edge_index": edge_rows,
    "top_k": 3,
    "metadata": {"run_id": str(obj["run_id"])},
}
print(json.dumps(payload, ensure_ascii=False))
"@

$body = $script | python -
Invoke-RestMethod -Uri "$BaseUrl/predict/graph" -Method Post -ContentType "application/json" -Body $body
