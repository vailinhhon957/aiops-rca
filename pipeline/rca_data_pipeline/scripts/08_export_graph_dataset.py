from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.rca_data_pipeline.config import GRAPH_PAYLOAD_ROOT, GRAPH_TENSOR_ROOT
from pipeline.rca_data_pipeline.io_utils import ensure_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Export graph payloads to torch tensors when torch is available.")
    parser.add_argument("--graph-root", type=Path, default=GRAPH_PAYLOAD_ROOT)
    args = parser.parse_args()

    try:
        import torch
    except Exception:
        print("torch is not installed. Graph payload JSON files are ready; skip .pt export.")
        return

    exported = 0
    ensure_dir(GRAPH_TENSOR_ROOT)
    for graph_file in sorted(args.graph_root.glob("graph_*.json")):
        payload = json.loads(graph_file.read_text(encoding="utf-8"))
        x = torch.tensor([node["features"] for node in payload["nodes"]], dtype=torch.float32)
        if payload["edge_index"]:
            edge_index = torch.tensor(payload["edge_index"], dtype=torch.long).t().contiguous()
        else:
            edge_index = torch.empty((2, 0), dtype=torch.long)
        y = torch.tensor([payload["root_cause_node_index"]], dtype=torch.long)

        out_obj = {
            "graph_id": payload["graph_id"],
            "system_id": payload.get("system_id", "unknown"),
            "run_id": payload["run_id"],
            "window_id": payload["window_id"],
            "window_phase": payload.get("window_phase", "active"),
            "node_names": payload["node_names"],
            "node_roles": payload.get("node_roles", []),
            "node_tiers": payload.get("node_tiers", []),
            "node_criticalities": payload.get("node_criticalities", []),
            "node_feature_names": payload["node_feature_names"],
            "x": x,
            "edge_index": edge_index,
            "y": y,
            "root_cause_service": payload["root_cause_service"],
            "root_cause_role": payload.get("root_cause_role", "unknown"),
            "root_cause_node_index": payload["root_cause_node_index"],
            "fault_family": payload.get("fault_family", "unknown"),
        }
        torch.save(out_obj, GRAPH_TENSOR_ROOT / f"{payload['graph_id']}.pt")
        exported += 1

    print(f"Exported {exported} graph tensor files")


if __name__ == "__main__":
    main()
