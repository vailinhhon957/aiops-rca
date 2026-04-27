from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.rca_data_pipeline.feature_engineering import build_service_graphs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild RCA graph payloads/tensors directly inside a snapshot data root.")
    parser.add_argument("--data-root", type=Path, required=True)
    return parser.parse_args()


def reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


def main() -> None:
    args = parse_args()
    data_root = args.data_root
    run_catalog_path = data_root / "processed" / "run_catalog.parquet"
    spans_root = data_root / "interim" / "spans"
    rca_root = data_root / "processed" / "rca"
    payload_root = rca_root / "graph_payloads"
    tensor_root = rca_root / "graph_tensors"

    if not run_catalog_path.exists():
        raise FileNotFoundError(f"Missing run_catalog: {run_catalog_path}")
    if not spans_root.exists():
        raise FileNotFoundError(f"Missing spans root: {spans_root}")

    run_catalog_df = pd.read_parquet(run_catalog_path)
    reset_dir(payload_root)
    reset_dir(tensor_root)
    rca_root.mkdir(parents=True, exist_ok=True)

    try:
        import torch
    except Exception as exc:
        raise RuntimeError("torch is required to rebuild RCA snapshot tensors.") from exc

    graph_index_frames: list[pd.DataFrame] = []
    graph_count = 0

    span_files = sorted(spans_root.glob("spans_*_clean.parquet"))
    for span_file in span_files:
        spans_df = pd.read_parquet(span_file)
        if spans_df.empty:
            continue
        run_id = str(spans_df["run_id"].iloc[0])
        meta_row = run_catalog_df.loc[run_catalog_df["run_id"] == run_id]
        if meta_row.empty or int(meta_row.iloc[0].get("label", 0)) != 1:
            continue

        graph_index_df, payloads = build_service_graphs(spans_df, run_catalog_df)
        for payload in payloads:
            (payload_root / f"graph_{payload['graph_id']}.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
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
            torch.save(out_obj, tensor_root / f"{payload['graph_id']}.pt")

        if not graph_index_df.empty:
            graph_index_frames.append(graph_index_df)
            graph_count += len(graph_index_df)

    if graph_index_frames:
        graph_index = pd.concat(graph_index_frames, ignore_index=True)
    else:
        graph_index = pd.DataFrame()

    graph_index_path = rca_root / "graph_index.parquet"
    graph_index.to_parquet(graph_index_path, index=False)

    print(f"Rebuilt RCA snapshot: {data_root}")
    print(f"graph_index={graph_index_path}")
    print(f"graph_count={graph_count}")


if __name__ == "__main__":
    main()
