from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.rca_data_pipeline.config import GRAPH_PAYLOAD_ROOT, PROCESSED_ROOT, RCA_ROOT, SPANS_ROOT
from pipeline.rca_data_pipeline.feature_engineering import build_service_graphs
from pipeline.rca_data_pipeline.io_utils import latest_table, read_table, save_json, write_table


def main() -> None:
    parser = argparse.ArgumentParser(description="Build service-level graph payloads for RCA.")
    parser.add_argument("--run-catalog", type=Path, default=latest_table(PROCESSED_ROOT, "run_catalog"))
    parser.add_argument("--spans-root", type=Path, default=SPANS_ROOT)
    args = parser.parse_args()

    if args.run_catalog is None:
        raise FileNotFoundError("Missing run catalog.")

    run_catalog_df = read_table(args.run_catalog)
    graph_index_frames = []
    graph_count = 0

    span_files = sorted(list(args.spans_root.glob("spans_*_clean.parquet")) + list(args.spans_root.glob("spans_*_clean.csv")))
    for span_file in span_files:
        spans_df = read_table(span_file)
        if spans_df.empty:
            continue
        run_id = str(spans_df["run_id"].iloc[0])
        meta_row = run_catalog_df.loc[run_catalog_df["run_id"] == run_id]
        if meta_row.empty or int(meta_row.iloc[0].get("label", 0)) != 1:
            continue

        graph_index_df, payloads = build_service_graphs(spans_df, run_catalog_df)
        for payload in payloads:
            save_json(GRAPH_PAYLOAD_ROOT / f"graph_{payload['graph_id']}.json", payload)
        if not graph_index_df.empty:
            graph_index_frames.append(graph_index_df)
            graph_count += len(graph_index_df)

    if graph_index_frames:
        graph_index = pd.concat(graph_index_frames, ignore_index=True)
    else:
        graph_index = pd.DataFrame(
            columns=[
                "graph_id",
                "system_id",
                "run_id",
                "window_id",
                "window_phase",
                "root_cause_service",
                "root_cause_role",
                "root_cause_node_index",
                "fault_family",
                "num_nodes",
                "num_edges",
                "node_feature_names",
            ]
        )

    out_path = write_table(graph_index, RCA_ROOT / "graph_index")
    print(f"Graph index: {out_path}")
    print(f"Graph payload count: {graph_count}")


if __name__ == "__main__":
    main()
