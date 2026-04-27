from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import pandas as pd


DEFAULT_DATA_ROOT = Path(r"D:\HOCTAP\2025-2026\HK2\DACN\microservices-demo\data_anomaly_balanced_v3")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean noisy anomaly negatives from a snapshot and write a new cleaned snapshot.")
    parser.add_argument("--source-root", type=Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--avg-trace-threshold", type=float, default=200.0)
    parser.add_argument("--p95-trace-threshold", type=float, default=1500.0)
    parser.add_argument("--error-trace-threshold", type=float, default=0.10)
    return parser.parse_args()


def ensure_sample_class(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    values = []
    for _, row in df.iterrows():
        sample_class = row.get("sample_class")
        if pd.notna(sample_class) and str(sample_class).strip().lower() not in {"", "nan", "none"}:
            values.append(str(sample_class).strip().lower())
            continue
        if int(row.get("is_anomaly", 0)) == 1:
            values.append("fault")
        elif str(row.get("fault_type", "none")).strip().lower() == "none":
            values.append("normal")
        else:
            values.append("hard-negative")
    df["sample_class"] = values
    return df


def main() -> None:
    args = parse_args()
    source_root = args.source_root
    output_root = args.output_root or source_root.with_name(source_root.name + "_clean")

    output_root.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source_root, output_root, dirs_exist_ok=True)

    window_path = output_root / "processed" / "anomaly" / "window_features_labeled.parquet"
    trace_path = output_root / "processed" / "anomaly" / "trace_features_labeled.parquet"
    window_labels_path = output_root / "processed" / "anomaly" / "window_labels.parquet"
    trace_labels_path = output_root / "processed" / "anomaly" / "trace_labels.parquet"

    window_df = pd.read_parquet(window_path)
    window_df = ensure_sample_class(window_df)

    noisy_negative_mask = (
        window_df["is_anomaly"].eq(0)
        & window_df["window_phase"].astype(str).isin(["pre", "recovery"])
        & (
            (window_df["avg_trace_duration_ms"] > args.avg_trace_threshold)
            | (window_df["p95_trace_duration_ms"] > args.p95_trace_threshold)
            | (window_df["error_trace_ratio"] > args.error_trace_threshold)
        )
    )
    removed_windows = window_df.loc[noisy_negative_mask, ["run_id", "window_id", "fault_type", "window_phase"]].copy()
    cleaned_window_df = window_df.loc[~noisy_negative_mask].copy()
    cleaned_window_df.to_parquet(window_path, index=False)
    cleaned_window_df.to_parquet(window_labels_path, index=False)

    if trace_path.exists():
        trace_df = pd.read_parquet(trace_path)
        trace_df = ensure_sample_class(trace_df)
        drop_keys = set(zip(removed_windows["run_id"], removed_windows["window_id"]))
        trace_keep_mask = [(str(rid), str(wid)) not in drop_keys for rid, wid in zip(trace_df["run_id"], trace_df["window_id"])]
        cleaned_trace_df = trace_df.loc[trace_keep_mask].copy()
        cleaned_trace_df.to_parquet(trace_path, index=False)
        cleaned_trace_df.to_parquet(trace_labels_path, index=False)

    summary = {
        "source_root": str(source_root),
        "output_root": str(output_root),
        "removed_windows": int(len(removed_windows)),
        "remaining_windows": int(len(cleaned_window_df)),
        "removed_by_fault": removed_windows["fault_type"].value_counts().to_dict(),
        "removed_by_phase": removed_windows["window_phase"].value_counts().to_dict(),
        "label_counts_after": cleaned_window_df["is_anomaly"].value_counts().to_dict(),
        "sample_class_after": cleaned_window_df["sample_class"].value_counts().to_dict(),
    }
    (output_root / "processed" / "anomaly" / "cleaning_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
