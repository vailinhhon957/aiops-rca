from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.rca_data_pipeline.config import ANOMALY_ROOT, PROCESSED_ROOT
from pipeline.rca_data_pipeline.feature_engineering import label_trace_features, label_window_features
from pipeline.rca_data_pipeline.io_utils import latest_table, read_table, write_table


def resolve_trace_features_file() -> Path | None:
    for candidate in [ANOMALY_ROOT / "trace_features.parquet", ANOMALY_ROOT / "trace_features.csv"]:
        if candidate.exists():
            return candidate
    return latest_table(ANOMALY_ROOT, "trace_features")


def resolve_window_features_file() -> Path | None:
    for candidate in [ANOMALY_ROOT / "window_features.parquet", ANOMALY_ROOT / "window_features.csv"]:
        if candidate.exists():
            return candidate
    return latest_table(ANOMALY_ROOT, "window_features")


def main() -> None:
    parser = argparse.ArgumentParser(description="Attach anomaly labels and run metadata to trace features.")
    parser.add_argument("--feature-file", type=Path, default=resolve_trace_features_file())
    parser.add_argument("--window-feature-file", type=Path, default=resolve_window_features_file())
    parser.add_argument("--run-catalog", type=Path, default=latest_table(PROCESSED_ROOT, "run_catalog"))
    args = parser.parse_args()

    if args.run_catalog is None:
        raise FileNotFoundError("Missing run catalog.")

    run_catalog_df = read_table(args.run_catalog)
    if args.feature_file is not None:
        trace_features_df = read_table(args.feature_file)
        labeled_df = label_trace_features(trace_features_df, run_catalog_df)
        features_path = write_table(labeled_df, ANOMALY_ROOT / "trace_features_labeled")
        trace_label_columns = [
            "system_id",
            "run_id",
            "window_id",
            "window_phase",
            "trace_id",
            "is_anomaly",
            "sample_class",
            "phase_policy",
            "fault_type",
            "fault_family",
            "root_cause_service",
            "fault_target_service",
            "fault_target_role",
            "scenario_name",
        ]
        for column in trace_label_columns:
            if column not in labeled_df.columns:
                labeled_df[column] = None
        labels_only = labeled_df[trace_label_columns].copy()
        labels_path = write_table(labels_only, ANOMALY_ROOT / "trace_labels")
        print(f"Labeled trace features: {features_path}")
        print(f"Trace labels: {labels_path}")

    if args.window_feature_file is not None:
        window_features_df = read_table(args.window_feature_file)
        labeled_window_df = label_window_features(window_features_df, run_catalog_df)
        window_features_path = write_table(labeled_window_df, ANOMALY_ROOT / "window_features_labeled")
        window_label_columns = [
            "system_id",
            "run_id",
            "window_id",
            "window_phase",
            "is_anomaly",
            "sample_class",
            "phase_policy",
            "fault_type",
            "fault_family",
            "root_cause_service",
            "fault_target_service",
            "fault_target_role",
            "scenario_name",
        ]
        for column in window_label_columns:
            if column not in labeled_window_df.columns:
                labeled_window_df[column] = None
        window_labels = labeled_window_df[window_label_columns].copy()
        window_labels_path = write_table(window_labels, ANOMALY_ROOT / "window_labels")
        print(f"Labeled window features: {window_features_path}")
        print(f"Window labels: {window_labels_path}")


if __name__ == "__main__":
    main()
