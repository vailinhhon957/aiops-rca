from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.rca_data_pipeline.config import ANOMALY_ROOT, PROCESSED_ROOT, SPANS_ROOT
from pipeline.rca_data_pipeline.feature_engineering import build_window_features
from pipeline.rca_data_pipeline.io_utils import latest_table, read_table, write_table


def main() -> None:
    parser = argparse.ArgumentParser(description="Build window-level tabular features for anomaly detection.")
    parser.add_argument("--spans-root", type=Path, default=SPANS_ROOT)
    parser.add_argument("--run-catalog", type=Path, default=latest_table(PROCESSED_ROOT, "run_catalog"))
    args = parser.parse_args()

    run_catalog_df = read_table(args.run_catalog) if args.run_catalog is not None else pd.DataFrame()

    feature_frames = []
    span_files = sorted(list(args.spans_root.glob("spans_*_clean.parquet")) + list(args.spans_root.glob("spans_*_clean.csv")))
    for span_file in span_files:
        spans_df = read_table(span_file)
        features_df = build_window_features(spans_df, run_catalog_df=run_catalog_df)
        if not features_df.empty:
            feature_frames.append(features_df)

    all_features = pd.concat(feature_frames, ignore_index=True) if feature_frames else pd.DataFrame()
    out_path = write_table(all_features, ANOMALY_ROOT / "window_features")
    print(f"Window features: {out_path}")


if __name__ == "__main__":
    main()
