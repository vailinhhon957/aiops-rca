import argparse
from pathlib import Path

import numpy as np
import pandas as pd


OUTPUT_COLUMNS = [
    "trace_id",
    "source_file",
    "split_group",
    "sequence_group",
    "split_tag",
    "row_order",
    "scenario",
    "label",
    "root_cause",
    "span_count",
    "service_count",
    "app_service_count",
    "avg_latency",
    "max_latency",
    "std_latency",
    "trace_latency",
    "error_rate",
    "http_5xx_rate",
    "depth",
    "latency_zscore",
    "duration_ratio",
    "root_service",
    "data_source",
    # Per-service features
    "adservice_avg_latency",
    "adservice_error_rate",
    "cartservice_avg_latency",
    "cartservice_error_rate",
    "checkoutservice_avg_latency",
    "checkoutservice_error_rate",
    "currencyservice_avg_latency",
    "currencyservice_error_rate",
    "emailservice_avg_latency",
    "emailservice_error_rate",
    "frontend_avg_latency",
    "frontend_error_rate",
    "paymentservice_avg_latency",
    "paymentservice_error_rate",
    "productcatalogservice_avg_latency",
    "productcatalogservice_error_rate",
    "recommendationservice_avg_latency",
    "recommendationservice_error_rate",
    "shippingservice_avg_latency",
    "shippingservice_error_rate",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Convert Data29.3 anomaly parquet into FINAL_SYSTEM CSV format."
    )
    parser.add_argument("--trace-features-path", required=True)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--source-tag", default="data29_3")
    parser.add_argument("--train-runs-path", default=None)
    parser.add_argument("--val-runs-path", default=None)
    parser.add_argument("--test-runs-path", default=None)
    return parser.parse_args()


def load_run_splits(args) -> dict[str, str]:
    mapping: dict[str, str] = {}
    split_paths = [
        ("train", args.train_runs_path),
        ("val", args.val_runs_path),
        ("test", args.test_runs_path),
    ]
    for split_name, split_path in split_paths:
        if not split_path:
            continue
        with Path(split_path).open("r", encoding="utf-8") as f:
            for line in f:
                run_id = line.strip()
                if run_id:
                    mapping[run_id] = split_name
    return mapping


def scenario_name(row: pd.Series) -> str:
    fault_type = str(row.get("fault_type", "") or "").strip().lower()
    root_cause = str(row.get("root_cause_service", "") or "").strip().lower()
    run_id = str(row.get("run_id", "") or "").strip().lower()

    if int(row.get("label", row.get("is_anomaly", 0)) or 0) == 0:
        if "norm_high" in run_id:
            return "normal_high"
        if "norm_mid" in run_id:
            return "normal_mid"
        if "norm_low" in run_id:
            return "normal_low"
        return "normal"

    mapping = {
        ("cpu-stress", "recommendationservice"): "cpu_recommendation",
        ("cpu-stress", "frontend"): "cpu_frontend",
        ("pod-kill", "checkoutservice"): "pod_kill_checkout",
        ("pod-kill", "recommendationservice"): "pod_kill_recommendation",
        ("memory-stress", "cartservice"): "memory_cart",
        ("latency-injection", "paymentservice"): "latency_payment",
        ("latency-injection", "productcatalogservice"): "latency_productcatalog",
        ("timeout", "currencyservice"): "timeout_currency",
        ("http-500", "frontend"): "http_frontend_500",
        ("replica-drop", "productcatalogservice"): "replica_drop_productcatalog",
    }
    if (fault_type, root_cause) in mapping:
        return mapping[(fault_type, root_cause)]
    return str(row.get("scenario_name", fault_type or "unknown")) or "unknown"


def main():
    args = parse_args()
    df = pd.read_parquet(args.trace_features_path).copy()
    df = df.reset_index(drop=True)
    run_splits = load_run_splits(args)

    # For anomaly detection we need the actual trace-level anomaly flag, not the
    # run/fault-campaign label. Data29.x stores this in `is_anomaly`.
    if "is_anomaly" in df.columns:
        label_series = df["is_anomaly"]
    elif "label" in df.columns:
        label_series = df["label"]
    else:
        raise ValueError("Khong tim thay cot nhan anomaly (`is_anomaly` hoac `label`).")
    error_rate = (df["error_span_count"] / df["span_count"].replace(0, np.nan)).fillna(0.0)
    row_order = df.groupby("run_id").cumcount()
    split_tag = df["run_id"].astype(str).map(run_splits).fillna("")
    duration_ratio = df.get("duration_ratio_to_run_baseline", 1.0)
    latency_zscore = df.get("latency_zscore", 0.0)

    per_service_cols = [
        "adservice", "cartservice", "checkoutservice", "currencyservice",
        "emailservice", "frontend", "paymentservice", "productcatalogservice",
        "recommendationservice", "shippingservice",
    ]

    converted = pd.DataFrame(
        {
            "trace_id": df["trace_id"].astype(str),
            "source_file": df["run_id"].astype(str),
            "split_group": df["run_id"].astype(str),
            "sequence_group": df["run_id"].astype(str),
            "split_tag": split_tag.astype(str),
            "row_order": row_order.astype(int),
            "scenario": df.apply(scenario_name, axis=1),
            "label": label_series.astype(int),
            "root_cause": df["root_cause_service"].fillna("none").astype(str),
            "span_count": df["span_count"].astype(float),
            "service_count": df["service_count"].astype(float),
            "app_service_count": df["service_count"].astype(float),
            "avg_latency": df["mean_span_duration_ms"].astype(float),
            "max_latency": df["max_span_duration_ms"].astype(float),
            "std_latency": df["std_span_duration_ms"].astype(float),
            "trace_latency": df["trace_duration_ms"].astype(float),
            "error_rate": error_rate.astype(float),
            # Data29.3 does not keep a dedicated HTTP-5xx-only ratio at trace level.
            "http_5xx_rate": error_rate.astype(float),
            # Current pipeline expects a depth-like feature; fanout_count is the nearest generic proxy.
            "depth": (df["fanout_count"].astype(float) + 1.0),
            "latency_zscore": pd.Series(latency_zscore).astype(float),
            "duration_ratio": pd.Series(duration_ratio).astype(float),
            "root_service": df["source_service"].fillna("unknown").astype(str),
            "data_source": args.source_tag,
            **{
                f"{svc}_avg_latency": pd.Series(df.get(f"{svc}_avg_latency_ms", 0.0)).astype(float)
                for svc in per_service_cols
            },
            **{
                f"{svc}_error_rate": pd.Series(df.get(f"{svc}_error_rate", 0.0)).astype(float)
                for svc in per_service_cols
            },
        }
    )

    output_path = Path(args.output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    converted.to_csv(output_path, index=False, columns=OUTPUT_COLUMNS)

    print(
        {
            "rows": int(len(converted)),
            "normal_rows": int((converted["label"] == 0).sum()),
            "anomaly_rows": int((converted["label"] == 1).sum()),
            "source_files": int(converted["source_file"].nunique()),
            "output_csv": str(output_path),
        }
    )


if __name__ == "__main__":
    main()
