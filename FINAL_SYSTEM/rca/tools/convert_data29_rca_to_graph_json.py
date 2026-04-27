import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Convert Data29.3 run catalog + clean spans into FINAL_SYSTEM RCA graph JSON."
    )
    parser.add_argument("--run-catalog-path", required=True)
    parser.add_argument("--spans-dir", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--metadata-output-path", required=True)
    return parser.parse_args()


def safe_percentile(values: np.ndarray, q: float) -> float:
    if values.size == 0:
        return 0.0
    return float(np.percentile(values, q))


def scenario_name(row: pd.Series) -> str:
    run_id = str(row.get("run_id", "") or "").strip().lower()
    fault_type = str(row.get("fault_type", "") or "").strip().lower()
    root_cause = str(row.get("root_cause_service", "") or "").strip().lower()
    label = int(row.get("label", 0) or 0)

    if label == 0:
        if "norm_high" in run_id:
            return "normal_high"
        if "norm_mid" in run_id:
            return "normal_mid"
        if "norm_low" in run_id:
            return "normal_low"
        return "normal"

    mapping = {
        ("cpu-stress", "recommendationservice"): "cpu_recommendation",
        ("http-500", "frontend"): "http_frontend_500",
        ("pod-kill", "checkoutservice"): "pod_kill_checkout",
        ("pod-kill", "recommendationservice"): "pod_kill_recommendation",
        ("latency-injection", "paymentservice"): "latency_payment",
        ("latency-injection", "productcatalogservice"): "latency_productcatalog",
        ("memory-stress", "cartservice"): "memory_cart",
        ("replica-drop", "productcatalogservice"): "replica_drop_productcatalog",
        ("timeout", "currencyservice"): "timeout_currency",
    }
    return mapping.get((fault_type, root_cause), str(row.get("scenario_name", fault_type or "unknown")))


def load_services(run_catalog: pd.DataFrame, spans_dir: Path):
    service_rows = []
    for run_id in run_catalog["run_id"].astype(str):
        spans_path = spans_dir / f"spans_{run_id}_clean.parquet"
        df = pd.read_parquet(spans_path)
        subset = (
            df[
                [
                    "service_name",
                    "service_role",
                    "service_tier",
                    "criticality",
                    "is_entrypoint",
                    "is_stateful",
                ]
            ]
            .drop_duplicates()
            .copy()
        )
        service_rows.append(subset)

    services_df = pd.concat(service_rows, ignore_index=True).drop_duplicates(subset=["service_name"])
    services_df = services_df.sort_values("service_name").reset_index(drop=True)

    roles = sorted(services_df["service_role"].fillna("unknown").astype(str).unique().tolist())
    tiers = sorted(services_df["service_tier"].fillna("unknown").astype(str).unique().tolist())
    criticalities = sorted(services_df["criticality"].fillna("unknown").astype(str).unique().tolist())

    role_to_id = {name: idx + 1 for idx, name in enumerate(roles)}
    tier_to_id = {name: idx + 1 for idx, name in enumerate(tiers)}
    criticality_to_id = {name: idx + 1 for idx, name in enumerate(criticalities)}

    services = services_df["service_name"].astype(str).tolist()
    service_to_idx = {name: idx for idx, name in enumerate(services)}
    service_catalog = {
        row["service_name"]: {
            "service_role": str(row["service_role"]),
            "service_tier": str(row["service_tier"]),
            "criticality": str(row["criticality"]),
            "is_entrypoint": int(row["is_entrypoint"]),
            "is_stateful": int(row["is_stateful"]),
        }
        for _, row in services_df.iterrows()
    }

    return services, service_to_idx, service_catalog, role_to_id, tier_to_id, criticality_to_id


def build_edge_data(df: pd.DataFrame):
    span_to_service = {
        str(row["span_id"]): str(row["service_name"])
        for _, row in df[["span_id", "service_name"]].iterrows()
    }
    edge_counts = {}
    for _, row in df[["parent_span_id", "service_name"]].iterrows():
        parent_span_id = row["parent_span_id"]
        if pd.isna(parent_span_id):
            continue
        src_service = span_to_service.get(str(parent_span_id))
        dst_service = str(row["service_name"])
        if not src_service:
            continue
        key = (src_service, dst_service)
        edge_counts[key] = edge_counts.get(key, 0) + 1
    return edge_counts


def build_record(
    row: pd.Series,
    spans_dir: Path,
    services: list[str],
    service_to_idx: dict[str, int],
    service_catalog: dict,
    role_to_id: dict[str, int],
    tier_to_id: dict[str, int],
    criticality_to_id: dict[str, int],
):
    run_id = str(row["run_id"])
    spans_path = spans_dir / f"spans_{run_id}_clean.parquet"
    df = pd.read_parquet(spans_path).copy()

    edge_counts = build_edge_data(df)
    incoming = {service: 0.0 for service in services}
    outgoing = {service: 0.0 for service in services}
    for (src_service, dst_service), count in edge_counts.items():
        if src_service in outgoing:
            outgoing[src_service] += float(count)
        if dst_service in incoming:
            incoming[dst_service] += float(count)

    total_spans = float(len(df))
    x = []
    active_services = []
    for service_name in services:
        service_df = df[df["service_name"] == service_name]
        if len(service_df) > 0:
            active_services.append(service_name)
        durations = service_df["duration_ms"].astype(float).to_numpy()
        span_count = float(len(service_df))
        error_rate = float(service_df["is_error"].astype(float).mean()) if len(service_df) else 0.0
        info = service_catalog[service_name]
        x.append(
            [
                float(durations.mean()) if durations.size else 0.0,
                safe_percentile(durations, 95.0),
                error_rate,
                span_count,
                span_count / total_spans if total_spans > 0 else 0.0,
                incoming[service_name],
                outgoing[service_name],
                float(role_to_id[str(info["service_role"])]),
                float(tier_to_id[str(info["service_tier"])]),
                float(criticality_to_id[str(info["criticality"])]),
                float(info["is_entrypoint"]),
                float(info["is_stateful"]),
            ]
        )

    edge_index = []
    for (src_service, dst_service), count in sorted(edge_counts.items()):
        if count <= 0:
            continue
        if src_service not in service_to_idx or dst_service not in service_to_idx:
            continue
        edge_index.append([service_to_idx[src_service], service_to_idx[dst_service]])

    label = int(row["label"])
    y = [0] * len(services)
    root_cause_service = str(row["root_cause_service"] if label == 1 else "none")
    if label == 1 and root_cause_service in service_to_idx:
        y[service_to_idx[root_cause_service]] = 1

    trace_id = f"{run_id}__{row['trace_file'].replace('.json', '')}"
    return {
        "x": x,
        "edge_index": edge_index,
        "y": y,
        "graph_label": label,
        "trace_id": trace_id,
        "scenario": scenario_name(row),
        "source_file": str(row.get("raw_trace_path", row["trace_file"])),
        "root_cause_service": root_cause_service,
        "active_services": active_services,
    }


def main():
    args = parse_args()
    run_catalog = pd.read_parquet(args.run_catalog_path).copy()
    spans_dir = Path(args.spans_dir)

    services, service_to_idx, service_catalog, role_to_id, tier_to_id, criticality_to_id = load_services(
        run_catalog,
        spans_dir,
    )

    records = []
    graph_summaries = []
    for _, row in run_catalog.iterrows():
        record = build_record(
            row,
            spans_dir,
            services,
            service_to_idx,
            service_catalog,
            role_to_id,
            tier_to_id,
            criticality_to_id,
        )
        records.append(record)
        graph_summaries.append(
            {
                "trace_id": record["trace_id"],
                "scenario": record["scenario"],
                "graph_label": record["graph_label"],
                "root_cause_service": record["root_cause_service"],
                "active_services": record["active_services"],
                "source_file": record["source_file"],
            }
        )

    metadata = {
        "services": services,
        "service_to_idx": service_to_idx,
        "node_feature_names": [
            "avg_latency_ms",
            "p95_latency_ms",
            "error_rate",
            "request_count",
            "request_share",
            "in_degree",
            "out_degree",
            "role_id",
            "tier_id",
            "criticality_id",
            "is_entrypoint",
            "is_stateful",
        ],
        "graphs": graph_summaries,
    }

    output_path = Path(args.output_path)
    metadata_output_path = Path(args.metadata_output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(records, indent=2, ensure_ascii=True), encoding="utf-8")
    metadata_output_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=True), encoding="utf-8")

    print(
        {
            "graphs": len(records),
            "normal_graphs": int(sum(1 for record in records if int(record["graph_label"]) == 0)),
            "fault_graphs": int(sum(1 for record in records if int(record["graph_label"]) == 1)),
            "services": services,
            "output_path": str(output_path),
            "metadata_output_path": str(metadata_output_path),
        }
    )


if __name__ == "__main__":
    main()
