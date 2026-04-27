from __future__ import annotations

import ast
from collections import defaultdict

import pandas as pd

from .jaeger_parser import build_service_edges

TRACE_FEATURE_COLUMNS = [
    "system_id",
    "run_id",
    "window_id",
    "window_phase",
    "trace_id",
    "trace_duration_ms",
    "span_count",
    "service_count",
    "service_role_count",
    "error_span_count",
    "error_service_count",
    "max_span_duration_ms",
    "mean_span_duration_ms",
    "std_span_duration_ms",
    "critical_path_est_ms",
    "fanout_count",
    "entrypoint_ratio",
    "stateful_service_ratio",
    "edge_span_ratio",
    "backend_span_ratio",
    "async_span_ratio",
    "stateful_span_ratio",
    "client_span_ratio",
    "server_span_ratio",
    "grpc_span_ratio",
    "http_span_ratio",
    "db_span_ratio",
    "cache_span_ratio",
    "mq_span_ratio",
    "internal_span_ratio",
    "latency_zscore",
    "duration_ratio_to_run_baseline",
]

WINDOW_FEATURE_COLUMNS = [
    "system_id",
    "run_id",
    "window_id",
    "window_phase",
    "trace_count",
    "service_count",
    "service_role_count",
    "avg_trace_duration_ms",
    "p95_trace_duration_ms",
    "max_trace_duration_ms",
    "error_trace_ratio",
    "error_span_ratio",
    "request_fanout_mean",
    "critical_path_mean_ms",
    "latency_cv",
    "mean_span_count",
    "call_type_diversity",
    "entrypoint_trace_ratio",
    "stateful_trace_ratio",
]

LABEL_METADATA_COLUMNS = [
    "label",
    "sample_class",
    "phase_policy",
    "fault_type",
    "fault_family",
    "root_cause_service",
    "fault_target_service",
    "fault_target_role",
    "source_service",
    "source_service_role",
    "scenario_name",
    "start_time",
    "fault_start_time",
    "fault_end_time",
    "end_time",
]

ROLE_TO_ID = {
    "unknown": 0,
    "entrypoint": 1,
    "checkout": 2,
    "cart": 3,
    "payment": 4,
    "catalog": 5,
    "recommendation": 6,
    "currency": 7,
    "shipping": 8,
    "notification": 9,
    "ad": 10,
    "cache": 11,
}

TIER_TO_ID = {
    "unknown": 0,
    "edge": 1,
    "backend": 2,
    "async": 3,
    "stateful": 4,
}

CRITICALITY_TO_ID = {
    "unknown": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
}

PHASE_ORDER = {
    "steady": 0,
    "pre": 1,
    "active": 2,
    "recovery": 3,
}


def _normalize_text(value: object, default: str = "") -> str:
    if value is None:
        return default
    if pd.isna(value):
        return default
    text = str(value).strip()
    if text.lower() == "nan":
        return default
    return text or default


def _parse_legacy_metadata(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    text = _normalize_text(value)
    if not text:
        return {}
    try:
        parsed = ast.literal_eval(text)
    except (SyntaxError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _metadata_value(meta_row: pd.Series, key: str, default: str = "") -> str:
    direct_value = _normalize_text(meta_row.get(key), "")
    if direct_value:
        return direct_value
    legacy = _parse_legacy_metadata(meta_row.get("legacy_metadata"))
    return _normalize_text(legacy.get(key), default)


def _iso_to_ms(value: str) -> float | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return float(pd.to_datetime(text).timestamp() * 1000.0)
    except Exception:
        return None


def _build_run_phase_lookup(run_catalog_df: pd.DataFrame) -> dict[str, dict[str, object]]:
    lookup: dict[str, dict[str, object]] = {}
    if run_catalog_df.empty:
        return lookup

    for _, meta_row in run_catalog_df.iterrows():
        run_id = _normalize_text(meta_row.get("run_id"))
        if not run_id:
            continue
        fault_type = _metadata_value(meta_row, "fault_type", "none").lower()
        label = int(float(_normalize_text(meta_row.get("label"), "0") or 0))
        lookup[run_id] = {
            "label": label,
            "fault_type": fault_type,
            "start_ms": _iso_to_ms(_metadata_value(meta_row, "start_time")),
            "fault_start_ms": _iso_to_ms(_metadata_value(meta_row, "fault_start_time")),
            "fault_end_ms": _iso_to_ms(_metadata_value(meta_row, "fault_end_time")),
            "end_ms": _iso_to_ms(_metadata_value(meta_row, "end_time")),
        }
    return lookup


def _phase_for_trace(midpoint_ms: float, meta: dict[str, object] | None) -> str:
    if not meta:
        return "steady"

    fault_start_ms = meta.get("fault_start_ms")
    fault_end_ms = meta.get("fault_end_ms")
    if fault_start_ms is None or fault_end_ms is None or fault_end_ms < fault_start_ms:
        if int(meta.get("label", 0)) == 1 and str(meta.get("fault_type", "none")).lower() not in {"", "none"}:
            return "active"
        return "steady"

    if midpoint_ms < float(fault_start_ms):
        return "pre"
    if midpoint_ms <= float(fault_end_ms):
        return "active"
    return "recovery"


def assign_phase_windows(spans_df: pd.DataFrame, run_catalog_df: pd.DataFrame | None) -> pd.DataFrame:
    if spans_df.empty:
        phased = spans_df.copy()
        phased["window_phase"] = []
        return phased

    if run_catalog_df is None or run_catalog_df.empty:
        if "window_phase" in spans_df.columns:
            return spans_df.copy()
        phased = spans_df.copy()
        phased["window_phase"] = "steady"
        return phased

    run_phase_lookup = _build_run_phase_lookup(run_catalog_df)
    trace_bounds = (
        spans_df.groupby(["run_id", "trace_id"], as_index=False)
        .agg(
            trace_start_ms=("start_time_ms", "min"),
            trace_end_ms=("end_time_ms", "max"),
            original_window_id=("window_id", "first"),
        )
        .copy()
    )
    midpoint_series = (trace_bounds["trace_start_ms"] + trace_bounds["trace_end_ms"]) / 2.0
    trace_bounds["window_phase"] = [
        _phase_for_trace(midpoint_ms=float(midpoint), meta=run_phase_lookup.get(str(run_id)))
        for midpoint, run_id in zip(midpoint_series, trace_bounds["run_id"])
    ]
    trace_bounds["phase_window_id"] = [
        f"{original_window_id}__{window_phase}"
        for original_window_id, window_phase in zip(trace_bounds["original_window_id"], trace_bounds["window_phase"])
    ]

    phased = spans_df.merge(
        trace_bounds[["run_id", "trace_id", "phase_window_id", "window_phase"]],
        on=["run_id", "trace_id"],
        how="left",
    )
    phased["window_id"] = phased["phase_window_id"].fillna(phased["window_id"])
    phased["window_phase"] = phased["window_phase"].fillna("steady")
    phased = phased.drop(columns=["phase_window_id"])
    return phased


def clean_spans(spans_df: pd.DataFrame) -> pd.DataFrame:
    if spans_df.empty:
        return spans_df.copy()

    cleaned = spans_df.copy()
    cleaned = cleaned.dropna(subset=["trace_id", "span_id", "service_name"])
    cleaned = cleaned[cleaned["duration_ms"] > 0]
    cleaned["service_name"] = cleaned["service_name"].astype(str).str.lower().str.strip()
    cleaned["operation_name"] = cleaned["operation_name"].fillna("unknown").astype(str)
    cleaned["status_code"] = cleaned["status_code"].fillna("unknown").astype(str)
    cleaned["span_kind"] = cleaned["span_kind"].fillna("unknown").astype(str).str.lower()
    cleaned["parent_span_id"] = cleaned["parent_span_id"].fillna("")
    cleaned["is_root_span"] = cleaned["parent_span_id"].eq("").astype(int)

    trace_counts = cleaned.groupby("trace_id")["span_id"].count()
    valid_trace_ids = trace_counts[trace_counts >= 2].index
    return cleaned[cleaned["trace_id"].isin(valid_trace_ids)].reset_index(drop=True)


def build_trace_features(spans_df: pd.DataFrame, run_catalog_df: pd.DataFrame | None = None) -> pd.DataFrame:
    if spans_df.empty:
        return pd.DataFrame(columns=TRACE_FEATURE_COLUMNS)

    spans_df = assign_phase_windows(spans_df, run_catalog_df)
    edge_df = build_service_edges(spans_df)
    fanout_lookup = edge_df.groupby(["system_id", "run_id", "window_id", "trace_id"]).size().to_dict()

    records = []
    group_keys = ["system_id", "run_id", "window_id", "trace_id"]
    for (system_id, run_id, window_id, trace_id), group in spans_df.groupby(group_keys):
        root_duration = group.loc[group["is_root_span"] == 1, "duration_ms"]
        unique_services = group[["service_name", "service_role", "service_tier", "is_entrypoint", "is_stateful"]].drop_duplicates(
            subset=["service_name"]
        )
        span_count = len(group)
        record = {
            "system_id": system_id,
            "run_id": run_id,
            "window_id": window_id,
            "window_phase": str(group["window_phase"].iloc[0]) if "window_phase" in group.columns else "steady",
            "trace_id": trace_id,
            "trace_duration_ms": float(group["end_time_ms"].max() - group["start_time_ms"].min()),
            "span_count": int(span_count),
            "service_count": int(group["service_name"].nunique()),
            "service_role_count": int(group["service_role"].nunique()),
            "error_span_count": int(group["is_error"].sum()),
            "error_service_count": int(group.loc[group["is_error"] == 1, "service_name"].nunique()),
            "max_span_duration_ms": float(group["duration_ms"].max()),
            "mean_span_duration_ms": float(group["duration_ms"].mean()),
            "std_span_duration_ms": float(group["duration_ms"].std(ddof=0) if span_count > 1 else 0.0),
            "critical_path_est_ms": float(root_duration.max() if not root_duration.empty else group["duration_ms"].max()),
            "fanout_count": int(fanout_lookup.get((system_id, run_id, window_id, trace_id), 0)),
            "entrypoint_ratio": float(unique_services["is_entrypoint"].mean() if not unique_services.empty else 0.0),
            "stateful_service_ratio": float(unique_services["is_stateful"].mean() if not unique_services.empty else 0.0),
            "edge_span_ratio": float(group["service_tier"].eq("edge").mean()),
            "backend_span_ratio": float(group["service_tier"].eq("backend").mean()),
            "async_span_ratio": float(group["service_tier"].eq("async").mean()),
            "stateful_span_ratio": float(group["service_tier"].eq("stateful").mean()),
            "client_span_ratio": float(group["span_kind"].eq("client").mean()),
            "server_span_ratio": float(group["span_kind"].eq("server").mean()),
            "grpc_span_ratio": float(group["call_type"].eq("grpc").mean()),
            "http_span_ratio": float(group["call_type"].eq("http").mean()),
            "db_span_ratio": float(group["call_type"].eq("db").mean()),
            "cache_span_ratio": float(group["call_type"].eq("cache").mean()),
            "mq_span_ratio": float(group["call_type"].eq("mq").mean()),
            "internal_span_ratio": float(group["call_type"].eq("internal").mean()),
        }
        records.append(record)

    features_df = pd.DataFrame(records)
    baseline = (
        features_df.groupby(["system_id", "run_id"])["trace_duration_ms"]
        .agg(run_trace_duration_mean="mean", run_trace_duration_std="std")
        .reset_index()
    )
    features_df = features_df.merge(baseline, on=["system_id", "run_id"], how="left")
    features_df["run_trace_duration_std"] = features_df["run_trace_duration_std"].fillna(0.0)
    safe_std = features_df["run_trace_duration_std"].replace(0.0, 1.0)
    safe_mean = features_df["run_trace_duration_mean"].replace(0.0, 1.0)
    features_df["latency_zscore"] = (features_df["trace_duration_ms"] - features_df["run_trace_duration_mean"]) / safe_std
    features_df["duration_ratio_to_run_baseline"] = features_df["trace_duration_ms"] / safe_mean
    features_df = features_df.drop(columns=["run_trace_duration_mean", "run_trace_duration_std"])
    return features_df[TRACE_FEATURE_COLUMNS]


def build_window_features(spans_df: pd.DataFrame, run_catalog_df: pd.DataFrame | None = None) -> pd.DataFrame:
    if spans_df.empty:
        return pd.DataFrame(columns=WINDOW_FEATURE_COLUMNS)

    spans_df = assign_phase_windows(spans_df, run_catalog_df)
    trace_features_df = build_trace_features(spans_df, run_catalog_df=None)
    if trace_features_df.empty:
        return pd.DataFrame(columns=WINDOW_FEATURE_COLUMNS)

    window_rows = []
    for (system_id, run_id, window_id), trace_group in trace_features_df.groupby(["system_id", "run_id", "window_id"]):
        span_group = spans_df.loc[
            (spans_df["system_id"] == system_id)
            & (spans_df["run_id"] == run_id)
            & (spans_df["window_id"] == window_id)
        ]
        total_spans = int(trace_group["span_count"].sum())
        error_spans = int(trace_group["error_span_count"].sum())
        avg_trace_duration = float(trace_group["trace_duration_ms"].mean())
        window_rows.append(
            {
                "system_id": system_id,
                "run_id": run_id,
                "window_id": window_id,
                "window_phase": str(trace_group["window_phase"].iloc[0]) if "window_phase" in trace_group.columns else "steady",
                "trace_count": int(len(trace_group)),
                "service_count": int(span_group["service_name"].nunique()),
                "service_role_count": int(span_group["service_role"].nunique()),
                "avg_trace_duration_ms": avg_trace_duration,
                "p95_trace_duration_ms": float(trace_group["trace_duration_ms"].quantile(0.95)),
                "max_trace_duration_ms": float(trace_group["trace_duration_ms"].max()),
                "error_trace_ratio": float((trace_group["error_span_count"] > 0).mean()),
                "error_span_ratio": float(error_spans / total_spans) if total_spans else 0.0,
                "request_fanout_mean": float(trace_group["fanout_count"].mean()),
                "critical_path_mean_ms": float(trace_group["critical_path_est_ms"].mean()),
                "latency_cv": float(trace_group["trace_duration_ms"].std(ddof=0) / avg_trace_duration)
                if avg_trace_duration
                else 0.0,
                "mean_span_count": float(trace_group["span_count"].mean()),
                "call_type_diversity": int(span_group["call_type"].nunique()),
                "entrypoint_trace_ratio": float((trace_group["entrypoint_ratio"] > 0).mean()),
                "stateful_trace_ratio": float((trace_group["stateful_service_ratio"] > 0).mean()),
            }
        )
    return pd.DataFrame(window_rows, columns=WINDOW_FEATURE_COLUMNS)


def label_feature_table(features_df: pd.DataFrame, run_catalog_df: pd.DataFrame) -> pd.DataFrame:
    if features_df.empty:
        columns = list(features_df.columns) + [col for col in LABEL_METADATA_COLUMNS + ["is_anomaly"] if col not in features_df.columns]
        return pd.DataFrame(columns=columns)
    cols = ["run_id", *LABEL_METADATA_COLUMNS]
    catalog_df = run_catalog_df.copy()
    for column in cols:
        if column not in catalog_df.columns:
            catalog_df[column] = None
    merged = features_df.merge(catalog_df[cols], on="run_id", how="left")
    if "window_phase" not in merged.columns:
        merged["window_phase"] = "steady"
    label_mask = merged["label"].fillna(0).astype(int).eq(1)
    active_mask = merged["window_phase"].astype(str).eq("active")
    fallback_mask = merged["window_phase"].astype(str).eq("steady") & merged["fault_type"].fillna("none").astype(str).ne("none")
    merged["is_anomaly"] = (label_mask & (active_mask | fallback_mask)).astype(int)
    return merged


def label_trace_features(trace_features_df: pd.DataFrame, run_catalog_df: pd.DataFrame) -> pd.DataFrame:
    return label_feature_table(trace_features_df, run_catalog_df)


def label_window_features(window_features_df: pd.DataFrame, run_catalog_df: pd.DataFrame) -> pd.DataFrame:
    return label_feature_table(window_features_df, run_catalog_df)


def build_service_graphs(spans_df: pd.DataFrame, run_catalog_df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    if spans_df.empty:
        return (
            pd.DataFrame(
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
            ),
            [],
        )

    graph_rows = []
    payloads = []
    spans_df = assign_phase_windows(spans_df, run_catalog_df)
    run_service_defaults: dict[tuple[str, str], dict[str, object]] = {}
    if not spans_df.empty:
        service_meta_df = (
            spans_df.groupby(["run_id", "service_name"], as_index=False)
            .agg(
                service_role=("service_role", "first"),
                service_tier=("service_tier", "first"),
                criticality=("criticality", "first"),
                is_entrypoint=("is_entrypoint", "max"),
                is_stateful=("is_stateful", "max"),
            )
        )
        for row in service_meta_df.itertuples(index=False):
            run_service_defaults[(str(getattr(row, "run_id")), str(getattr(row, "service_name")))] = {
                "service_role": str(getattr(row, "service_role") or "unknown").lower(),
                "service_tier": str(getattr(row, "service_tier") or "unknown").lower(),
                "criticality": str(getattr(row, "criticality") or "unknown").lower(),
                "is_entrypoint": int(getattr(row, "is_entrypoint")),
                "is_stateful": int(getattr(row, "is_stateful")),
            }

    for (system_id, run_id, window_id), group in spans_df.groupby(["system_id", "run_id", "window_id"]):
        meta = run_catalog_df.loc[run_catalog_df["run_id"] == run_id]
        if meta.empty:
            continue
        meta_row = meta.iloc[0]
        window_phase = str(group["window_phase"].iloc[0]) if "window_phase" in group.columns else "steady"
        fault_target_service = str(meta_row.get("fault_target_service", meta_row.get("root_cause_service", "none"))).strip().lower()
        fault_target_role = str(meta_row.get("fault_target_role", "unknown")).strip().lower()
        fault_family = str(meta_row.get("fault_family", meta_row.get("fault_type", "unknown"))).strip().lower()
        if int(meta_row.get("label", 0)) != 1 or window_phase != "active":
            continue

        node_stats = (
            group.groupby("service_name")
            .agg(
                avg_latency_ms=("duration_ms", "mean"),
                p95_latency_ms=("duration_ms", lambda s: float(s.quantile(0.95))),
                error_rate=("is_error", "mean"),
                request_count=("span_id", "count"),
                service_role=("service_role", "first"),
                service_tier=("service_tier", "first"),
                criticality=("criticality", "first"),
                is_entrypoint=("is_entrypoint", "max"),
                is_stateful=("is_stateful", "max"),
            )
            .reset_index()
        )
        if fault_target_service and fault_target_service != "none" and fault_target_service not in set(node_stats["service_name"].astype(str)):
            default_meta = run_service_defaults.get(
                (str(run_id), fault_target_service),
                {
                    "service_role": fault_target_role or "unknown",
                    "service_tier": "unknown",
                    "criticality": "unknown",
                    "is_entrypoint": 0,
                    "is_stateful": 0,
                },
            )
            # Inject a zero-traffic root-cause node when the faulty service disappears from the active window
            # (for example pod-kill or full outage). This preserves the RCA label instead of emitting y=-1.
            node_stats = pd.concat(
                [
                    node_stats,
                    pd.DataFrame(
                        [
                            {
                                "service_name": fault_target_service,
                                "avg_latency_ms": 0.0,
                                "p95_latency_ms": 0.0,
                                "error_rate": 0.0,
                                "request_count": 0,
                                "service_role": default_meta["service_role"],
                                "service_tier": default_meta["service_tier"],
                                "criticality": default_meta["criticality"],
                                "is_entrypoint": int(default_meta["is_entrypoint"]),
                                "is_stateful": int(default_meta["is_stateful"]),
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )
        total_requests = int(node_stats["request_count"].sum())
        edges_df = build_service_edges(group)
        degree_in = defaultdict(int)
        degree_out = defaultdict(int)
        edges = []
        for edge in edges_df.groupby(["src_service", "dst_service"])["edge_count"].sum().reset_index().itertuples(index=False):
            src = getattr(edge, "src_service")
            dst = getattr(edge, "dst_service")
            weight = int(getattr(edge, "edge_count"))
            degree_out[src] += weight
            degree_in[dst] += weight
            edges.append({"src": src, "dst": dst, "weight": weight})

        node_names = node_stats["service_name"].tolist()
        node_to_idx = {name: idx for idx, name in enumerate(node_names)}
        node_roles = node_stats["service_role"].astype(str).tolist()
        node_tiers = node_stats["service_tier"].astype(str).tolist()
        node_criticalities = node_stats["criticality"].astype(str).tolist()
        nodes = []
        for row in node_stats.itertuples(index=False):
            service_name = getattr(row, "service_name")
            service_role = str(getattr(row, "service_role") or "unknown").lower()
            service_tier = str(getattr(row, "service_tier") or "unknown").lower()
            criticality = str(getattr(row, "criticality") or "unknown").lower()
            request_count = int(getattr(row, "request_count"))
            nodes.append(
                {
                    "service_name": service_name,
                    "service_role": service_role,
                    "service_tier": service_tier,
                    "criticality": criticality,
                    "is_entrypoint": int(getattr(row, "is_entrypoint")),
                    "is_stateful": int(getattr(row, "is_stateful")),
                    "features": [
                        float(getattr(row, "avg_latency_ms")),
                        float(getattr(row, "p95_latency_ms")),
                        float(getattr(row, "error_rate")),
                        float(request_count),
                        float(request_count / total_requests) if total_requests else 0.0,
                        float(degree_in.get(service_name, 0)),
                        float(degree_out.get(service_name, 0)),
                        float(ROLE_TO_ID.get(service_role, ROLE_TO_ID["unknown"])),
                        float(TIER_TO_ID.get(service_tier, TIER_TO_ID["unknown"])),
                        float(CRITICALITY_TO_ID.get(criticality, CRITICALITY_TO_ID["unknown"])),
                        float(int(getattr(row, "is_entrypoint"))),
                        float(int(getattr(row, "is_stateful"))),
                    ],
                }
            )

        edge_index = [
            [node_to_idx[e["src"]], node_to_idx[e["dst"]]]
            for e in edges
            if e["src"] in node_to_idx and e["dst"] in node_to_idx
        ]
        graph_id = f"{run_id}__{window_id}"
        root_cause_node_index = int(node_to_idx.get(fault_target_service, -1))
        payload = {
            "graph_id": graph_id,
            "system_id": system_id,
            "run_id": run_id,
            "window_id": window_id,
            "window_phase": window_phase,
            "root_cause_service": fault_target_service,
            "root_cause_role": fault_target_role,
            "root_cause_node_index": root_cause_node_index,
            "fault_family": fault_family,
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
            "node_names": node_names,
            "node_roles": node_roles,
            "node_tiers": node_tiers,
            "node_criticalities": node_criticalities,
            "nodes": nodes,
            "edges": edges,
            "edge_index": edge_index,
        }
        payloads.append(payload)
        graph_rows.append(
            {
                "graph_id": graph_id,
                "system_id": system_id,
                "run_id": run_id,
                "window_id": window_id,
                "window_phase": window_phase,
                "root_cause_service": fault_target_service,
                "root_cause_role": fault_target_role,
                "root_cause_node_index": root_cause_node_index,
                "fault_family": fault_family,
                "num_nodes": len(node_names),
                "num_edges": len(edge_index),
                "node_feature_names": ",".join(payload["node_feature_names"]),
            }
        )

    return pd.DataFrame(graph_rows), payloads
