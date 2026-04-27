from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

from aiops_framework.registry.service_catalog import load_service_catalog, service_lookup
from pipeline.rca_data_pipeline.feature_engineering import (
    CRITICALITY_TO_ID,
    ROLE_TO_ID,
    TIER_TO_ID,
    build_window_features,
    clean_spans,
)
from pipeline.rca_data_pipeline.jaeger_parser import build_service_edges, parse_jaeger_payload


DEFAULT_JAEGER_URL = os.environ.get("AIOPS_LIVE_JAEGER_URL", "http://127.0.0.1:16686")
DEFAULT_PROMETHEUS_URL = os.environ.get("AIOPS_LIVE_PROM_URL", "http://127.0.0.1:9090")
DEFAULT_SYSTEM_ID = os.environ.get("AIOPS_LIVE_SYSTEM_ID", "online-boutique")
DEFAULT_SOURCE_SERVICE = os.environ.get("AIOPS_LIVE_SOURCE_SERVICE", "frontend")


def _load_json(url: str) -> dict[str, Any]:
    with urlopen(url, timeout=30) as response:
        return json.loads(response.read().decode("utf-8-sig"))


def fetch_jaeger_payload(
    jaeger_url: str = DEFAULT_JAEGER_URL,
    source_service: str = DEFAULT_SOURCE_SERVICE,
    lookback_minutes: int = 2,
    query_limit: int = 150,
) -> dict[str, Any]:
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=max(1, int(lookback_minutes)))
    params = urlencode(
        {
            "service": source_service,
            "start": int(start.timestamp() * 1_000_000),
            "end": int(end.timestamp() * 1_000_000),
            "limit": int(query_limit),
        }
    )
    url = f"{jaeger_url.rstrip('/')}/api/traces?{params}"
    return _load_json(url)


def _extract_prom_value(payload: dict[str, Any]) -> float | None:
    data = payload.get("data", {})
    result = data.get("result", [])
    if not result:
        scalar = data.get("result")
        if isinstance(scalar, list) and len(scalar) == 2:
            try:
                return float(scalar[1])
            except Exception:
                return None
        return None
    first = result[0]
    value = first.get("value")
    if isinstance(value, list) and len(value) == 2:
        try:
            return float(value[1])
        except Exception:
            return None
    return None


def _prom_query(prometheus_url: str, query: str) -> float | None:
    params = urlencode({"query": query})
    url = f"{prometheus_url.rstrip('/')}/api/v1/query?{params}"
    payload = _load_json(url)
    return _extract_prom_value(payload)


def fetch_prometheus_snapshot(prometheus_url: str = DEFAULT_PROMETHEUS_URL) -> dict[str, Any]:
    query_candidates = {
        "up_targets": ["sum(up)"],
        "cpu_usage": ['sum(rate(container_cpu_usage_seconds_total{container!="",pod!=""}[2m]))'],
        "memory_usage": ['sum(container_memory_working_set_bytes{container!="",pod!=""})'],
        "request_rate": [
            "sum(rate(http_server_requests_seconds_count[1m]))",
            "sum(rate(traces_spanmetrics_calls_total[1m]))",
        ],
        "error_rate": [
            'sum(rate(http_server_requests_seconds_count{status=~"5.."}[1m]))',
            'sum(rate(traces_spanmetrics_calls_total{status_code="STATUS_CODE_ERROR"}[1m]))',
        ],
    }
    snapshot: dict[str, Any] = {"prometheus_url": prometheus_url, "status": "ok", "values": {}, "missing": []}
    for label, queries in query_candidates.items():
        value = None
        for query in queries:
            try:
                value = _prom_query(prometheus_url, query)
                if value is not None:
                    snapshot["values"][label] = {"value": value, "query": query}
                    break
            except Exception:
                continue
        if value is None:
            snapshot["missing"].append(label)
    if not snapshot["values"]:
        snapshot["status"] = "unavailable"
    return snapshot


def build_live_graph_payload(spans_df, run_id: str, window_id: str) -> dict[str, Any]:
    node_stats = (
        spans_df.groupby("service_name")
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
    total_requests = int(node_stats["request_count"].sum())
    edges_df = build_service_edges(spans_df)
    degree_in: dict[str, int] = {}
    degree_out: dict[str, int] = {}
    edges = []
    for edge in edges_df.groupby(["src_service", "dst_service"])["edge_count"].sum().reset_index().itertuples(index=False):
        src = str(getattr(edge, "src_service"))
        dst = str(getattr(edge, "dst_service"))
        weight = int(getattr(edge, "edge_count"))
        degree_out[src] = degree_out.get(src, 0) + weight
        degree_in[dst] = degree_in.get(dst, 0) + weight
        edges.append({"src": src, "dst": dst, "weight": weight})

    node_names = node_stats["service_name"].astype(str).tolist()
    node_roles = node_stats["service_role"].astype(str).tolist()
    node_to_idx = {name: idx for idx, name in enumerate(node_names)}
    x_rows: list[list[float]] = []
    for row in node_stats.itertuples(index=False):
        service_name = str(getattr(row, "service_name"))
        service_role = str(getattr(row, "service_role") or "unknown").lower()
        service_tier = str(getattr(row, "service_tier") or "unknown").lower()
        criticality = str(getattr(row, "criticality") or "unknown").lower()
        request_count = int(getattr(row, "request_count"))
        x_rows.append(
            [
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
            ]
        )

    edge_index = [
        [node_to_idx[e["src"]], node_to_idx[e["dst"]]]
        for e in edges
        if e["src"] in node_to_idx and e["dst"] in node_to_idx
    ]

    return {
        "graph_id": f"{run_id}__{window_id}",
        "node_names": node_names,
        "node_roles": node_roles,
        "x": x_rows,
        "edge_index": edge_index,
        "top_k": 3,
        "metadata": {"run_id": run_id, "window_id": window_id, "source": "live_jaeger"},
    }


def collect_live_inputs(
    system_id: str = DEFAULT_SYSTEM_ID,
    source_service: str = DEFAULT_SOURCE_SERVICE,
    jaeger_url: str = DEFAULT_JAEGER_URL,
    prometheus_url: str = DEFAULT_PROMETHEUS_URL,
    lookback_minutes: int = 2,
    query_limit: int = 150,
) -> dict[str, Any]:
    catalog_df = load_service_catalog(system_id)
    lookup = service_lookup(catalog_df)

    payload = fetch_jaeger_payload(
        jaeger_url=jaeger_url,
        source_service=source_service,
        lookback_minutes=lookback_minutes,
        query_limit=query_limit,
    )
    run_id = f"live_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    window_id = "live_recent"
    spans_df, traces_df = parse_jaeger_payload(
        payload=payload,
        run_id=run_id,
        window_id=window_id,
        service_metadata_lookup=lookup,
        system_id=system_id,
    )
    spans_df = clean_spans(spans_df)
    if spans_df.empty:
        raise ValueError("No live spans were returned from Jaeger in the selected time window.")

    window_df = build_window_features(spans_df, run_catalog_df=None)
    if window_df.empty:
        raise ValueError("Unable to build live window features from the current trace window.")
    window_row = window_df.iloc[0].to_dict()

    graph_payload = build_live_graph_payload(spans_df, run_id=run_id, window_id=window_id)
    metrics_snapshot = fetch_prometheus_snapshot(prometheus_url=prometheus_url)
    trace_snapshot = {
        "trace_count": int(traces_df["trace_id"].nunique()) if not traces_df.empty else 0,
        "span_count": int(len(spans_df)),
        "service_count": int(spans_df["service_name"].nunique()),
        "source_service": source_service,
        "jaeger_url": jaeger_url,
        "lookback_minutes": lookback_minutes,
    }
    return {
        "window": {
            "features": {k: float(v) for k, v in window_row.items() if k not in {"system_id", "run_id", "window_id", "window_phase"}},
            "metadata": {
                "run_id": run_id,
                "window_id": str(window_row.get("window_id", window_id)),
                "window_phase": str(window_row.get("window_phase", "steady")),
                "source": "live_jaeger",
            },
        },
        "graph": graph_payload,
        "trace_snapshot": trace_snapshot,
        "metrics_snapshot": metrics_snapshot,
    }
