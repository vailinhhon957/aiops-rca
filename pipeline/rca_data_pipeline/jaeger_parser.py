from __future__ import annotations

from collections import defaultdict
from typing import Any

import pandas as pd

from .service_catalog import map_service_metadata


def _tags_to_map(tags: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for tag in tags or []:
        out[tag.get("key")] = tag.get("value")
    return out


def _parent_span_id(span: dict[str, Any]) -> str | None:
    for ref in span.get("references", []) or []:
        if ref.get("refType") == "CHILD_OF":
            return ref.get("spanID")
    return None


def _status_code(tags: dict[str, Any]) -> str:
    for key in ("rpc.grpc.status_code", "http.status_code", "otel.status_code"):
        if key in tags:
            return str(tags[key])
    return "unknown"


def _is_error(tags: dict[str, Any]) -> int:
    grpc_code = tags.get("rpc.grpc.status_code")
    if grpc_code is not None:
        try:
            return 0 if int(grpc_code) == 0 else 1
        except Exception:
            pass
    http_code = tags.get("http.status_code")
    if http_code is not None:
        try:
            return 1 if int(http_code) >= 500 else 0
        except Exception:
            pass
    otel_code = tags.get("otel.status_code")
    if otel_code is not None:
        return 1 if str(otel_code).lower() == "error" else 0
    return 0


def _call_type(tags: dict[str, Any], operation_name: str) -> str:
    if tags.get("rpc.system") == "grpc":
        return "grpc"
    if "http.method" in tags or str(operation_name).upper().startswith(("GET ", "POST ", "PUT ", "DELETE ")):
        return "http"
    if "db.system" in tags or "db.statement" in tags:
        return "db"
    if "messaging.system" in tags:
        return "mq"
    if "cache" in str(operation_name).lower():
        return "cache"
    return "internal"


def parse_jaeger_payload(
    payload: dict[str, Any],
    run_id: str,
    window_id: str,
    service_metadata_lookup: dict[str, dict[str, object]] | None = None,
    system_id: str = "unknown",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    span_rows: list[dict[str, Any]] = []
    trace_rows: list[dict[str, Any]] = []
    service_metadata_lookup = service_metadata_lookup or {}

    for trace in payload.get("data", []) or []:
        process_map = trace.get("processes", {}) or {}
        trace_id = trace.get("traceID")
        trace_spans = trace.get("spans", []) or []

        for span in trace_spans:
            tags = _tags_to_map(span.get("tags", []))
            process = process_map.get(span.get("processID"), {})
            service_name = process.get("serviceName", "unknown")
            service_metadata = map_service_metadata(service_name, service_metadata_lookup)
            operation_name = span.get("operationName")

            start_time_us = int(span.get("startTime", 0))
            duration_us = int(span.get("duration", 0))
            duration_ms = duration_us / 1000.0
            start_ms = start_time_us / 1000.0
            end_ms = (start_time_us + duration_us) / 1000.0

            span_rows.append(
                {
                    "system_id": system_id,
                    "run_id": run_id,
                    "window_id": window_id,
                    "trace_id": trace_id,
                    "span_id": span.get("spanID"),
                    "parent_span_id": _parent_span_id(span),
                    "service_name": service_metadata["service_name"],
                    "service_role": service_metadata["service_role"],
                    "service_tier": service_metadata["service_tier"],
                    "criticality": service_metadata["criticality"],
                    "is_entrypoint": service_metadata["is_entrypoint"],
                    "is_stateful": service_metadata["is_stateful"],
                    "operation_name": operation_name,
                    "call_type": _call_type(tags, str(operation_name)),
                    "start_time_us": start_time_us,
                    "duration_us": duration_us,
                    "start_time_ms": start_ms,
                    "end_time_ms": end_ms,
                    "duration_ms": duration_ms,
                    "status_code": _status_code(tags),
                    "span_kind": str(tags.get("span.kind", "unknown")).lower(),
                    "is_error": _is_error(tags),
                }
            )

        if trace_spans:
            trace_service_names = set()
            trace_service_roles = set()
            for span in trace_spans:
                process = process_map.get(span.get("processID"), {})
                service_name = process.get("serviceName", "unknown")
                service_metadata = map_service_metadata(service_name, service_metadata_lookup)
                trace_service_names.add(service_metadata["service_name"])
                trace_service_roles.add(str(service_metadata["service_role"]))
            trace_rows.append(
                {
                    "system_id": system_id,
                    "run_id": run_id,
                    "window_id": window_id,
                    "trace_id": trace_id,
                    "trace_start_ms": min(int(s.get("startTime", 0)) for s in trace_spans) / 1000.0,
                    "trace_end_ms": max(int(s.get("startTime", 0)) + int(s.get("duration", 0)) for s in trace_spans)
                    / 1000.0,
                    "trace_duration_ms": (
                        max(int(s.get("startTime", 0)) + int(s.get("duration", 0)) for s in trace_spans)
                        - min(int(s.get("startTime", 0)) for s in trace_spans)
                    )
                    / 1000.0,
                    "span_count": len(trace_spans),
                    "service_count": len(trace_service_names),
                    "service_role_count": len(trace_service_roles),
                    "error_span_count": sum(_is_error(_tags_to_map(s.get("tags", []))) for s in trace_spans),
                }
            )

    spans_df = pd.DataFrame(span_rows)
    traces_df = pd.DataFrame(trace_rows)
    return spans_df, traces_df


def build_service_edges(spans_df: pd.DataFrame) -> pd.DataFrame:
    if spans_df.empty:
        return pd.DataFrame(
            columns=["system_id", "run_id", "window_id", "trace_id", "src_service", "dst_service", "edge_count"]
        )

    span_lookup = spans_df.set_index("span_id")[["system_id", "service_name", "trace_id"]].to_dict(orient="index")
    edge_counts: dict[tuple[str, str, str, str, str, str], int] = defaultdict(int)

    for row in spans_df.itertuples(index=False):
        parent_id = getattr(row, "parent_span_id")
        if not parent_id:
            continue
        parent = span_lookup.get(parent_id)
        if not parent:
            continue
        system_id = getattr(row, "system_id")
        src = parent["service_name"]
        dst = getattr(row, "service_name")
        key = (system_id, getattr(row, "run_id"), getattr(row, "window_id"), getattr(row, "trace_id"), src, dst)
        edge_counts[key] += 1

    return pd.DataFrame(
        [
            {
                "system_id": system_id,
                "run_id": run_id,
                "window_id": window_id,
                "trace_id": trace_id,
                "src_service": src,
                "dst_service": dst,
                "edge_count": count,
            }
            for (system_id, run_id, window_id, trace_id, src, dst), count in edge_counts.items()
        ]
    )
