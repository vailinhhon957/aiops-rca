from __future__ import annotations

import json
import math
from pathlib import Path

APP_SERVICES = [
    "frontend",
    "cartservice",
    "productcatalogservice",
    "currencyservice",
    "paymentservice",
    "shippingservice",
    "emailservice",
    "checkoutservice",
    "recommendationservice",
    "adservice",
]

SERVICE_TO_IDX = {name: idx for idx, name in enumerate(APP_SERVICES)}

SCENARIO_TO_ROOT_CAUSE = {
    "normal": "none",
    "memory_cart": "cartservice",
    "cpu_recommendation": "recommendationservice",
    "pod_kill_checkout": "checkoutservice",
}


def load_json(path: str | Path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def get_label(scenario: str) -> int:
    return 0 if scenario.lower() == "normal" else 1


def safe_std(values) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def tag_dict(span: dict) -> dict:
    result = {}
    for item in span.get("tags", []):
        key = item.get("key")
        if key is not None:
            result[key] = item.get("value")
    return result


def normalize_service_name(operation_name: str) -> str:
    if not operation_name:
        return "unknown"

    name = operation_name.strip("/")
    if name == "frontend":
        return "frontend"
    if name.startswith("grpc."):
        name = name[len("grpc.") :]
    left = name.split("/", 1)[0] if "/" in name else name
    if "." in left:
        return left.split(".")[-1].replace("Service", "").lower() + "service"
    return left.lower()


def extract_parent_ids(span: dict) -> list[str]:
    parents = []
    for ref in span.get("references", []):
        parent_id = ref.get("spanID")
        if parent_id:
            parents.append(parent_id)
    return parents


def summarize_service_features(spans: list[dict]):
    per_service = {
        service: {
            "span_count": 0.0,
            "latency_sum": 0.0,
            "max_latency": 0.0,
            "durations": [],
            "error_count": 0.0,
        }
        for service in APP_SERVICES
    }

    span_lookup = {}
    parent_edges = set()

    for span in spans:
        span_id = span.get("spanID")
        service = normalize_service_name(span.get("operationName", ""))
        if service not in SERVICE_TO_IDX:
            continue
        span_lookup[span_id] = service

    for span in spans:
        service = normalize_service_name(span.get("operationName", ""))
        if service not in SERVICE_TO_IDX:
            continue

        duration = float(span.get("duration", 0.0))
        tags = tag_dict(span)
        status_code = tags.get("http.status_code")
        if isinstance(status_code, str) and status_code.isdigit():
            status_code = int(status_code)

        has_error = tags.get("error") in (True, "true", "True", 1, "1")
        if isinstance(status_code, int) and status_code >= 400:
            has_error = True

        row = per_service[service]
        row["span_count"] += 1.0
        row["latency_sum"] += duration
        row["max_latency"] = max(row["max_latency"], duration)
        row["durations"].append(duration)
        row["error_count"] += 1.0 if has_error else 0.0

        for parent_id in extract_parent_ids(span):
            parent_service = span_lookup.get(parent_id)
            if not parent_service or parent_service == service:
                continue
            parent_edges.add((SERVICE_TO_IDX[parent_service], SERVICE_TO_IDX[service]))

    features = []
    active_services = set()
    total_service_spans = sum(row["span_count"] for row in per_service.values()) or 1.0
    for service in APP_SERVICES:
        row = per_service[service]
        count = row["span_count"]
        avg_latency = row["latency_sum"] / count if count else 0.0
        error_rate = row["error_count"] / count if count else 0.0
        span_ratio = count / total_service_spans if total_service_spans else 0.0
        features.append(
            [
                count,
                avg_latency,
                row["max_latency"],
                safe_std(row["durations"]),
                row["error_count"],
                error_rate,
                span_ratio,
            ]
        )
        if count > 0:
            active_services.add(service)

    if not parent_edges:
        parent_edges = {(idx, idx) for idx in range(len(APP_SERVICES))}

    return features, sorted(parent_edges), active_services

