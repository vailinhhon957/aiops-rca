from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from time import perf_counter
from urllib.parse import urlencode
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.rca_data_pipeline.config import LEGACY_DATASET_ROOT


DEFAULT_METADATA_FILE = ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "batch1_fill_only.csv"
DEFAULT_OUTPUT_DIR = LEGACY_DATASET_ROOT / "raw" / "collected"
EXTRA_COLUMNS = [
    "export_duration_ms",
    "query_limit",
    "query_lookback",
    "trace_count",
    "span_count_total",
    "avg_spans_per_trace",
    "unique_service_count",
    "unique_services",
    "root_cause_trace_hits",
    "health_trace_count",
    "otel_export_trace_count",
    "business_trace_count",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export one run worth of traces from Jaeger using run metadata.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--metadata-file", type=Path, default=DEFAULT_METADATA_FILE)
    parser.add_argument("--jaeger-url", default="http://127.0.0.1:16686")
    parser.add_argument("--service", default="frontend")
    parser.add_argument("--query-limit", type=int, default=500)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def load_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return list(reader.fieldnames or []), rows


def save_rows(path: Path, header: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def positive_int_from_value(value: object, default: int) -> int:
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    try:
        parsed = int(float(text))
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def iso_to_unix_micros(value: str) -> int:
    dt = datetime.fromisoformat(value)
    return int(dt.timestamp() * 1_000_000)


def load_run_row(run_id: str, metadata_file: Path) -> tuple[list[str], list[dict[str, str]], dict[str, str]]:
    header, rows = load_rows(metadata_file)
    for row in rows:
        if row.get("run_id") == run_id:
            return header, rows, row
    raise ValueError(f"Run id not found: {run_id}")


def fetch_jaeger_payload(jaeger_url: str, service: str, start_micros: int, end_micros: int, limit: int) -> tuple[dict, float]:
    params = urlencode(
        {
            "service": service,
            "start": start_micros,
            "end": end_micros,
            "limit": limit,
        }
    )
    url = f"{jaeger_url.rstrip('/')}/api/traces?{params}"
    started = perf_counter()
    with urlopen(url, timeout=120) as response:
        payload = json.loads(response.read().decode("utf-8-sig"))
    elapsed_ms = (perf_counter() - started) * 1000.0
    return payload, elapsed_ms


def summarize_payload(payload: dict, fault_target_service: str) -> dict[str, str]:
    traces = payload.get("data", []) or []
    trace_count = len(traces)
    span_count_total = 0
    unique_services: set[str] = set()
    root_cause_trace_hits = 0
    health_trace_count = 0
    otel_export_trace_count = 0

    for trace in traces:
        spans = trace.get("spans", []) or []
        processes = trace.get("processes", {}) or {}
        span_count_total += len(spans)
        trace_services = {
            str(processes.get(span.get("processID"), {}).get("serviceName", "unknown")).strip().lower()
            for span in spans
        }
        unique_services.update(trace_services)
        if fault_target_service and fault_target_service in trace_services:
            root_cause_trace_hits += 1
        if any("_healthz" in str(span.get("operationName", "")).lower() for span in spans):
            health_trace_count += 1
        if any("otel" in service or "jaeger" in service for service in trace_services):
            otel_export_trace_count += 1

    business_trace_count = max(trace_count - health_trace_count - otel_export_trace_count, 0)
    avg_spans_per_trace = (span_count_total / trace_count) if trace_count else 0.0

    return {
        "trace_count": str(trace_count),
        "span_count_total": str(span_count_total),
        "avg_spans_per_trace": f"{avg_spans_per_trace:.2f}",
        "unique_service_count": str(len(unique_services)),
        "unique_services": ";".join(sorted(unique_services)),
        "root_cause_trace_hits": str(root_cause_trace_hits),
        "health_trace_count": str(health_trace_count),
        "otel_export_trace_count": str(otel_export_trace_count),
        "business_trace_count": str(business_trace_count),
    }


def ensure_header_columns(header: list[str]) -> list[str]:
    updated = list(header)
    for column in EXTRA_COLUMNS:
        if column not in updated:
            updated.append(column)
    return updated


def main() -> None:
    args = parse_args()
    header, rows, row = load_run_row(args.run_id, args.metadata_file)

    start_time = row.get("start_time", "").strip()
    end_time = row.get("end_time", "").strip()
    if not start_time or not end_time:
        raise ValueError("This run does not have start_time/end_time yet. Run the collection step first.")

    service = row.get("source_service", "").strip().lower() or args.service
    trace_file = row.get("trace_file", "").strip() or f"{args.run_id}.json"
    fault_target_service = row.get("fault_target_service", "").strip().lower()
    query_limit = positive_int_from_value(row.get("query_limit"), args.query_limit)

    start_micros = iso_to_unix_micros(start_time)
    end_micros = iso_to_unix_micros(end_time)
    payload, export_duration_ms = fetch_jaeger_payload(
        jaeger_url=args.jaeger_url,
        service=service,
        start_micros=start_micros,
        end_micros=end_micros,
        limit=query_limit,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / trace_file
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    header = ensure_header_columns(header)
    stats = summarize_payload(payload, fault_target_service=fault_target_service)
    for key, value in stats.items():
        row[key] = value
    row["export_duration_ms"] = f"{export_duration_ms:.2f}"
    row["query_limit"] = str(query_limit)
    row["query_lookback"] = "custom-window"

    for idx, existing_row in enumerate(rows):
        if existing_row.get("run_id") == args.run_id:
            rows[idx] = row
            break
    save_rows(args.metadata_file, header, rows)

    print(f"Saved traces: {output_path}")
    print(f"trace_count={row.get('trace_count', '0')}")
    print(f"span_count_total={row.get('span_count_total', '0')}")
    print(f"unique_service_count={row.get('unique_service_count', '0')}")
    print(f"metadata_file={args.metadata_file}")


if __name__ == "__main__":
    main()
