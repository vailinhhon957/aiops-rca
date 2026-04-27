from __future__ import annotations

import argparse
import csv
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from pipeline.rca_data_pipeline.config import DEFAULT_SYSTEM_FAMILY, DEFAULT_SYSTEM_ID, DEFAULT_TOPOLOGY_VERSION


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "light_anomaly_batch.csv"

HEADER = [
    "run_id","trace_file","label","sample_class","phase_policy","fault_family","fault_type","root_cause_service",
    "fault_target_service","fault_target_role","source_service","source_service_role","system_id","system_family",
    "topology_version","experiment_group","chaos_name","chaos_kind","target_service","target_pod","target_container",
    "severity","load_profile","split_tag","start_time","fault_start_time","fault_end_time","end_time",
    "export_duration_ms","query_limit","query_lookback","trace_count","span_count_total","avg_spans_per_trace",
    "unique_service_count","unique_services","root_cause_trace_hits","health_trace_count","otel_export_trace_count",
    "business_trace_count","notes","warmup_seconds","cooldown_seconds","fault_duration_seconds","replica_drop_to",
    "cpu_request_m","cpu_limit_m","memory_request_mib","memory_limit_mib","latency_delay_seconds","pod_kill_repeats",
    "pod_kill_interval_seconds",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate extra light-anomaly runs (label=1, subtle/short faults).")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--jaeger-url", default="http://127.0.0.1:16686")
    parser.add_argument("--mode", choices=["generate-only", "collect-export", "all", "rebuild-only"], default="generate-only")
    parser.add_argument("--clean", action="store_true")
    return parser.parse_args()


def build_row(
    run_id: str,
    fault_family: str,
    fault_type: str,
    service: str,
    role: str,
    split_tag: str,
    notes: str,
    extra: dict[str, str],
) -> dict[str, str]:
    runtime = {
        "warmup_seconds": "55",
        "cooldown_seconds": "35",
        "fault_duration_seconds": "20",
        "query_limit": "500",
        "query_lookback": "custom-window",
        "replica_drop_to": "",
        "cpu_request_m": "",
        "cpu_limit_m": "",
        "memory_request_mib": "",
        "memory_limit_mib": "",
        "latency_delay_seconds": "",
        "pod_kill_repeats": "",
        "pod_kill_interval_seconds": "",
    }
    runtime.update(extra)
    chaos_kind = {
        "latency-injection": "network",
        "timeout": "network",
        "pod-kill": "pod",
        "cpu-stress": "stress",
        "memory-stress": "stress",
    }.get(fault_type, "app")
    chaos_name = {
        "latency-injection": "delay",
        "timeout": "loss",
        "pod-kill": "kill",
        "cpu-stress": "stress-ng",
        "memory-stress": "stress-ng",
    }.get(fault_type, fault_type)
    return {
        "run_id": run_id,
        "trace_file": f"{run_id}.json",
        "label": "1",
        "sample_class": "fault",
        "phase_policy": "fault-phases",
        "fault_family": fault_family,
        "fault_type": fault_type,
        "root_cause_service": service,
        "fault_target_service": service,
        "fault_target_role": role,
        "source_service": "frontend",
        "source_service_role": "entrypoint",
        "system_id": DEFAULT_SYSTEM_ID,
        "system_family": DEFAULT_SYSTEM_FAMILY,
        "topology_version": DEFAULT_TOPOLOGY_VERSION,
        "experiment_group": "light-anomaly",
        "chaos_name": chaos_name,
        "chaos_kind": chaos_kind,
        "target_service": service,
        "target_pod": "",
        "target_container": service,
        "severity": "low",
        "load_profile": "medium",
        "split_tag": split_tag,
        "start_time": "",
        "fault_start_time": "",
        "fault_end_time": "",
        "end_time": "",
        "export_duration_ms": "",
        "query_limit": runtime["query_limit"],
        "query_lookback": runtime["query_lookback"],
        "trace_count": "",
        "span_count_total": "",
        "avg_spans_per_trace": "",
        "unique_service_count": "",
        "unique_services": "",
        "root_cause_trace_hits": "",
        "health_trace_count": "",
        "otel_export_trace_count": "",
        "business_trace_count": "",
        "notes": notes,
        "warmup_seconds": runtime["warmup_seconds"],
        "cooldown_seconds": runtime["cooldown_seconds"],
        "fault_duration_seconds": runtime["fault_duration_seconds"],
        "replica_drop_to": runtime["replica_drop_to"],
        "cpu_request_m": runtime["cpu_request_m"],
        "cpu_limit_m": runtime["cpu_limit_m"],
        "memory_request_mib": runtime["memory_request_mib"],
        "memory_limit_mib": runtime["memory_limit_mib"],
        "latency_delay_seconds": runtime["latency_delay_seconds"],
        "pod_kill_repeats": runtime["pod_kill_repeats"],
        "pod_kill_interval_seconds": runtime["pod_kill_interval_seconds"],
    }


def write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    rows = [
        build_row("ob_lat_pay_light_001", "latency", "latency-injection", "paymentservice", "payment", "train", "light anomaly: short payment latency spike", {"latency_delay_seconds": "1"}),
        build_row("ob_lat_pay_light_002", "latency", "latency-injection", "paymentservice", "payment", "val", "light anomaly: short payment latency spike", {"latency_delay_seconds": "1"}),
        build_row("ob_timeout_currency_light_001", "latency", "timeout", "currencyservice", "currency", "train", "light anomaly: short currency timeout", {}),
        build_row("ob_timeout_currency_light_002", "latency", "timeout", "currencyservice", "currency", "test", "light anomaly: short currency timeout", {}),
        build_row("ob_kill_checkout_light_001", "availability", "pod-kill", "checkoutservice", "checkout", "train", "light anomaly: single short checkout pod kill", {"pod_kill_repeats": "1", "pod_kill_interval_seconds": "20"}),
        build_row("ob_cpu_reco_light_001", "resource", "cpu-stress", "recommendationservice", "recommendation", "val", "light anomaly: mild recommendation cpu pressure", {"cpu_request_m": "80", "cpu_limit_m": "120"}),
        build_row("ob_mem_cart_light_001", "resource", "memory-stress", "cartservice", "cart", "test", "light anomaly: mild cart memory pressure", {"memory_request_mib": "128", "memory_limit_mib": "192"}),
        build_row("ob_scale_catalog_light_001", "availability", "replica-drop", "productcatalogservice", "catalog", "train", "light anomaly: catalog replica drop but recoverable", {"replica_drop_to": "1"}),
    ]

    write_rows(args.output, rows)
    print(f"Generated light-anomaly batch: {args.output}")
    print(f"total_rows={len(rows)}")

    if args.mode != "generate-only":
        batch_args = [
            "--metadata-file", str(args.output),
            "--namespace", args.namespace,
            "--jaeger-url", args.jaeger_url,
            "--mode", args.mode,
        ]
        if args.clean:
            batch_args.append("--clean")
        subprocess.run([sys.executable, str(SCRIPT_DIR / "13_run_batch_dataset.py"), *batch_args], check=True)


if __name__ == "__main__":
    main()
