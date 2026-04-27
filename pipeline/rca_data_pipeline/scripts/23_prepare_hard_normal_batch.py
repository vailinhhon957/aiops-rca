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
DEFAULT_OUTPUT = ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "hard_normal_batch.csv"

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
    parser = argparse.ArgumentParser(description="Generate extra hard-normal runs (label=0, no fault).")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--jaeger-url", default="http://127.0.0.1:16686")
    parser.add_argument("--mode", choices=["generate-only", "collect-export", "all", "rebuild-only"], default="generate-only")
    parser.add_argument("--clean", action="store_true")
    return parser.parse_args()


def build_row(run_id: str, severity: str, load_profile: str, split_tag: str, query_limit: str, warmup: str, cooldown: str, notes: str) -> dict[str, str]:
    return {
        "run_id": run_id,
        "trace_file": f"{run_id}.json",
        "label": "0",
        "sample_class": "normal",
        "phase_policy": "steady",
        "fault_family": "none",
        "fault_type": "none",
        "root_cause_service": "none",
        "fault_target_service": "none",
        "fault_target_role": "none",
        "source_service": "frontend",
        "source_service_role": "entrypoint",
        "system_id": DEFAULT_SYSTEM_ID,
        "system_family": DEFAULT_SYSTEM_FAMILY,
        "topology_version": DEFAULT_TOPOLOGY_VERSION,
        "experiment_group": "normal",
        "chaos_name": "none",
        "chaos_kind": "none",
        "target_service": "none",
        "target_pod": "",
        "target_container": "",
        "severity": severity,
        "load_profile": load_profile,
        "split_tag": split_tag,
        "start_time": "",
        "fault_start_time": "",
        "fault_end_time": "",
        "end_time": "",
        "export_duration_ms": "",
        "query_limit": query_limit,
        "query_lookback": "custom-window",
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
        "warmup_seconds": warmup,
        "cooldown_seconds": cooldown,
        "fault_duration_seconds": "",
        "replica_drop_to": "",
        "cpu_request_m": "",
        "cpu_limit_m": "",
        "memory_request_mib": "",
        "memory_limit_mib": "",
        "latency_delay_seconds": "",
        "pod_kill_repeats": "",
        "pod_kill_interval_seconds": "",
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
        build_row("ob_norm_burst_hard_001", "high", "burst", "train", "1000", "90", "60", "hard normal: burst traffic without faults"),
        build_row("ob_norm_very_high_hard_001", "high", "very_high", "train", "900", "80", "55", "hard normal: very high steady traffic without faults"),
        build_row("ob_norm_medium_high_hard_001", "medium", "medium_high", "train", "650", "65", "45", "hard normal: medium_high traffic with wider latency spread"),
        build_row("ob_norm_low_medium_hard_001", "medium", "low_medium", "val", "450", "50", "35", "hard normal: low_medium traffic but still healthy"),
        build_row("ob_norm_idle_hard_001", "low", "idle", "test", "200", "30", "20", "hard normal: idle but realistic baseline"),
        build_row("ob_norm_very_low_hard_001", "low", "very_low", "test", "250", "35", "25", "hard normal: very low noisy baseline"),
    ]

    write_rows(args.output, rows)
    print(f"Generated hard-normal batch: {args.output}")
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
