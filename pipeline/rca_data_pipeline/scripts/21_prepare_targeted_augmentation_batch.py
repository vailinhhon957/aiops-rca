from __future__ import annotations

import argparse
import csv
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from pipeline.rca_data_pipeline.config import (
    DEFAULT_SYSTEM_FAMILY,
    DEFAULT_SYSTEM_ID,
    DEFAULT_TOPOLOGY_VERSION,
)


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "targeted_augmentation_batch.csv"

HEADER = [
    "run_id",
    "trace_file",
    "label",
    "sample_class",
    "phase_policy",
    "fault_family",
    "fault_type",
    "root_cause_service",
    "fault_target_service",
    "fault_target_role",
    "source_service",
    "source_service_role",
    "system_id",
    "system_family",
    "topology_version",
    "experiment_group",
    "chaos_name",
    "chaos_kind",
    "target_service",
    "target_pod",
    "target_container",
    "severity",
    "load_profile",
    "split_tag",
    "start_time",
    "fault_start_time",
    "fault_end_time",
    "end_time",
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
    "notes",
    "warmup_seconds",
    "cooldown_seconds",
    "fault_duration_seconds",
    "replica_drop_to",
    "cpu_request_m",
    "cpu_limit_m",
    "memory_request_mib",
    "memory_limit_mib",
    "latency_delay_seconds",
    "pod_kill_repeats",
    "pod_kill_interval_seconds",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a focused augmentation batch to improve weak RCA classes and anomaly negatives."
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--jaeger-url", default="http://127.0.0.1:16686")
    parser.add_argument(
        "--mode",
        choices=["generate-only", "collect-export", "all", "rebuild-only"],
        default="generate-only",
    )
    parser.add_argument("--clean", action="store_true")
    return parser.parse_args()


def write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        writer.writeheader()
        writer.writerows(rows)


def build_row(
    *,
    run_id: str,
    label: int,
    sample_class: str,
    fault_family: str,
    fault_type: str,
    fault_target_service: str,
    fault_target_role: str,
    severity: str,
    load_profile: str,
    split_tag: str,
    notes: str,
    chaos_kind: str,
    chaos_name: str,
    runtime: dict[str, str],
) -> dict[str, str]:
    return {
        "run_id": run_id,
        "trace_file": f"{run_id}.json",
        "label": str(label),
        "sample_class": sample_class,
        "phase_policy": "steady" if fault_type == "none" else "fault-phases",
        "fault_family": fault_family,
        "fault_type": fault_type,
        "root_cause_service": fault_target_service if label == 1 else ("none" if fault_type == "none" else fault_target_service),
        "fault_target_service": fault_target_service,
        "fault_target_role": fault_target_role,
        "source_service": "frontend",
        "source_service_role": "entrypoint",
        "system_id": DEFAULT_SYSTEM_ID,
        "system_family": DEFAULT_SYSTEM_FAMILY,
        "topology_version": DEFAULT_TOPOLOGY_VERSION,
        "experiment_group": sample_class,
        "chaos_name": chaos_name,
        "chaos_kind": chaos_kind,
        "target_service": fault_target_service,
        "target_pod": "",
        "target_container": "" if fault_target_service in {"", "none"} else fault_target_service,
        "severity": severity,
        "load_profile": load_profile,
        "split_tag": split_tag,
        "start_time": "",
        "fault_start_time": "",
        "fault_end_time": "",
        "end_time": "",
        "export_duration_ms": "",
        "query_limit": runtime.get("query_limit", ""),
        "query_lookback": runtime.get("query_lookback", "custom-window"),
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
        "warmup_seconds": runtime.get("warmup_seconds", ""),
        "cooldown_seconds": runtime.get("cooldown_seconds", ""),
        "fault_duration_seconds": runtime.get("fault_duration_seconds", ""),
        "replica_drop_to": runtime.get("replica_drop_to", ""),
        "cpu_request_m": runtime.get("cpu_request_m", ""),
        "cpu_limit_m": runtime.get("cpu_limit_m", ""),
        "memory_request_mib": runtime.get("memory_request_mib", ""),
        "memory_limit_mib": runtime.get("memory_limit_mib", ""),
        "latency_delay_seconds": runtime.get("latency_delay_seconds", ""),
        "pod_kill_repeats": runtime.get("pod_kill_repeats", ""),
        "pod_kill_interval_seconds": runtime.get("pod_kill_interval_seconds", ""),
    }


def positive_runtime(query_limit: str = "650") -> dict[str, str]:
    return {
        "warmup_seconds": "60",
        "cooldown_seconds": "45",
        "fault_duration_seconds": "75",
        "query_limit": query_limit,
        "query_lookback": "custom-window",
    }


def main() -> None:
    args = parse_args()
    rows: list[dict[str, str]] = []

    # 12 targeted positive runs for weak RCA classes.
    positive_specs = [
        ("ob_lat_pay_aug", "latency", "latency-injection", "paymentservice", "payment", "network", "delay", {"latency_delay_seconds": "3"}),
        ("ob_timeout_currency_aug", "latency", "timeout", "currencyservice", "currency", "network", "loss", {}),
        ("ob_kill_checkout_aug", "availability", "pod-kill", "checkoutservice", "checkout", "pod", "kill", {"pod_kill_repeats": "1", "pod_kill_interval_seconds": "15"}),
    ]
    positive_splits = ["train", "train", "val", "test"]
    for scenario_id, fault_family, fault_type, service, role, chaos_kind, chaos_name, extra_runtime in positive_specs:
        for index, split_tag in enumerate(positive_splits, start=1):
            runtime = positive_runtime()
            runtime.update(extra_runtime)
            rows.append(
                build_row(
                    run_id=f"{scenario_id}_{index:03d}",
                    label=1,
                    sample_class="fault",
                    fault_family=fault_family,
                    fault_type=fault_type,
                    fault_target_service=service,
                    fault_target_role=role,
                    severity="medium",
                    load_profile="medium",
                    split_tag=split_tag,
                    notes=f"targeted augmentation for weak RCA class: {service}",
                    chaos_kind=chaos_kind,
                    chaos_name=chaos_name,
                    runtime=runtime,
                )
            )

    # 4 extra normal runs on harder benign traffic profiles.
    normal_specs = [
        ("ob_norm_medium_high_aug_001", "medium", "medium_high", "train", "extra normal baseline on medium_high traffic"),
        ("ob_norm_very_high_aug_001", "high", "very_high", "train", "extra normal baseline on very_high traffic"),
        ("ob_norm_burst_aug_001", "high", "burst", "val", "extra normal baseline on burst traffic"),
        ("ob_norm_idle_aug_001", "low", "idle", "test", "extra normal baseline on idle traffic"),
    ]
    normal_query_limits = {
        "medium_high": "650",
        "very_high": "900",
        "burst": "1000",
        "idle": "200",
    }
    normal_warmup = {"medium_high": "65", "very_high": "80", "burst": "90", "idle": "30"}
    normal_cooldown = {"medium_high": "45", "very_high": "55", "burst": "60", "idle": "20"}
    for run_id, severity, load_profile, split_tag, notes in normal_specs:
        rows.append(
            build_row(
                run_id=run_id,
                label=0,
                sample_class="normal",
                fault_family="none",
                fault_type="none",
                fault_target_service="none",
                fault_target_role="none",
                severity=severity,
                load_profile=load_profile,
                split_tag=split_tag,
                notes=notes,
                chaos_kind="none",
                chaos_name="none",
                runtime={
                    "warmup_seconds": normal_warmup[load_profile],
                    "cooldown_seconds": normal_cooldown[load_profile],
                    "fault_duration_seconds": "",
                    "query_limit": normal_query_limits[load_profile],
                    "query_lookback": "custom-window",
                },
            )
        )

    # 4 extra hard negatives to reduce false positives.
    hard_negative_specs = [
        ("ob_lat_pay_hn_aug_001", "latency", "latency-injection", "paymentservice", "payment", "low", "medium", "train", "benign payment latency hard negative", {"latency_delay_seconds": "1"}),
        ("ob_cpu_reco_hn_aug_001", "resource", "cpu-stress", "recommendationservice", "recommendation", "low", "medium", "train", "benign recommendation cpu stress hard negative", {"cpu_request_m": "140", "cpu_limit_m": "200"}),
        ("ob_mem_cart_hn_aug_001", "resource", "memory-stress", "cartservice", "cart", "low", "medium", "val", "benign cart memory stress hard negative", {"memory_request_mib": "160", "memory_limit_mib": "224"}),
        ("ob_scale_catalog_hn_aug_001", "availability", "replica-drop", "productcatalogservice", "catalog", "low", "medium", "test", "benign catalog replica drop hard negative", {"replica_drop_to": "1"}),
    ]
    for run_id, fault_family, fault_type, service, role, severity, load_profile, split_tag, notes, extra_runtime in hard_negative_specs:
        runtime = {
            "warmup_seconds": "60",
            "cooldown_seconds": "40",
            "fault_duration_seconds": "20",
            "query_limit": "550",
            "query_lookback": "custom-window",
        }
        runtime.update(extra_runtime)
        rows.append(
            build_row(
                run_id=run_id,
                label=0,
                sample_class="hard-negative",
                fault_family=fault_family,
                fault_type=fault_type,
                fault_target_service=service,
                fault_target_role=role,
                severity=severity,
                load_profile=load_profile,
                split_tag=split_tag,
                notes=notes,
                chaos_kind="benign-perturbation",
                chaos_name=fault_type,
                runtime=runtime,
            )
        )

    write_rows(args.output, rows)
    print(f"Generated targeted augmentation batch: {args.output}")
    print(f"total_rows={len(rows)}")
    print("breakdown=12 positive, 4 normal, 4 hard-negative")

    if args.mode != "generate-only":
        batch_args = [
            "--metadata-file",
            str(args.output),
            "--namespace",
            args.namespace,
            "--jaeger-url",
            args.jaeger_url,
            "--mode",
            args.mode if args.mode != "generate-only" else "all",
        ]
        if args.clean:
            batch_args.append("--clean")
        subprocess.run([sys.executable, str(SCRIPT_DIR / "13_run_batch_dataset.py"), *batch_args], check=True)


if __name__ == "__main__":
    main()
