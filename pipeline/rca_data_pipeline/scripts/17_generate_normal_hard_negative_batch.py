from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.rca_data_pipeline.config import (
    DEFAULT_SERVICE_CATALOG,
    DEFAULT_SYSTEM_FAMILY,
    DEFAULT_SYSTEM_ID,
    DEFAULT_TOPOLOGY_VERSION,
)
from pipeline.rca_data_pipeline.service_catalog import load_service_catalog, map_service_metadata, service_lookup


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

NORMAL_SCENARIO_IDS = {
    "ob_norm_low",
    "ob_norm_mid",
    "ob_norm_high",
}
DEFAULT_HARD_NEGATIVE_SCENARIO_IDS = [
    "ob_cpu_reco",
    "ob_mem_cart",
    "ob_lat_pay",
    "ob_lat_catalog",
    "ob_kill_checkout",
    "ob_kill_reco",
    "ob_scale_catalog",
]
HARD_NEGATIVE_VARIANTS = {
    "compact": [("low", "medium")],
    "balanced": [("low", "medium"), ("medium", "low"), ("low", "high")],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate metadata rows for extra normal runs and benign hard-negative perturbations."
    )
    parser.add_argument(
        "--scenario-catalog",
        type=Path,
        default=ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "scenario_catalog_online_boutique.csv",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "normal_hard_negative_batch.csv",
    )
    parser.add_argument("--train-runs", type=int, default=2)
    parser.add_argument("--val-runs", type=int, default=1)
    parser.add_argument("--test-runs", type=int, default=1)
    parser.add_argument(
        "--hard-negative-profile",
        choices=sorted(HARD_NEGATIVE_VARIANTS),
        default="balanced",
    )
    parser.add_argument(
        "--hard-negative-scenarios",
        default=",".join(DEFAULT_HARD_NEGATIVE_SCENARIO_IDS),
        help="Comma-separated scenario_ids to convert into hard negatives.",
    )
    parser.add_argument("--system-id", type=str, default=DEFAULT_SYSTEM_ID)
    parser.add_argument("--system-family", type=str, default=DEFAULT_SYSTEM_FAMILY)
    parser.add_argument("--topology-version", type=str, default=DEFAULT_TOPOLOGY_VERSION)
    parser.add_argument("--service-catalog", type=Path, default=DEFAULT_SERVICE_CATALOG)
    return parser.parse_args()


def normalize_text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def parse_csv_list(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def split_sequence(train_runs: int, val_runs: int, test_runs: int) -> list[str]:
    sequence = (["train"] * max(train_runs, 0)) + (["val"] * max(val_runs, 0)) + (["test"] * max(test_runs, 0))
    if not sequence:
        raise ValueError("At least one split count must be positive.")
    return sequence


def load_scenarios(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def normal_runtime_controls(load_profile: str) -> dict[str, str]:
    warmup_map = {"low": "45", "medium": "60", "high": "75"}
    cooldown_map = {"low": "30", "medium": "45", "high": "60"}
    query_map = {"low": "350", "medium": "550", "high": "800"}
    return {
        "warmup_seconds": warmup_map.get(load_profile, "60"),
        "cooldown_seconds": cooldown_map.get(load_profile, "45"),
        "fault_duration_seconds": "",
        "query_limit": query_map.get(load_profile, "550"),
        "query_lookback": "custom-window",
    }


def hard_negative_runtime_controls(fault_type: str, severity: str, load_profile: str) -> dict[str, str]:
    controls = {
        "warmup_seconds": {"low": "45", "medium": "60", "high": "75"}.get(load_profile, "60"),
        "cooldown_seconds": {"low": "30", "medium": "40", "high": "50"}.get(load_profile, "40"),
        "fault_duration_seconds": {"low": "20", "medium": "35", "high": "45"}.get(severity, "35"),
        "query_limit": str({"low": 350, "medium": 550, "high": 800}.get(load_profile, 550)),
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
    if fault_type == "cpu-stress":
        controls["cpu_request_m"] = {"low": "140", "medium": "110", "high": "90"}.get(severity, "110")
        controls["cpu_limit_m"] = {"low": "200", "medium": "160", "high": "130"}.get(severity, "160")
    elif fault_type == "memory-stress":
        controls["memory_request_mib"] = {"low": "160", "medium": "128", "high": "112"}.get(severity, "128")
        controls["memory_limit_mib"] = {"low": "224", "medium": "192", "high": "160"}.get(severity, "192")
    elif fault_type == "latency-injection":
        controls["latency_delay_seconds"] = {"low": "1", "medium": "2", "high": "2"}.get(severity, "2")
    elif fault_type == "replica-drop":
        controls["replica_drop_to"] = "1"
    elif fault_type == "pod-kill":
        controls["pod_kill_repeats"] = "1"
        controls["pod_kill_interval_seconds"] = "20"
    return controls


def build_row(
    *,
    scenario_id: str,
    trace_suffix: str,
    label: int,
    sample_class: str,
    phase_policy: str,
    fault_family: str,
    fault_type: str,
    fault_target_service: str,
    fault_target_role: str,
    source_service: str,
    source_service_role: str,
    severity: str,
    load_profile: str,
    split_tag: str,
    notes: str,
    runtime_controls: dict[str, str],
    args: argparse.Namespace,
) -> dict[str, str]:
    run_id = f"{scenario_id}_{trace_suffix}"
    return {
        "run_id": run_id,
        "trace_file": f"{run_id}.json",
        "label": str(label),
        "sample_class": sample_class,
        "phase_policy": phase_policy,
        "fault_family": fault_family,
        "fault_type": fault_type,
        "root_cause_service": fault_target_service if fault_target_service not in {"", "none"} else "none",
        "fault_target_service": fault_target_service,
        "fault_target_role": fault_target_role,
        "source_service": source_service,
        "source_service_role": source_service_role,
        "system_id": args.system_id,
        "system_family": args.system_family,
        "topology_version": args.topology_version,
        "experiment_group": sample_class,
        "chaos_name": normalize_text(scenario_id, "none").lower(),
        "chaos_kind": "none" if fault_type == "none" else "benign-perturbation",
        "target_service": fault_target_service,
        "target_pod": "",
        "target_container": fault_target_service if fault_target_service not in {"", "none"} else "",
        "severity": severity,
        "load_profile": load_profile,
        "split_tag": split_tag,
        "start_time": "",
        "fault_start_time": "",
        "fault_end_time": "",
        "end_time": "",
        "export_duration_ms": "",
        "query_limit": runtime_controls.get("query_limit", ""),
        "query_lookback": runtime_controls.get("query_lookback", "custom-window"),
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
        "warmup_seconds": runtime_controls.get("warmup_seconds", ""),
        "cooldown_seconds": runtime_controls.get("cooldown_seconds", ""),
        "fault_duration_seconds": runtime_controls.get("fault_duration_seconds", ""),
        "replica_drop_to": runtime_controls.get("replica_drop_to", ""),
        "cpu_request_m": runtime_controls.get("cpu_request_m", ""),
        "cpu_limit_m": runtime_controls.get("cpu_limit_m", ""),
        "memory_request_mib": runtime_controls.get("memory_request_mib", ""),
        "memory_limit_mib": runtime_controls.get("memory_limit_mib", ""),
        "latency_delay_seconds": runtime_controls.get("latency_delay_seconds", ""),
        "pod_kill_repeats": runtime_controls.get("pod_kill_repeats", ""),
        "pod_kill_interval_seconds": runtime_controls.get("pod_kill_interval_seconds", ""),
    }


def main() -> None:
    args = parse_args()
    scenarios = load_scenarios(args.scenario_catalog)
    split_tags = split_sequence(args.train_runs, args.val_runs, args.test_runs)
    hard_negative_ids = parse_csv_list(args.hard_negative_scenarios)
    catalog_lookup = service_lookup(load_service_catalog(args.service_catalog))

    rows: list[dict[str, str]] = []

    for scenario in scenarios:
        scenario_id = normalize_text(scenario.get("scenario_id"))
        fault_type = normalize_text(scenario.get("fault_type"), "none").lower()
        source_service = normalize_text(scenario.get("source_service"), "frontend").lower()
        source_service_role = map_service_metadata(source_service, catalog_lookup)["service_role"]
        fault_target_service = normalize_text(scenario.get("fault_target_service"), "none").lower()
        fault_target_role = normalize_text(scenario.get("fault_target_role"), "none").lower()
        fault_family = normalize_text(scenario.get("fault_family"), "none").lower()

        if scenario_id in NORMAL_SCENARIO_IDS:
            severity = normalize_text(scenario.get("severity"), "medium").lower()
            load_profile = normalize_text(scenario.get("load_profile"), "medium").lower()
            runtime_controls = normal_runtime_controls(load_profile)
            for idx, split_tag in enumerate(split_tags, start=1):
                suffix = f"normal_{idx:03d}"
                rows.append(
                    build_row(
                        scenario_id=scenario_id,
                        trace_suffix=suffix,
                        label=0,
                        sample_class="normal",
                        phase_policy="steady",
                        fault_family="none",
                        fault_type="none",
                        fault_target_service="none",
                        fault_target_role="none",
                        source_service=source_service,
                        source_service_role=source_service_role,
                        severity=severity,
                        load_profile=load_profile,
                        split_tag=split_tag,
                        notes=f"{normalize_text(scenario.get('notes'), 'normal baseline')}; auto-generated normal batch",
                        runtime_controls=runtime_controls,
                        args=args,
                    )
                )
            continue

        if scenario_id not in hard_negative_ids or fault_type == "none":
            continue

        run_index = 1
        for severity, load_profile in HARD_NEGATIVE_VARIANTS[args.hard_negative_profile]:
            runtime_controls = hard_negative_runtime_controls(fault_type=fault_type, severity=severity, load_profile=load_profile)
            for split_tag in split_tags:
                suffix = f"hn_sev{severity}_load{load_profile}_{run_index:03d}"
                rows.append(
                    build_row(
                        scenario_id=scenario_id,
                        trace_suffix=suffix,
                        label=0,
                        sample_class="hard-negative",
                        phase_policy="fault-phases",
                        fault_family=fault_family,
                        fault_type=fault_type,
                        fault_target_service=fault_target_service,
                        fault_target_role=fault_target_role,
                        source_service=source_service,
                        source_service_role=source_service_role,
                        severity=severity,
                        load_profile=load_profile,
                        split_tag=split_tag,
                        notes=(
                            f"{normalize_text(scenario.get('notes'), 'benign perturbation')}; "
                            "auto-generated hard negative; label=0; "
                            f"severity={severity}; load={load_profile}"
                        ),
                        runtime_controls=runtime_controls,
                        args=args,
                    )
                )
                run_index += 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Generated {len(rows)} rows")
    print(f"Output: {args.output}")
    print(f"Normal scenarios: {len(NORMAL_SCENARIO_IDS)}")
    print(f"Hard-negative scenarios: {len(hard_negative_ids)}")


if __name__ == "__main__":
    main()
