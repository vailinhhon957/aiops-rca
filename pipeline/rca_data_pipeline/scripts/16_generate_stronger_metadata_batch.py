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


BASE_HEADER = [
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
]
CONTROL_HEADER = [
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
HEADER = BASE_HEADER + [column for column in CONTROL_HEADER if column not in BASE_HEADER]

SUPPORTED_FAULT_TYPES = {
    "none",
    "pod-kill",
    "replica-drop",
    "cpu-stress",
    "memory-stress",
    "latency-injection",
    "timeout",
    "http-500",
}
VARIANT_PROFILES: dict[str, list[tuple[str, str]]] = {
    "compact": [("medium", "medium")],
    "strong": [("low", "low"), ("medium", "medium"), ("high", "high")],
    "xstrong": [
        ("low", "low"),
        ("medium", "medium"),
        ("high", "high"),
        ("medium", "high"),
        ("high", "medium"),
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a stronger RCA metadata batch with balanced splits and per-run runtime overrides."
    )
    parser.add_argument(
        "--scenario-catalog",
        type=Path,
        default=ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "scenario_catalog_online_boutique.csv",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "stronger_metadata_batch.csv",
    )
    parser.add_argument(
        "--variant-profile",
        choices=sorted(VARIANT_PROFILES),
        default="strong",
        help="Preset severity/load combinations to generate for fault scenarios.",
    )
    parser.add_argument("--train-runs", type=int, default=2)
    parser.add_argument("--val-runs", type=int, default=1)
    parser.add_argument("--test-runs", type=int, default=1)
    parser.add_argument(
        "--scenario-ids",
        default="",
        help="Optional comma-separated subset of scenario_id values to keep.",
    )
    parser.add_argument(
        "--include-unsupported",
        action="store_true",
        help="Include scenarios whose fault_type is not yet supported by 11_collect_run.py.",
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


def load_scenarios(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def parse_csv_list(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def split_sequence(train_runs: int, val_runs: int, test_runs: int) -> list[str]:
    sequence = (["train"] * max(train_runs, 0)) + (["val"] * max(val_runs, 0)) + (["test"] * max(test_runs, 0))
    if not sequence:
        raise ValueError("At least one split count must be positive.")
    return sequence


def build_variant_slug(severity: str, load_profile: str, is_normal: bool) -> str:
    if is_normal:
        return "baseline"
    return f"sev{severity}_load{load_profile}"


def base_runtime_controls(load_profile: str, severity: str, is_normal: bool) -> dict[str, str]:
    warmup_map = {"low": 45, "medium": 60, "high": 75}
    cooldown_map = {"low": 30, "medium": 45, "high": 60}
    query_base_map = {"low": 300, "medium": 500, "high": 750}
    fault_duration_map = {"low": 45, "medium": 75, "high": 105}
    severity_bonus_map = {"low": 0, "medium": 100, "high": 200}

    query_limit = query_base_map.get(load_profile, 500)
    if not is_normal:
        query_limit += severity_bonus_map.get(severity, 100)

    return {
        "warmup_seconds": str(warmup_map.get(load_profile, 60)),
        "cooldown_seconds": str(cooldown_map.get(load_profile, 45)),
        "fault_duration_seconds": "" if is_normal else str(fault_duration_map.get(severity, 75)),
        "query_limit": str(query_limit),
        "query_lookback": "custom-window",
    }


def fault_specific_controls(fault_type: str, severity: str) -> dict[str, str]:
    controls: dict[str, str] = {}

    if fault_type == "cpu-stress":
        request_map = {"low": "80", "medium": "40", "high": "20"}
        limit_map = {"low": "120", "medium": "70", "high": "40"}
        controls.update(
            {
                "cpu_request_m": request_map.get(severity, "40"),
                "cpu_limit_m": limit_map.get(severity, "70"),
            }
        )
    elif fault_type == "memory-stress":
        request_map = {"low": "96", "medium": "48", "high": "24"}
        limit_map = {"low": "128", "medium": "80", "high": "48"}
        controls.update(
            {
                "memory_request_mib": request_map.get(severity, "48"),
                "memory_limit_mib": limit_map.get(severity, "80"),
            }
        )
    elif fault_type == "latency-injection":
        delay_map = {"low": "1", "medium": "3", "high": "5"}
        controls["latency_delay_seconds"] = delay_map.get(severity, "3")
    elif fault_type == "replica-drop":
        replica_map = {"low": "1", "medium": "0", "high": "0"}
        controls["replica_drop_to"] = replica_map.get(severity, "0")
    elif fault_type == "pod-kill":
        repeats_map = {"low": "1", "medium": "2", "high": "3"}
        interval_map = {"low": "20", "medium": "15", "high": "10"}
        controls["pod_kill_repeats"] = repeats_map.get(severity, "1")
        controls["pod_kill_interval_seconds"] = interval_map.get(severity, "15")

    return controls


def recommended_variants_for_scenario(
    scenario: dict[str, str],
    variant_profile: str,
) -> list[tuple[str, str]]:
    fault_type = normalize_text(scenario.get("fault_type"), "none").lower()
    if fault_type == "none":
        severity = normalize_text(scenario.get("severity"), "medium").lower()
        load_profile = normalize_text(scenario.get("load_profile"), "medium").lower()
        return [(severity, load_profile)]
    return VARIANT_PROFILES[variant_profile]


def create_row(
    scenario: dict[str, str],
    scenario_run_index: int,
    split_tag: str,
    severity: str,
    load_profile: str,
    service_catalog_lookup: dict[str, dict[str, str]],
    args: argparse.Namespace,
) -> dict[str, object]:
    scenario_id = normalize_text(scenario.get("scenario_id"))
    label = int(float(normalize_text(scenario.get("label"), "0") or 0))
    fault_type = normalize_text(scenario.get("fault_type"), "none").lower()
    fault_family = normalize_text(scenario.get("fault_family"), "none").lower()
    source_service = normalize_text(scenario.get("source_service"), "frontend").lower()
    source_service_role = map_service_metadata(source_service, service_catalog_lookup)["service_role"]
    fault_target_service = normalize_text(scenario.get("fault_target_service"), "none").lower()
    fault_target_role = normalize_text(scenario.get("fault_target_role"), "none").lower()
    root_cause_service = fault_target_service if fault_target_service not in ("", "none") else "none"
    is_normal = fault_type == "none"
    variant_slug = build_variant_slug(severity, load_profile, is_normal=is_normal)
    run_id = f"{scenario_id}_{variant_slug}_{scenario_run_index:03d}"

    runtime_controls = base_runtime_controls(load_profile=load_profile, severity=severity, is_normal=is_normal)
    runtime_controls.update(fault_specific_controls(fault_type=fault_type, severity=severity))

    note_parts = [
        normalize_text(scenario.get("notes"), "auto-generated batch").strip(),
        "auto-generated strong batch",
        f"severity={severity}",
        f"load={load_profile}",
    ]
    if runtime_controls.get("fault_duration_seconds"):
        note_parts.append(f"fault_duration={runtime_controls['fault_duration_seconds']}s")
    if runtime_controls.get("query_limit"):
        note_parts.append(f"query_limit={runtime_controls['query_limit']}")

    row = {
        "run_id": run_id,
        "trace_file": f"{run_id}.json",
        "label": label,
        "sample_class": "fault" if label == 1 else "normal",
        "phase_policy": "fault-phases",
        "fault_family": fault_family,
        "fault_type": fault_type,
        "root_cause_service": root_cause_service,
        "fault_target_service": fault_target_service,
        "fault_target_role": fault_target_role,
        "source_service": source_service,
        "source_service_role": source_service_role,
        "system_id": args.system_id,
        "system_family": args.system_family,
        "topology_version": args.topology_version,
        "experiment_group": fault_family,
        "chaos_name": normalize_text(scenario.get("chaos_name"), "none").lower(),
        "chaos_kind": normalize_text(scenario.get("chaos_kind"), "none").lower(),
        "target_service": fault_target_service,
        "target_pod": "",
        "target_container": fault_target_service if fault_target_service not in ("", "none") else "",
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
        "notes": "; ".join(part for part in note_parts if part),
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
    return row


def main() -> None:
    args = parse_args()
    scenarios = load_scenarios(args.scenario_catalog)
    selected_scenarios = parse_csv_list(args.scenario_ids)
    split_tags = split_sequence(args.train_runs, args.val_runs, args.test_runs)
    catalog_lookup = service_lookup(load_service_catalog(args.service_catalog))

    filtered_scenarios: list[dict[str, str]] = []
    skipped_unsupported = 0
    for scenario in scenarios:
        scenario_id = normalize_text(scenario.get("scenario_id"))
        fault_type = normalize_text(scenario.get("fault_type"), "none").lower()
        if selected_scenarios and scenario_id not in selected_scenarios:
            continue
        if not args.include_unsupported and fault_type not in SUPPORTED_FAULT_TYPES:
            skipped_unsupported += 1
            continue
        filtered_scenarios.append(scenario)

    if not filtered_scenarios:
        raise ValueError("No scenarios left after filtering.")

    rows: list[dict[str, object]] = []
    for scenario in filtered_scenarios:
        run_index = 1
        for severity, load_profile in recommended_variants_for_scenario(
            scenario=scenario,
            variant_profile=args.variant_profile,
        ):
            for split_tag in split_tags:
                rows.append(
                    create_row(
                        scenario=scenario,
                        scenario_run_index=run_index,
                        split_tag=split_tag,
                        severity=severity,
                        load_profile=load_profile,
                        service_catalog_lookup=catalog_lookup,
                        args=args,
                    )
                )
                run_index += 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Generated {len(rows)} metadata rows")
    print(f"Scenarios kept: {len(filtered_scenarios)}")
    print(f"Skipped unsupported: {skipped_unsupported}")
    print(f"Variant profile: {args.variant_profile}")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
