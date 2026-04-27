from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.rca_data_pipeline.config import DEFAULT_SERVICE_CATALOG, DEFAULT_SYSTEM_FAMILY, DEFAULT_SYSTEM_ID, DEFAULT_TOPOLOGY_VERSION
from pipeline.rca_data_pipeline.service_catalog import load_service_catalog, map_service_metadata, service_lookup


HEADER = [
    "run_id",
    "trace_file",
    "label",
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate skeleton metadata rows from the scenario catalog.")
    parser.add_argument(
        "--scenario-catalog",
        type=Path,
        default=ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "scenario_catalog_online_boutique.csv",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "generated_metadata_batch.csv",
    )
    parser.add_argument("--runs-per-scenario", type=int, default=3)
    parser.add_argument("--start-index", type=int, default=1)
    parser.add_argument("--split-tags", type=str, default="train,val,test")
    parser.add_argument("--system-id", type=str, default=DEFAULT_SYSTEM_ID)
    parser.add_argument("--system-family", type=str, default=DEFAULT_SYSTEM_FAMILY)
    parser.add_argument("--topology-version", type=str, default=DEFAULT_TOPOLOGY_VERSION)
    parser.add_argument("--service-catalog", type=Path, default=DEFAULT_SERVICE_CATALOG)
    return parser.parse_args()


def load_scenarios(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def main() -> None:
    args = parse_args()
    scenarios = load_scenarios(args.scenario_catalog)
    split_tags = [item.strip() for item in args.split_tags.split(",") if item.strip()]
    if not split_tags:
        split_tags = ["train"]

    catalog_lookup = service_lookup(load_service_catalog(args.service_catalog))
    rows: list[dict[str, object]] = []

    for scenario in scenarios:
        scenario_id = str(scenario["scenario_id"]).strip()
        source_service = str(scenario.get("source_service", "frontend")).strip().lower()
        source_service_role = map_service_metadata(source_service, catalog_lookup)["service_role"]
        fault_target_service = str(scenario.get("fault_target_service", "none")).strip().lower()
        fault_target_role = str(scenario.get("fault_target_role", "none")).strip().lower()

        for offset in range(args.runs_per_scenario):
            run_no = args.start_index + offset
            run_id = f"{scenario_id}_{run_no:03d}"
            split_tag = split_tags[offset % len(split_tags)]
            row = {
                "run_id": run_id,
                "trace_file": f"{run_id}.json",
                "label": int(str(scenario.get("label", "0")).strip() or 0),
                "fault_family": str(scenario.get("fault_family", "none")).strip().lower(),
                "fault_type": str(scenario.get("fault_type", "none")).strip().lower(),
                "root_cause_service": fault_target_service,
                "fault_target_service": fault_target_service,
                "fault_target_role": fault_target_role,
                "source_service": source_service,
                "source_service_role": source_service_role,
                "system_id": args.system_id,
                "system_family": args.system_family,
                "topology_version": args.topology_version,
                "experiment_group": str(scenario.get("fault_family", "none")).strip().lower(),
                "chaos_name": str(scenario.get("chaos_name", "none")).strip().lower(),
                "chaos_kind": str(scenario.get("chaos_kind", "none")).strip().lower(),
                "target_service": fault_target_service,
                "target_pod": "",
                "target_container": "",
                "severity": str(scenario.get("severity", "medium")).strip().lower(),
                "load_profile": str(scenario.get("load_profile", "medium")).strip().lower(),
                "split_tag": split_tag,
                "start_time": "",
                "fault_start_time": "",
                "fault_end_time": "",
                "end_time": "",
                "export_duration_ms": "",
                "query_limit": "",
                "query_lookback": "1h",
                "trace_count": "",
                "span_count_total": "",
                "avg_spans_per_trace": "",
                "unique_service_count": "",
                "unique_services": "",
                "root_cause_trace_hits": "",
                "health_trace_count": "",
                "otel_export_trace_count": "",
                "business_trace_count": "",
                "notes": str(scenario.get("notes", "")).strip(),
            }
            rows.append(row)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Generated {len(rows)} metadata rows")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
