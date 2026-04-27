from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.rca_data_pipeline.config import (
    DEFAULT_SERVICE_CATALOG,
    DEFAULT_SYSTEM_FAMILY,
    DEFAULT_SYSTEM_ID,
    DEFAULT_TOPOLOGY_VERSION,
    LEGACY_DATASET_ROOT,
    PROCESSED_ROOT,
    RAW_ROOT,
)
from pipeline.rca_data_pipeline.io_utils import append_jsonl, copy_file, ensure_dir, save_json, write_table
from pipeline.rca_data_pipeline.service_catalog import load_service_catalog, map_service_metadata, service_lookup


def normalized_text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip().strip('"')
    return text or default


def infer_label(row: dict[str, object]) -> int:
    raw_label = normalized_text(row.get("label"), "")
    if raw_label:
        try:
            return int(float(raw_label))
        except ValueError:
            pass
    fault_type = normalized_text(row.get("fault_type"), "none").lower()
    return 0 if fault_type in ("", "none") else 1


def infer_fault_target_service(row: dict[str, object]) -> str:
    return normalized_text(
        row.get("root_cause_service") or row.get("fault_target_service"),
        "none",
    ).lower()


def infer_source_service(row: dict[str, object]) -> str:
    return normalized_text(row.get("source_service"), "frontend").lower()


def main() -> None:
    parser = argparse.ArgumentParser(description="Import the existing trace dataset into the run-based layout.")
    parser.add_argument("--metadata", type=Path, default=LEGACY_DATASET_ROOT / "metadata" / "metadata_rich.csv")
    parser.add_argument("--raw-root", type=Path, default=LEGACY_DATASET_ROOT / "raw")
    parser.add_argument("--service-catalog", type=Path, default=DEFAULT_SERVICE_CATALOG)
    parser.add_argument("--system-id", type=str, default=DEFAULT_SYSTEM_ID)
    parser.add_argument("--system-family", type=str, default=DEFAULT_SYSTEM_FAMILY)
    parser.add_argument("--topology-version", type=str, default=DEFAULT_TOPOLOGY_VERSION)
    args = parser.parse_args()

    metadata_df = pd.read_csv(args.metadata)
    catalog_df = load_service_catalog(args.service_catalog)
    catalog_lookup = service_lookup(catalog_df)
    catalog_rows = []
    imported = 0

    for row in metadata_df.to_dict(orient="records"):
        run_id = normalized_text(row["run_id"])
        trace_file = normalized_text(row["trace_file"])
        run_root = ensure_dir(RAW_ROOT / run_id)
        windows_root = ensure_dir(run_root / "windows")

        candidates = list(args.raw_root.rglob(trace_file))
        if not candidates:
            print(f"skip missing trace file: {trace_file}")
            continue

        copy_file(candidates[0], windows_root / "traces_0001.json")
        copy_file(args.service_catalog, run_root / "service_catalog.json")

        label = infer_label(row)
        fault_target_service = infer_fault_target_service(row)
        fault_target_role = map_service_metadata(fault_target_service, catalog_lookup)["service_role"]
        source_service = infer_source_service(row)
        source_service_role = map_service_metadata(source_service, catalog_lookup)["service_role"]
        fault_family = normalized_text(row.get("fault_type"), "none").lower()
        run_meta = {
            "run_id": run_id,
            "system_id": args.system_id,
            "system_family": args.system_family,
            "topology_version": args.topology_version,
            "scenario_name": row.get("fault_type", "none"),
            "trace_file": trace_file,
            "label": label,
            "fault_type": fault_family,
            "fault_family": fault_family,
            "root_cause_service": fault_target_service,
            "fault_target_service": fault_target_service,
            "fault_target_role": fault_target_role,
            "source_service": source_service,
            "source_service_role": source_service_role,
            "sample_class": normalized_text(row.get("sample_class"), "fault" if label == 1 else "normal").lower(),
            "phase_policy": normalized_text(row.get("phase_policy"), "fault-phases").lower(),
            "split_tag": normalized_text(row.get("split_tag")).lower(),
            "start_time": row.get("start_time", ""),
            "fault_start_time": row.get("fault_start_time", ""),
            "fault_end_time": row.get("fault_end_time", ""),
            "end_time": row.get("end_time", ""),
            "notes": row.get("notes", ""),
            "legacy_metadata": row,
        }
        save_json(run_root / "run_meta.json", run_meta)

        events = [{"ts": run_meta["start_time"], "event": "run_started", "run_id": run_id}]
        if int(run_meta["label"]) == 1 and run_meta["fault_type"] not in ("none", "", None):
            events.append(
                {
                    "ts": run_meta["start_time"],
                    "event": "fault_started",
                    "fault_type": run_meta["fault_type"],
                    "fault_service": run_meta["fault_target_service"],
                    "fault_role": run_meta["fault_target_role"],
                }
            )
            events.append(
                {
                    "ts": run_meta["end_time"],
                    "event": "fault_ended",
                    "fault_type": run_meta["fault_type"],
                    "fault_service": run_meta["fault_target_service"],
                    "fault_role": run_meta["fault_target_role"],
                }
            )
        events.append({"ts": run_meta["end_time"], "event": "run_finished", "run_id": run_id})
        append_jsonl(run_root / "events.jsonl", events)

        catalog_rows.append(run_meta | {"raw_trace_path": str(windows_root / "traces_0001.json")})
        imported += 1

    catalog_df = pd.DataFrame(catalog_rows)
    out_path = write_table(catalog_df, PROCESSED_ROOT / "run_catalog")
    print(f"Imported {imported} runs")
    print(f"Run catalog: {out_path}")


if __name__ == "__main__":
    main()
