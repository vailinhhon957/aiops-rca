from __future__ import annotations

import argparse
import csv
import importlib.util
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_METADATA_FILE = ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "batch1_fill_only.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch autofill metadata rows in the same spirit as 13_run_batch_dataset.py."
    )
    parser.add_argument("--metadata-file", type=Path, default=DEFAULT_METADATA_FILE)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--run-ids", default="")
    parser.add_argument("--split-tags", default="")
    parser.add_argument("--fill-target-pods", action="store_true")
    parser.add_argument("--overwrite-target-pod", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--show-missing",
        action="store_true",
        help="Print remaining missing fields after autofill.",
    )
    return parser.parse_args()


def load_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


def save_rows(path: Path, header: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def normalize_csv_list(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def select_rows(rows: list[dict[str, str]], run_ids: set[str], split_tags: set[str]) -> list[dict[str, str]]:
    selected = []
    for row in rows:
        run_id = (row.get("run_id") or "").strip()
        split_tag = (row.get("split_tag") or "").strip()
        if run_ids and run_id not in run_ids:
            continue
        if split_tags and split_tag not in split_tags:
            continue
        selected.append(row)
    return selected


def get_missing_fields(row: dict[str, str]) -> list[str]:
    fault_type = (row.get("fault_type") or "none").strip().lower()
    if fault_type == "none":
        required = ["start_time", "end_time"]
    elif fault_type in {"pod-kill", "replica-drop"}:
        required = ["target_container", "start_time", "fault_start_time", "fault_end_time", "end_time"]
        if fault_type == "pod-kill":
            required.insert(0, "target_pod")
    else:
        required = ["target_container", "start_time", "fault_start_time", "fault_end_time", "end_time"]
    return [field for field in required if not str(row.get(field, "")).strip()]


def main() -> None:
    args = parse_args()
    header, rows = load_rows(args.metadata_file)
    run_ids = normalize_csv_list(args.run_ids)
    split_tags = normalize_csv_list(args.split_tags)

    selected_run_ids = {row["run_id"] for row in select_rows(rows, run_ids, split_tags)}
    if not selected_run_ids:
        raise ValueError("No rows selected from metadata file.")

    autofill_path = SCRIPT_DIR / "14_autofill_metadata_fields.py"
    spec = importlib.util.spec_from_file_location("autofill_metadata_fields", autofill_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load autofill module from {autofill_path}")
    autofill_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(autofill_module)

    changed_rows: list[tuple[str, list[str]]] = []
    updated_rows: list[dict[str, str]] = []
    for row in rows:
        run_id = (row.get("run_id") or "").strip()
        if run_id not in selected_run_ids:
            updated_rows.append(row)
            continue
        updated_row, changed_fields = autofill_module.autofill_row(
            row=row,
            namespace=args.namespace,
            fill_target_pods=args.fill_target_pods,
            overwrite_target_pod=args.overwrite_target_pod,
        )
        updated_rows.append(updated_row)
        if changed_fields:
            changed_rows.append((run_id, changed_fields))

    if not args.dry_run:
        save_rows(args.metadata_file, header, updated_rows)

    print("Metadata autofill batch finished.")
    print(f"metadata_file={args.metadata_file}")
    print(f"selected_runs={len(selected_run_ids)}")
    print(f"changed_rows={len(changed_rows)}")
    for run_id, fields in changed_rows:
        print(f"{run_id}: {', '.join(fields)}")

    if args.show_missing:
        print("remaining_missing:")
        for row in updated_rows:
            run_id = (row.get("run_id") or "").strip()
            if run_id not in selected_run_ids:
                continue
            missing = get_missing_fields(row)
            print(f"{run_id}: {', '.join(missing) if missing else 'none'}")


if __name__ == "__main__":
    main()
