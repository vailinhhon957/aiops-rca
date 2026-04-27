from __future__ import annotations

import argparse
import csv
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_METADATA_FILE = ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "batch1_fill_only.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Autofill safe metadata fields such as target_container and optional target_pod."
    )
    parser.add_argument("--metadata-file", type=Path, default=DEFAULT_METADATA_FILE)
    parser.add_argument("--namespace", default="default")
    parser.add_argument(
        "--fill-target-pods",
        action="store_true",
        help="Try to fetch the current pod name from Kubernetes for pod-kill rows whose target_pod is blank.",
    )
    parser.add_argument(
        "--overwrite-target-pod",
        action="store_true",
        help="Refresh target_pod even when a value already exists.",
    )
    parser.add_argument("--dry-run", action="store_true")
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


def normalized(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def get_first_pod_name(namespace: str, app_label: str) -> str:
    result = subprocess.run(
        [
            "kubectl",
            "-n",
            namespace,
            "get",
            "pod",
            "-l",
            f"app={app_label}",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
        check=True,
        text=True,
        capture_output=True,
    )
    return result.stdout.strip()


def autofill_row(
    row: dict[str, str],
    namespace: str,
    fill_target_pods: bool,
    overwrite_target_pod: bool,
) -> tuple[dict[str, str], list[str]]:
    changed_fields: list[str] = []
    fault_type = normalized(row.get("fault_type"), "none").lower()
    target_service = normalized(row.get("fault_target_service"), "none").lower()
    target_container = normalized(row.get("target_container"))
    target_pod = normalized(row.get("target_pod"))

    if target_service not in ("", "none") and not target_container:
        row["target_container"] = target_service
        changed_fields.append("target_container")

    should_fill_pod = fault_type == "pod-kill" and target_service not in ("", "none")
    if should_fill_pod and fill_target_pods and (overwrite_target_pod or not target_pod):
        try:
            live_pod = get_first_pod_name(namespace, target_service)
        except subprocess.CalledProcessError:
            live_pod = ""
        if live_pod and live_pod != target_pod:
            row["target_pod"] = live_pod
            changed_fields.append("target_pod")

    return row, changed_fields


def main() -> None:
    args = parse_args()
    header, rows = load_rows(args.metadata_file)
    changed_rows: list[tuple[str, list[str]]] = []

    for idx, row in enumerate(rows):
        updated_row, changed_fields = autofill_row(
            row=row,
            namespace=args.namespace,
            fill_target_pods=args.fill_target_pods,
            overwrite_target_pod=args.overwrite_target_pod,
        )
        rows[idx] = updated_row
        if changed_fields:
            changed_rows.append((normalized(row.get("run_id")), changed_fields))

    if not args.dry_run:
        save_rows(args.metadata_file, header, rows)

    print(f"metadata_file={args.metadata_file}")
    print(f"changed_rows={len(changed_rows)}")
    for run_id, fields in changed_rows:
        print(f"{run_id}: {', '.join(fields)}")


if __name__ == "__main__":
    main()
