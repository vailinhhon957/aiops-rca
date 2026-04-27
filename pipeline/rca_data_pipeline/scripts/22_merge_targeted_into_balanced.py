from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_BALANCED_DIR = ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "balanced_manifests"
DEFAULT_ANOMALY_METADATA = DEFAULT_BALANCED_DIR / "anomaly_balanced_metadata.csv"
DEFAULT_RCA_METADATA = DEFAULT_BALANCED_DIR / "rca_balanced_metadata.csv"
DEFAULT_TARGETED_METADATA = ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "targeted_augmentation_batch.csv"
DEFAULT_OUTPUT_DIR = DEFAULT_BALANCED_DIR / "v2"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge targeted augmentation runs into the current balanced anomaly/RCA manifests."
    )
    parser.add_argument("--anomaly-metadata", type=Path, default=DEFAULT_ANOMALY_METADATA)
    parser.add_argument("--rca-metadata", type=Path, default=DEFAULT_RCA_METADATA)
    parser.add_argument("--targeted-metadata", type=Path, default=DEFAULT_TARGETED_METADATA)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def load_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return list(reader.fieldnames or []), rows


def merged_header(*headers: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for header in headers:
        for column in header:
            if column not in seen:
                result.append(column)
                seen.add(column)
    return result


def align_rows(rows: list[dict[str, str]], header: list[str]) -> list[dict[str, str]]:
    return [{column: row.get(column, "") for column in header} for row in rows]


def save_rows(path: Path, header: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(align_rows(rows, header))


def norm(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def label_of(row: dict[str, str]) -> int:
    raw = norm(row.get("label"), "0")
    try:
        return int(float(raw))
    except ValueError:
        return 0


def sample_class_of(row: dict[str, str]) -> str:
    return norm(row.get("sample_class")).lower()


def root_cause_of(row: dict[str, str]) -> str:
    return norm(row.get("root_cause_service"), "none").lower()


def merge_unique(base_rows: list[dict[str, str]], extra_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: dict[str, dict[str, str]] = {norm(row.get("run_id")): row for row in base_rows}
    for row in extra_rows:
        merged[norm(row.get("run_id"))] = row
    result = list(merged.values())
    result.sort(key=lambda row: norm(row.get("run_id")))
    return result


def main() -> None:
    args = parse_args()

    anomaly_header, anomaly_rows = load_rows(args.anomaly_metadata)
    rca_header, rca_rows = load_rows(args.rca_metadata)
    targeted_header, targeted_rows = load_rows(args.targeted_metadata)

    combined_header = merged_header(anomaly_header, rca_header, targeted_header)

    targeted_positive = [row for row in targeted_rows if label_of(row) == 1]
    targeted_negative = [row for row in targeted_rows if label_of(row) == 0]

    anomaly_v2_rows = merge_unique(anomaly_rows, targeted_rows)
    rca_v2_rows = merge_unique(rca_rows, targeted_positive)

    output_dir = args.output_dir
    anomaly_v2_path = output_dir / "anomaly_balanced_v2_metadata.csv"
    rca_v2_path = output_dir / "rca_balanced_v2_metadata.csv"
    summary_path = output_dir / "merge_summary.txt"

    save_rows(anomaly_v2_path, combined_header, anomaly_v2_rows)
    save_rows(rca_v2_path, combined_header, rca_v2_rows)

    anomaly_counts = Counter("positive" if label_of(row) == 1 else sample_class_of(row) or "negative" for row in anomaly_v2_rows)
    rca_root_counts = Counter(root_cause_of(row) for row in rca_v2_rows if root_cause_of(row) not in {"", "none"})

    summary_lines = [
        f"targeted_rows={len(targeted_rows)}",
        f"targeted_positive={len(targeted_positive)}",
        f"targeted_negative={len(targeted_negative)}",
        f"anomaly_base_rows={len(anomaly_rows)}",
        f"anomaly_v2_rows={len(anomaly_v2_rows)}",
        f"anomaly_v2_counts={dict(anomaly_counts)}",
        f"rca_base_rows={len(rca_rows)}",
        f"rca_v2_rows={len(rca_v2_rows)}",
        f"rca_v2_root_counts={dict(rca_root_counts)}",
        f"anomaly_v2_manifest={anomaly_v2_path}",
        f"rca_v2_manifest={rca_v2_path}",
    ]
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    print(f"Anomaly v2 manifest: {anomaly_v2_path}")
    print(f"RCA v2 manifest: {rca_v2_path}")
    print(f"Summary: {summary_path}")
    print(f"anomaly_v2_rows={len(anomaly_v2_rows)}")
    print(f"rca_v2_rows={len(rca_v2_rows)}")


if __name__ == "__main__":
    main()
