from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_V2_DIR = ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "balanced_manifests" / "v2"
DEFAULT_OUTPUT_DIR = ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "balanced_manifests" / "v3"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge hard-normal, light-anomaly, and weak-service RCA batches into v2 manifests.")
    parser.add_argument("--anomaly-v2", type=Path, default=DEFAULT_V2_DIR / "anomaly_balanced_v2_metadata.csv")
    parser.add_argument("--rca-v2", type=Path, default=DEFAULT_V2_DIR / "rca_balanced_v2_metadata.csv")
    parser.add_argument("--hard-normal", type=Path, default=ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "hard_normal_batch.csv")
    parser.add_argument("--light-anomaly", type=Path, default=ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "light_anomaly_batch.csv")
    parser.add_argument("--weak-rca", type=Path, default=ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "weak_service_rca_batch.csv")
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
        for col in header:
            if col not in seen:
                result.append(col)
                seen.add(col)
    return result


def align_rows(rows: list[dict[str, str]], header: list[str]) -> list[dict[str, str]]:
    return [{col: row.get(col, "") for col in header} for row in rows]


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
    try:
        return int(float(norm(row.get("label"), "0")))
    except ValueError:
        return 0


def root_cause_of(row: dict[str, str]) -> str:
    return norm(row.get("root_cause_service"), "none").lower()


def sample_class_of(row: dict[str, str]) -> str:
    return norm(row.get("sample_class")).lower()


def merge_unique(*row_groups: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: dict[str, dict[str, str]] = {}
    for rows in row_groups:
        for row in rows:
            merged[norm(row.get("run_id"))] = row
    result = list(merged.values())
    result.sort(key=lambda row: norm(row.get("run_id")))
    return result


def main() -> None:
    args = parse_args()
    anomaly_header, anomaly_v2_rows = load_rows(args.anomaly_v2)
    rca_header, rca_v2_rows = load_rows(args.rca_v2)
    hard_header, hard_rows = load_rows(args.hard_normal)
    light_header, light_rows = load_rows(args.light_anomaly)
    weak_header, weak_rows = load_rows(args.weak_rca)

    header = merged_header(anomaly_header, rca_header, hard_header, light_header, weak_header)

    anomaly_v3_rows = merge_unique(anomaly_v2_rows, hard_rows, light_rows, weak_rows)
    rca_v3_rows = merge_unique(rca_v2_rows, light_rows, weak_rows)

    output_dir = args.output_dir
    anomaly_v3_path = output_dir / "anomaly_balanced_v3_metadata.csv"
    rca_v3_path = output_dir / "rca_balanced_v3_metadata.csv"
    summary_path = output_dir / "merge_summary.txt"

    save_rows(anomaly_v3_path, header, anomaly_v3_rows)
    save_rows(rca_v3_path, header, rca_v3_rows)

    anomaly_counts = Counter("positive" if label_of(r) == 1 else sample_class_of(r) for r in anomaly_v3_rows)
    rca_root_counts = Counter(root_cause_of(r) for r in rca_v3_rows if root_cause_of(r) not in {"", "none"})

    summary_lines = [
        f"hard_normal_rows={len(hard_rows)}",
        f"light_anomaly_rows={len(light_rows)}",
        f"weak_service_rca_rows={len(weak_rows)}",
        f"anomaly_v2_rows={len(anomaly_v2_rows)}",
        f"anomaly_v3_rows={len(anomaly_v3_rows)}",
        f"anomaly_v3_counts={dict(anomaly_counts)}",
        f"rca_v2_rows={len(rca_v2_rows)}",
        f"rca_v3_rows={len(rca_v3_rows)}",
        f"rca_v3_root_counts={dict(rca_root_counts)}",
    ]
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    print(f"Anomaly v3 manifest: {anomaly_v3_path}")
    print(f"RCA v3 manifest: {rca_v3_path}")
    print(f"Summary: {summary_path}")


if __name__ == "__main__":
    main()
