from __future__ import annotations

import argparse
import csv
import random
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_POSITIVE_METADATA = ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "stronger_metadata_batch.csv"
DEFAULT_NEGATIVE_METADATA = ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "extra_normal_hard_negative_batch.csv"
DEFAULT_OUTPUT_DIR = ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "balanced_manifests"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build balanced metadata manifests for anomaly and RCA training."
    )
    parser.add_argument("--positive-metadata", type=Path, default=DEFAULT_POSITIVE_METADATA)
    parser.add_argument("--negative-metadata", type=Path, default=DEFAULT_NEGATIVE_METADATA)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def load_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return list(reader.fieldnames or []), rows


def save_rows(path: Path, header: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def merged_header(*headers: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for header in headers:
        for column in header:
            if column not in seen:
                merged.append(column)
                seen.add(column)
    return merged


def align_rows(rows: list[dict[str, str]], header: list[str]) -> list[dict[str, str]]:
    return [{column: row.get(column, "") for column in header} for row in rows]


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


def fault_type_of(row: dict[str, str]) -> str:
    return norm(row.get("fault_type"), "none").lower()


def root_cause_of(row: dict[str, str]) -> str:
    return norm(row.get("root_cause_service"), "none").lower()


def deterministic_shuffle(rows: list[dict[str, str]], seed: int) -> list[dict[str, str]]:
    keyed = list(rows)
    rnd = random.Random(seed)
    rnd.shuffle(keyed)
    return keyed


def allocate_balanced(groups: dict[str, list[dict[str, str]]], total_target: int, seed: int) -> list[dict[str, str]]:
    if total_target <= 0 or not groups:
        return []

    keys = sorted(groups)
    target_per_group = total_target // len(keys)
    remainder = total_target % len(keys)

    selected: list[dict[str, str]] = []
    leftovers: list[dict[str, str]] = []

    for idx, key in enumerate(keys):
        wanted = target_per_group + (1 if idx < remainder else 0)
        shuffled = deterministic_shuffle(groups[key], seed + idx)
        selected.extend(shuffled[:wanted])
        leftovers.extend(shuffled[wanted:])

    if len(selected) < total_target and leftovers:
        leftovers = deterministic_shuffle(leftovers, seed + 999)
        selected.extend(leftovers[: total_target - len(selected)])

    return selected[:total_target]


def main() -> None:
    args = parse_args()
    positive_header, positive_rows_all = load_rows(args.positive_metadata)
    negative_header, negative_rows_all = load_rows(args.negative_metadata)
    header = merged_header(positive_header, negative_header)

    positive_rows = [row for row in positive_rows_all if label_of(row) == 1]
    negative_rows = [row for row in negative_rows_all if label_of(row) == 0]
    normal_rows = [row for row in negative_rows if sample_class_of(row) == "normal" or fault_type_of(row) == "none"]
    hard_negative_rows = [row for row in negative_rows if row not in normal_rows]

    positive_by_fault: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in positive_rows:
        positive_by_fault[fault_type_of(row)].append(row)

    target_positive_for_anomaly = len(negative_rows)
    selected_positive_for_anomaly = allocate_balanced(
        positive_by_fault,
        total_target=target_positive_for_anomaly,
        seed=args.seed,
    )
    anomaly_rows = negative_rows + selected_positive_for_anomaly
    anomaly_rows.sort(key=lambda row: (label_of(row), sample_class_of(row), fault_type_of(row), norm(row.get("run_id"))))

    positive_by_root: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in positive_rows:
        root = root_cause_of(row)
        if root not in {"", "none"}:
            positive_by_root[root].append(row)

    min_per_root = min((len(rows) for rows in positive_by_root.values()), default=0)
    rca_rows: list[dict[str, str]] = []
    for idx, root in enumerate(sorted(positive_by_root)):
        shuffled = deterministic_shuffle(positive_by_root[root], args.seed + idx)
        rca_rows.extend(shuffled[:min_per_root])
    rca_rows.sort(key=lambda row: (root_cause_of(row), norm(row.get("run_id"))))

    args.output_dir.mkdir(parents=True, exist_ok=True)
    anomaly_path = args.output_dir / "anomaly_balanced_metadata.csv"
    rca_path = args.output_dir / "rca_balanced_metadata.csv"
    summary_path = args.output_dir / "balanced_summary.txt"

    save_rows(anomaly_path, header, align_rows(anomaly_rows, header))
    save_rows(rca_path, header, align_rows(rca_rows, header))

    anomaly_counts = Counter("positive" if label_of(row) == 1 else sample_class_of(row) or "negative" for row in anomaly_rows)
    rca_counts = Counter(root_cause_of(row) for row in rca_rows)

    summary_lines = [
        f"positive_source_rows={len(positive_rows)}",
        f"negative_source_rows={len(negative_rows)}",
        f"normal_negative_rows={len(normal_rows)}",
        f"hard_negative_rows={len(hard_negative_rows)}",
        f"anomaly_balanced_rows={len(anomaly_rows)}",
        f"anomaly_counts={dict(anomaly_counts)}",
        f"rca_balanced_rows={len(rca_rows)}",
        f"rca_min_per_root={min_per_root}",
        f"rca_counts={dict(rca_counts)}",
        f"anomaly_manifest={anomaly_path}",
        f"rca_manifest={rca_path}",
    ]
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    print(f"Anomaly manifest: {anomaly_path}")
    print(f"RCA manifest: {rca_path}")
    print(f"Summary: {summary_path}")
    print(f"Anomaly rows={len(anomaly_rows)}")
    print(f"RCA rows={len(rca_rows)}")


if __name__ == "__main__":
    main()
