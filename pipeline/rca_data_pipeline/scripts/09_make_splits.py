from __future__ import annotations

import argparse
import ast
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.rca_data_pipeline.config import PROCESSED_ROOT, RANDOM_SEED, SPLITS_ROOT
from pipeline.rca_data_pipeline.io_utils import ensure_dir, latest_table, read_table


VALID_SPLITS = ("train", "val", "test")


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def parse_legacy_metadata(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    text = normalize_text(value)
    if not text:
        return {}
    try:
        parsed = ast.literal_eval(text)
    except (ValueError, SyntaxError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def extract_split_tag(row: dict[str, object]) -> str:
    direct_split = normalize_text(row.get("split_tag")).lower()
    if direct_split in VALID_SPLITS:
        return direct_split

    legacy_split = normalize_text(parse_legacy_metadata(row.get("legacy_metadata")).get("split_tag")).lower()
    if legacy_split in VALID_SPLITS:
        return legacy_split

    return ""


def main() -> None:
    parser = argparse.ArgumentParser(description="Split runs into train/val/test using grouped run IDs.")
    parser.add_argument("--run-catalog", type=Path, default=latest_table(PROCESSED_ROOT, "run_catalog"))
    parser.add_argument("--train-ratio", type=float, default=0.7)
    parser.add_argument("--val-ratio", type=float, default=0.15)
    args = parser.parse_args()

    if args.run_catalog is None:
        raise FileNotFoundError("Missing run catalog.")

    run_catalog_df = read_table(args.run_catalog)
    grouped: dict[str, list[str]] = {}
    metadata_split_runs: dict[str, list[str]] = {split: [] for split in VALID_SPLITS}
    fallback_rows: list[dict[str, object]] = []

    for row in run_catalog_df.to_dict(orient="records"):
        split_tag = extract_split_tag(row)
        if split_tag:
            metadata_split_runs[split_tag].append(str(row["run_id"]))
            continue
        fallback_rows.append(row)

    for row in fallback_rows:
        key = f"{row.get('fault_type', 'none')}::{row.get('root_cause_service', 'none')}"
        grouped.setdefault(key, []).append(str(row["run_id"]))

    rng = random.Random(RANDOM_SEED)
    train_runs = list(metadata_split_runs["train"])
    val_runs = list(metadata_split_runs["val"])
    test_runs = list(metadata_split_runs["test"])

    for run_ids in grouped.values():
        unique_ids = sorted(set(run_ids))
        rng.shuffle(unique_ids)
        n = len(unique_ids)
        n_train = max(1, int(round(n * args.train_ratio))) if n >= 3 else max(1, n - 1)
        n_val = int(round(n * args.val_ratio)) if n >= 6 else 0
        n_train = min(n_train, n)
        n_val = min(n_val, max(0, n - n_train))
        n_test = max(0, n - n_train - n_val)

        train_runs.extend(unique_ids[:n_train])
        val_runs.extend(unique_ids[n_train : n_train + n_val])
        test_runs.extend(unique_ids[n_train + n_val : n_train + n_val + n_test])

    ensure_dir(SPLITS_ROOT)
    (SPLITS_ROOT / "train_runs.txt").write_text("\n".join(sorted(set(train_runs))), encoding="utf-8")
    (SPLITS_ROOT / "val_runs.txt").write_text("\n".join(sorted(set(val_runs))), encoding="utf-8")
    (SPLITS_ROOT / "test_runs.txt").write_text("\n".join(sorted(set(test_runs))), encoding="utf-8")
    print(f"train={len(train_runs)} val={len(val_runs)} test={len(test_runs)}")


if __name__ == "__main__":
    main()
