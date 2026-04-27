from __future__ import annotations

import argparse
import csv
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "extra_normal_hard_negative_batch.csv"
DEFAULT_NORMAL_PROFILES = "idle,very_low,low_medium,medium_high,very_high,burst"
DEFAULT_HARD_NEGATIVE_SCENARIOS = "ob_cpu_reco,ob_mem_cart,ob_lat_pay,ob_lat_catalog,ob_scale_catalog"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare a merged metadata batch that adds extra normal runs and hard-negative runs."
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--normal-profiles", default=DEFAULT_NORMAL_PROFILES)
    parser.add_argument(
        "--normal-runs-per-profile",
        type=int,
        default=3,
        help="How many extra normal runs to generate for each selected load profile.",
    )
    parser.add_argument("--train-runs", type=int, default=2)
    parser.add_argument("--val-runs", type=int, default=1)
    parser.add_argument("--test-runs", type=int, default=1)
    parser.add_argument(
        "--hard-negative-profile",
        choices=["compact", "balanced"],
        default="balanced",
        help="Preset severity/load combinations for hard negatives.",
    )
    parser.add_argument("--hard-negative-scenarios", default=DEFAULT_HARD_NEGATIVE_SCENARIOS)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--jaeger-url", default="http://127.0.0.1:16686")
    parser.add_argument(
        "--mode",
        choices=["generate-only", "collect-export", "all", "rebuild-only"],
        default="generate-only",
        help="generate-only: just write metadata. collect-export/all/rebuild-only delegate to 13_run_batch_dataset.py.",
    )
    parser.add_argument("--clean", action="store_true")
    return parser.parse_args()


def run_python(script_name: str, args: list[str]) -> None:
    script_path = SCRIPT_DIR / script_name
    subprocess.run([sys.executable, str(script_path), *args], check=True)


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


def merge_rows(normal_rows: list[dict[str, str]], hard_negative_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    merged = normal_rows + hard_negative_rows
    merged.sort(key=lambda row: ((row.get("sample_class") or ""), (row.get("load_profile") or ""), (row.get("run_id") or "")))
    return merged


def main() -> None:
    args = parse_args()

    with tempfile.TemporaryDirectory(prefix="rca-extra-batch-") as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        normal_tmp = tmp_dir / "normal_extended.csv"
        hard_negative_tmp = tmp_dir / "hard_negative.csv"

        run_python(
            "18_collect_normal_extended.py",
            [
                "--generate-only",
                "--profiles",
                args.normal_profiles,
                "--runs-per-profile",
                str(args.normal_runs_per_profile),
                "--output-metadata",
                str(normal_tmp),
            ],
        )
        normal_header, normal_rows = load_rows(normal_tmp)

        run_python(
            "17_generate_normal_hard_negative_batch.py",
            [
                "--output",
                str(hard_negative_tmp),
                "--train-runs",
                str(args.train_runs),
                "--val-runs",
                str(args.val_runs),
                "--test-runs",
                str(args.test_runs),
                "--hard-negative-profile",
                args.hard_negative_profile,
                "--hard-negative-scenarios",
                args.hard_negative_scenarios,
            ],
        )
        hard_negative_header, hard_negative_generated_rows = load_rows(hard_negative_tmp)
        hard_negative_rows = [
            row
            for row in hard_negative_generated_rows
            if (row.get("sample_class") or "").strip().lower() == "hard-negative"
        ]

        header = normal_header or hard_negative_header
        if not header:
            raise ValueError("Could not resolve CSV header for merged metadata batch.")

        merged_rows = merge_rows(normal_rows, hard_negative_rows)
        save_rows(args.output, header, merged_rows)

    print(f"Generated merged batch: {args.output}")
    print(f"normal_rows={len(normal_rows)}")
    print(f"hard_negative_rows={len(hard_negative_rows)}")
    print(f"total_rows={len(merged_rows)}")

    if args.mode != "generate-only":
        batch_args = [
            "--metadata-file",
            str(args.output),
            "--namespace",
            args.namespace,
            "--jaeger-url",
            args.jaeger_url,
        ]
        if args.clean:
            batch_args.append("--clean")
        if args.mode == "collect-export":
            batch_args.extend(["--mode", "collect-export"])
        elif args.mode == "all":
            batch_args.extend(["--mode", "all"])
        elif args.mode == "rebuild-only":
            batch_args.extend(["--mode", "rebuild-only"])
        run_python("13_run_batch_dataset.py", batch_args)


if __name__ == "__main__":
    main()
