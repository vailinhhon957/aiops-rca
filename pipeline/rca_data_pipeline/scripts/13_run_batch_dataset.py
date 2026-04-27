from __future__ import annotations

import argparse
import csv
import shutil
import stat
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.rca_data_pipeline.config import DATA_ROOT, INTERIM_ROOT, LEGACY_DATASET_ROOT, PROCESSED_ROOT, RAW_ROOT, SPLITS_ROOT


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_METADATA_FILE = ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "batch1_fill_only.csv"
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
PIPELINE_SCRIPTS = [
    "02_import_legacy_dataset.py",
    "03_parse_traces.py",
    "04_clean_spans.py",
    "05_build_trace_features.py",
    "05b_build_window_features.py",
    "06_label_anomaly.py",
    "07_build_service_graphs.py",
    "08_export_graph_dataset.py",
    "09_make_splits.py",
]


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def run_python(script_name: str, args: list[str]) -> None:
    script_path = SCRIPT_DIR / script_name
    subprocess.run([sys.executable, str(script_path), *args], check=True)


def remove_tree_robust(path: Path) -> None:
    def _onerror(func, value, exc_info):  # type: ignore[no-untyped-def]
        target = Path(value)
        try:
            target.chmod(stat.S_IWRITE)
        except OSError:
            pass
        func(value)

    shutil.rmtree(path, onerror=_onerror)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a whole supported batch: collect runs, export Jaeger traces, and rebuild the dataset."
    )
    parser.add_argument("--metadata-file", type=Path, default=DEFAULT_METADATA_FILE)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--jaeger-url", default="http://127.0.0.1:16686")
    parser.add_argument("--service", default="frontend")
    parser.add_argument("--query-limit", type=int, default=500)
    parser.add_argument("--warmup-seconds", type=int, default=60)
    parser.add_argument("--cooldown-seconds", type=int, default=45)
    parser.add_argument("--fault-duration-seconds", type=int, default=60)
    parser.add_argument("--replica-drop-to", type=int, default=0)
    parser.add_argument(
        "--mode",
        choices=["all", "collect-export", "rebuild-only"],
        default="all",
        help="`all` = collect/export/rebuild, `collect-export` = only collect and export, `rebuild-only` = only rebuild from metadata + raw traces.",
    )
    parser.add_argument(
        "--run-ids",
        default="",
        help="Comma-separated run ids to execute. Leave empty to auto-select all supported rows in the metadata file.",
    )
    parser.add_argument(
        "--split-tags",
        default="",
        help="Comma-separated split tags to keep, e.g. train,val. Applied before run-id filtering.",
    )
    parser.add_argument(
        "--fail-on-unsupported",
        action="store_true",
        help="Fail immediately if the metadata file contains selected runs with unsupported fault types.",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop the batch at the first failed run instead of continuing with the next one.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete local pipeline dataset directories (data/raw, data/interim, data/processed, data/splits) before rebuilding.",
    )
    return parser.parse_args()


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


def supported_rows(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    supported = []
    unsupported = []
    for row in rows:
        fault_type = (row.get("fault_type") or "none").strip().lower()
        if fault_type in SUPPORTED_FAULT_TYPES:
            supported.append(row)
        else:
            unsupported.append(row)
    return supported, unsupported


def collect_and_export_run(args: argparse.Namespace, row: dict[str, str]) -> None:
    run_id = (row.get("run_id") or "").strip()
    fault_type = (row.get("fault_type") or "none").strip().lower()
    print(f"[batch] collecting run_id={run_id} fault_type={fault_type}", flush=True)
    run_python(
        "11_collect_run.py",
        [
            "--run-id",
            run_id,
            "--metadata-file",
            str(args.metadata_file),
            "--namespace",
            args.namespace,
            "--warmup-seconds",
            str(args.warmup_seconds),
            "--cooldown-seconds",
            str(args.cooldown_seconds),
            "--fault-duration-seconds",
            str(args.fault_duration_seconds),
            "--replica-drop-to",
            str(args.replica_drop_to),
        ],
    )
    print(f"[batch] exporting Jaeger run_id={run_id}", flush=True)
    run_python(
        "12_export_jaeger_run.py",
        [
            "--run-id",
            run_id,
            "--metadata-file",
            str(args.metadata_file),
            "--jaeger-url",
            args.jaeger_url,
            "--service",
            args.service,
            "--query-limit",
            str(args.query_limit),
        ],
    )


def clean_local_dataset_roots() -> None:
    roots = [RAW_ROOT, INTERIM_ROOT, PROCESSED_ROOT, SPLITS_ROOT]
    data_root_resolved = DATA_ROOT.resolve()
    for root in roots:
        resolved = root.resolve()
        if data_root_resolved not in resolved.parents:
            raise ValueError(f"Refusing to delete path outside DATA_ROOT: {resolved}")
        if resolved.exists():
            remove_tree_robust(resolved)
        resolved.mkdir(parents=True, exist_ok=True)


def rebuild_dataset(metadata_file: Path) -> None:
    print("[batch] rebuilding dataset", flush=True)
    run_python("02_import_legacy_dataset.py", ["--metadata", str(metadata_file), "--raw-root", str(LEGACY_DATASET_ROOT / "raw")])
    for script_name in PIPELINE_SCRIPTS[1:]:
        print(f"[batch] running {script_name}", flush=True)
        run_python(script_name, [])


def main() -> None:
    args = parse_args()
    rows = load_rows(args.metadata_file)
    selected_rows = select_rows(rows, normalize_csv_list(args.run_ids), normalize_csv_list(args.split_tags))
    if not selected_rows:
        raise ValueError("No rows selected from metadata file.")

    runnable_rows, skipped_rows = supported_rows(selected_rows)
    if skipped_rows and args.fail_on_unsupported and args.mode != "rebuild-only":
        skipped_desc = ", ".join(
            f"{row.get('run_id')}({(row.get('fault_type') or 'none').strip()})"
            for row in skipped_rows
        )
        raise ValueError(f"Unsupported fault types found in selected rows: {skipped_desc}")

    completed: list[str] = []
    failed: list[str] = []

    if args.mode in {"all", "collect-export"}:
        for row in runnable_rows:
            run_id = (row.get("run_id") or "").strip()
            try:
                collect_and_export_run(args, row)
                completed.append(run_id)
            except subprocess.CalledProcessError:
                failed.append(run_id)
                if args.stop_on_error:
                    raise

    if args.mode in {"all", "rebuild-only"} and not failed:
        if args.clean:
            print("[batch] cleaning local dataset roots", flush=True)
            clean_local_dataset_roots()
        rebuild_dataset(args.metadata_file)

    print("Batch run finished.")
    print(f"metadata_file={args.metadata_file}")
    print(f"selected_runs={len(selected_rows)}")
    print(f"supported_runs={len(runnable_rows)}")
    print(f"skipped_unsupported={len(skipped_rows)}")
    if skipped_rows:
        print("unsupported_run_ids=" + ",".join((row.get("run_id") or "").strip() for row in skipped_rows))
    print(f"completed_runs={len(completed)}")
    if completed:
        print("completed_run_ids=" + ",".join(completed))
    print(f"failed_runs={len(failed)}")
    if failed:
        print("failed_run_ids=" + ",".join(failed))
    if args.mode in {"all", "rebuild-only"} and not failed:
        print("dataset_rebuild=done")
    print(f"clean_requested={args.clean}")


if __name__ == "__main__":
    main()
