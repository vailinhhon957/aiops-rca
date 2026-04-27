from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.rca_data_pipeline.config import DEFAULT_SERVICE_CATALOG, RAW_ROOT, SPANS_ROOT, TRACE_TABLE_ROOT
from pipeline.rca_data_pipeline.io_utils import load_json, write_table
from pipeline.rca_data_pipeline.jaeger_parser import parse_jaeger_payload
from pipeline.rca_data_pipeline.service_catalog import load_service_catalog, service_lookup


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse Jaeger raw JSON files into span and trace tables.")
    parser.add_argument("--raw-root", type=Path, default=RAW_ROOT)
    args = parser.parse_args()

    parsed = 0
    skipped_files = 0
    for run_root in sorted(args.raw_root.iterdir()):
        if not run_root.is_dir():
            continue
        run_meta_path = run_root / "run_meta.json"
        windows_root = run_root / "windows"
        if not run_meta_path.exists() or not windows_root.exists():
            continue

        run_meta = load_json(run_meta_path)
        catalog_path = run_root / "service_catalog.json"
        if not catalog_path.exists():
            catalog_path = DEFAULT_SERVICE_CATALOG
        catalog_lookup = service_lookup(load_service_catalog(catalog_path))
        span_frames = []
        trace_frames = []
        for raw_file in sorted(windows_root.glob("*.json")):
            try:
                payload = load_json(raw_file)
                spans_df, traces_df = parse_jaeger_payload(
                    payload=payload,
                    run_id=run_meta["run_id"],
                    window_id=raw_file.stem,
                    service_metadata_lookup=catalog_lookup,
                    system_id=str(run_meta.get("system_id", "unknown")),
                )
            except Exception as exc:
                skipped_files += 1
                print(f"skip invalid raw file: {raw_file} ({exc})")
                continue
            if not spans_df.empty:
                span_frames.append(spans_df)
            if not traces_df.empty:
                trace_frames.append(traces_df)

        if not span_frames:
            print(f"skip empty run: {run_meta['run_id']}")
            continue

        all_spans = pd.concat(span_frames, ignore_index=True)
        all_traces = pd.concat(trace_frames, ignore_index=True) if trace_frames else pd.DataFrame()
        write_table(all_spans, SPANS_ROOT / f"spans_{run_meta['run_id']}")
        if not all_traces.empty:
            write_table(all_traces, TRACE_TABLE_ROOT / f"trace_table_{run_meta['run_id']}")
        parsed += 1

    print(f"Parsed {parsed} runs")
    print(f"Skipped raw files: {skipped_files}")


if __name__ == "__main__":
    main()
