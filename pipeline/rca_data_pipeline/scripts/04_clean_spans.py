from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.rca_data_pipeline.config import SPANS_ROOT
from pipeline.rca_data_pipeline.feature_engineering import clean_spans
from pipeline.rca_data_pipeline.io_utils import read_table, write_table


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean span tables and drop invalid traces/spans.")
    parser.add_argument("--spans-root", type=Path, default=SPANS_ROOT)
    args = parser.parse_args()

    cleaned_count = 0
    span_files = sorted(list(args.spans_root.glob("spans_*.parquet")) + list(args.spans_root.glob("spans_*.csv")))
    for span_file in span_files:
        if "_clean" in span_file.stem:
            continue
        spans_df = read_table(span_file)
        cleaned_df = clean_spans(spans_df)
        write_table(cleaned_df, args.spans_root / f"{span_file.stem}_clean")
        cleaned_count += 1

    print(f"Cleaned {cleaned_count} span tables")


if __name__ == "__main__":
    main()
