from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.rca_data_pipeline.config import DEFAULT_WINDOW_ID, RAW_ROOT
from pipeline.rca_data_pipeline.io_utils import append_jsonl, ensure_dir, save_json


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a new raw run skeleton for future data collection.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--scenario-name", required=True)
    parser.add_argument("--label", type=int, default=0)
    parser.add_argument("--fault-type", default="none")
    parser.add_argument("--root-cause-service", default="none")
    parser.add_argument("--source-service", default="frontend")
    parser.add_argument("--notes", default="")
    args = parser.parse_args()

    now = datetime.now(timezone.utc).isoformat()
    run_root = ensure_dir(RAW_ROOT / args.run_id)
    ensure_dir(run_root / "windows")
    run_meta = {
        "run_id": args.run_id,
        "scenario_name": args.scenario_name,
        "trace_file": f"{DEFAULT_WINDOW_ID}.json",
        "label": args.label,
        "fault_type": args.fault_type,
        "root_cause_service": args.root_cause_service,
        "source_service": args.source_service,
        "start_time": now,
        "end_time": now,
        "notes": args.notes,
    }
    save_json(run_root / "run_meta.json", run_meta)
    append_jsonl(
        run_root / "events.jsonl",
        [{"ts": now, "event": "run_created", "run_id": args.run_id, "scenario_name": args.scenario_name}],
    )
    print(json.dumps({"status": "ok", "run_root": str(run_root)}, indent=2))


if __name__ == "__main__":
    main()
