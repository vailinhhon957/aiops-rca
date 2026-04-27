import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import error, request

import pandas as pd

REQUIRED_FIELDS = [
    "trace_id",
    "span_count",
    "service_count",
    "app_service_count",
    "avg_latency",
    "max_latency",
    "std_latency",
    "trace_latency",
    "error_rate",
    "http_5xx_rate",
    "depth",
]


def parse_args():
    base = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Read traces from CSV or JSON and call /detect API.")
    parser.add_argument("--csv-path", default=str(base / "dataset" / "anomaly_final.csv"))
    parser.add_argument("--request-json", default=None)
    parser.add_argument("--api-url", default="http://127.0.0.1:8000/detect")
    parser.add_argument("--scenario", default="normal_low")
    parser.add_argument("--source-file", default=None)
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--group-id", default=None)
    parser.add_argument("--output-dir", default=str(base / "demo_outputs"))
    parser.add_argument("--save-files", action="store_true")
    return parser.parse_args()


def choose_batch(df: pd.DataFrame, scenario: str | None, source_file: str | None, count: int):
    work = df.copy().reset_index(drop=True)
    work["row_order"] = work.index

    if source_file:
        picked = work[work["source_file"] == source_file].copy()
        if picked.empty:
            raise ValueError(f"Khong tim thay source_file: {source_file}")
        return picked.head(count), source_file

    if scenario:
        scenario_df = work[work["scenario"] == scenario].copy()
        if scenario_df.empty and scenario == "normal":
            scenario_df = work[work["scenario"].astype(str).str.startswith("normal")].copy()
        if scenario_df.empty:
            raise ValueError(f"Khong tim thay scenario: {scenario}")

        grouped = scenario_df.groupby("source_file", sort=False)
        for file_name, group in grouped:
            if len(group) >= count:
                return group.head(count).copy(), file_name

        if len(scenario_df) >= count:
            return scenario_df.head(count).copy(), f"scenario_{scenario}"

        raise ValueError(f"Scenario {scenario} chi co {len(scenario_df)} traces, khong du {count}.")

    if len(work) < count:
        raise ValueError(f"CSV chi co {len(work)} rows, khong du {count}.")
    return work.head(count).copy(), "default_batch"


def build_payload(batch_df: pd.DataFrame, group_id: str):
    start_time = datetime(2026, 3, 21, 20, 0, 0, tzinfo=timezone.utc)
    traces = []
    for idx, row in batch_df.reset_index(drop=True).iterrows():
        trace = {
            "trace_id": str(row["trace_id"]),
            "timestamp": (start_time + timedelta(seconds=idx)).isoformat().replace("+00:00", "Z"),
            "span_count": float(row["span_count"]),
            "service_count": float(row["service_count"]),
            "app_service_count": float(row["app_service_count"]),
            "avg_latency": float(row["avg_latency"]),
            "max_latency": float(row["max_latency"]),
            "std_latency": float(row["std_latency"]),
            "trace_latency": float(row["trace_latency"]),
            "error_rate": float(row["error_rate"]),
            "http_5xx_rate": float(row["http_5xx_rate"]),
            "depth": float(row["depth"]),
            "latency_zscore": float(row["latency_zscore"]) if "latency_zscore" in row else 0.0,
            "duration_ratio": float(row["duration_ratio"]) if "duration_ratio" in row else 0.0,
        }
        traces.append(trace)
    return {"group_id": group_id, "traces": traces}


def post_detect(api_url: str, payload: dict):
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        api_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=60) as resp:
            data = resp.read().decode("utf-8")
            return resp.status, json.loads(data)
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"API loi HTTP {exc.code}: {detail}") from exc
    except error.URLError as exc:
        raise RuntimeError(
            "Khong goi duoc API. Hay chac chan server dang chay tai /detect."
        ) from exc


def main():
    args = parse_args()

    if args.request_json:
        request_path = Path(args.request_json)
        payload = json.loads(request_path.read_text(encoding="utf-8"))
        picked_source = request_path.name
        scenario_name = request_path.stem
        row_count = len(payload.get("traces", []))
    else:
        df = pd.read_csv(args.csv_path)
        missing = [col for col in REQUIRED_FIELDS + ["scenario", "source_file"] if col not in df.columns]
        if missing:
            raise ValueError(f"CSV thieu cot: {', '.join(missing)}")

        batch_df, picked_source = choose_batch(df, args.scenario, args.source_file, args.count)
        scenario_name = str(batch_df.iloc[0]["scenario"])
        group_id = args.group_id or f"demo_{(args.scenario or 'batch')}"
        payload = build_payload(batch_df, group_id)
        row_count = len(batch_df)

    status, response = post_detect(args.api_url, payload)

    print(f"HTTP status: {status}")
    print(f"Scenario/Input: {scenario_name}")
    print(f"Source: {picked_source}")
    print(f"Rows sent: {row_count}")
    print(f"Model: {response.get('model')}")
    print(f"Threshold: {response.get('threshold')}")
    print(f"Total windows: {response.get('total_windows')}")
    print(f"Anomaly windows: {response.get('anomaly_windows')}")

    preds = response.get("predictions", [])
    if preds:
        last = preds[-1]
        print("Last prediction:")
        print(json.dumps(last, indent=2, ensure_ascii=False))

    if args.save_files:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        safe_name = scenario_name.replace(" ", "_")
        request_path = output_dir / f"{safe_name}_detect_request.json"
        response_path = output_dir / f"{safe_name}_detect_response.json"
        request_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        response_path.write_text(json.dumps(response, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Saved request to: {request_path}")
        print(f"Saved response to: {response_path}")


if __name__ == "__main__":
    main()

