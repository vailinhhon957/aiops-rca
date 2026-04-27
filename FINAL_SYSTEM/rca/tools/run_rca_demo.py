import argparse
import json
from pathlib import Path
from urllib import error, request


def parse_args():
    base = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description="Call RCA API with a sample graph from dataset.")
    parser.add_argument(
        "--graph-path",
        default=str(base / "rca" / "dataset" / "graph_dataset_final.json"),
    )
    parser.add_argument("--api-url", default="http://127.0.0.1:8100/rank")
    parser.add_argument("--scenario", default=None)
    parser.add_argument("--trace-id", default=None)
    parser.add_argument("--top-k", type=int, default=3)
    parser.add_argument("--output-dir", default=str(base / "demo_outputs"))
    parser.add_argument("--save-files", action="store_true")
    return parser.parse_args()


def choose_graph(records: list[dict], scenario: str | None, trace_id: str | None):
    if trace_id:
        for record in records:
            if record.get("trace_id") == trace_id:
                return record
        raise ValueError(f"Khong tim thay trace_id: {trace_id}")

    if scenario:
        for record in records:
            if record.get("scenario") == scenario and int(record.get("graph_label", 0)) == 1:
                return record
        raise ValueError(f"Khong tim thay graph anomaly cho scenario: {scenario}")

    for record in records:
        if int(record.get("graph_label", 0)) == 1:
            return record
    raise ValueError("Khong tim thay graph anomaly de demo.")


def post_rank(api_url: str, payload: dict):
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        api_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"RCA API loi HTTP {exc.code}: {detail}") from exc
    except error.URLError as exc:
        raise RuntimeError("Khong goi duoc RCA API. Hay chac chan server dang chay.") from exc


def main():
    args = parse_args()
    records = json.loads(Path(args.graph_path).read_text(encoding="utf-8"))
    graph = choose_graph(records, args.scenario, args.trace_id)

    payload = {
        "graph": {
            "trace_id": graph.get("trace_id"),
            "scenario": graph.get("scenario"),
            "x": graph.get("x"),
            "edge_index": graph.get("edge_index"),
        },
        "top_k": args.top_k,
    }

    status, response = post_rank(args.api_url, payload)
    print(f"HTTP status: {status}")
    print(f"Trace ID: {payload['graph']['trace_id']}")
    print(f"Scenario: {payload['graph']['scenario']}")
    print(f"Model: {response.get('model')}")

    predicted = response.get("predicted_root_cause", {})
    print("Predicted root cause:")
    print(json.dumps(predicted, indent=2, ensure_ascii=False))

    ranking = response.get("ranking", [])
    if ranking:
        print("Top ranking:")
        print(json.dumps(ranking, indent=2, ensure_ascii=False))

    if args.save_files:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        safe_name = str(payload["graph"]["scenario"]).replace(" ", "_")
        request_path = output_dir / f"{safe_name}_rca_request.json"
        response_path = output_dir / f"{safe_name}_rca_response.json"
        request_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        response_path.write_text(json.dumps(response, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Saved request to: {request_path}")
        print(f"Saved response to: {response_path}")


if __name__ == "__main__":
    main()

