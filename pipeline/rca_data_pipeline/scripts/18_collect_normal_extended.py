"""
18_collect_normal_extended.py

Thu thap them normal (label=0) baseline runs voi nhieu load profiles hon.

Profiles hien tai (script 17): low / medium / high  (3 profiles)
Profiles moi them:
  idle        : USERS=2,  RATE=1   -- traffic nhe nhat
  very_low    : USERS=3,  RATE=1
  low         : USERS=5,  RATE=1   (trung voi existing)
  low_medium  : USERS=8,  RATE=3
  medium      : USERS=10, RATE=5   (trung voi existing)
  medium_high : USERS=15, RATE=7
  high        : USERS=20, RATE=10  (trung voi existing)
  very_high   : USERS=30, RATE=15
  burst       : USERS=50, RATE=25  -- simulate traffic spike

Usage:
  # Xem truoc se collect gi (khong thuc hien gi)
  python 18_collect_normal_extended.py --dry-run

  # Chi generate metadata CSV, khong collect
  python 18_collect_normal_extended.py --generate-only

  # Thu thap 2 runs/profile cho tat ca profiles moi
  python 18_collect_normal_extended.py --runs-per-profile 2

  # Chi thu thap mot so profiles cu the
  python 18_collect_normal_extended.py --profiles idle,very_high,burst --runs-per-profile 3

  # Thu thap va rebuild dataset sau do
  python 18_collect_normal_extended.py --runs-per-profile 2 --rebuild

Prerequisites:
  1. kubectl port-forward svc/jaeger 16686:16686 -n <namespace>   # chay truoc
  2. kubectl co quyen tao/xoa pod trong namespace
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

# ── Duong dan ──────────────────────────────────────────────────────────────
ROOT       = Path(__file__).resolve().parents[3]
DATA_ROOT  = ROOT / "data"
RAW_ROOT   = DATA_ROOT / "raw"
SCRIPTS_DIR = Path(__file__).resolve().parent

SERVICE_CATALOG_TEMPLATE = ROOT / "pipeline" / "rca_data_pipeline" / "service_catalog_online_boutique.json"
METADATA_OUTPUT = ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "normal_extended_batch.csv"

LOADGEN_NAME  = "simple-loadgen"
LOADGEN_IMAGE = "us-central1-docker.pkg.dev/google-samples/microservices-demo/loadgenerator:v0.10.5"

SYSTEM_ID         = "online-boutique"
SYSTEM_FAMILY     = "ecommerce"
TOPOLOGY_VERSION  = "online-boutique-v0.10.5"

# ── Load profiles: (users, spawn_rate) ────────────────────────────────────
# spawn_rate = toc do tao user/giay (khong phai req/s)
LOAD_PROFILES: dict[str, tuple[int, int]] = {
    "idle":        (2,  1),
    "very_low":    (3,  1),
    "low":         (5,  1),   # trung voi existing
    "low_medium":  (8,  3),
    "medium":      (10, 5),   # trung voi existing
    "medium_high": (15, 7),
    "high":        (20, 10),  # trung voi existing
    "very_high":   (30, 15),
    "burst":       (50, 25),
}

# Warmup / collection / cooldown theo load profile
TIMING: dict[str, dict[str, int]] = {
    "idle":        {"warmup": 30, "collection": 60, "cooldown": 20, "query_limit": 200},
    "very_low":    {"warmup": 35, "collection": 60, "cooldown": 25, "query_limit": 250},
    "low":         {"warmup": 45, "collection": 60, "cooldown": 30, "query_limit": 350},
    "low_medium":  {"warmup": 50, "collection": 70, "cooldown": 35, "query_limit": 450},
    "medium":      {"warmup": 60, "collection": 75, "cooldown": 40, "query_limit": 550},
    "medium_high": {"warmup": 65, "collection": 80, "cooldown": 45, "query_limit": 650},
    "high":        {"warmup": 75, "collection": 90, "cooldown": 50, "query_limit": 800},
    "very_high":   {"warmup": 80, "collection": 90, "cooldown": 55, "query_limit": 900},
    "burst":       {"warmup": 90, "collection": 90, "cooldown": 60, "query_limit": 1000},
}

# Metadata CSV header (tuong thich voi batch1_fill_only.csv)
METADATA_HEADER = [
    "run_id", "trace_file", "label", "sample_class", "phase_policy",
    "fault_family", "fault_type", "root_cause_service", "fault_target_service",
    "fault_target_role", "source_service", "source_service_role",
    "system_id", "system_family", "topology_version",
    "experiment_group", "chaos_name", "chaos_kind",
    "target_service", "target_pod", "target_container",
    "severity", "load_profile", "split_tag",
    "start_time", "fault_start_time", "fault_end_time", "end_time",
    "export_duration_ms", "query_limit", "query_lookback",
    "trace_count", "span_count_total", "avg_spans_per_trace",
    "unique_service_count", "unique_services", "root_cause_trace_hits",
    "health_trace_count", "otel_export_trace_count", "business_trace_count",
    "notes",
    "warmup_seconds", "cooldown_seconds", "fault_duration_seconds",
    "replica_drop_to", "cpu_request_m", "cpu_limit_m",
    "memory_request_mib", "memory_limit_mib",
    "latency_delay_seconds", "pod_kill_repeats", "pod_kill_interval_seconds",
]


# ── Helpers ────────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_cmd(args: list[str], capture: bool = False) -> str:
    result = subprocess.run(args, check=True, text=True, capture_output=capture)
    return result.stdout.strip() if capture else ""


def kubectl(args: list[str], namespace: str, capture: bool = False) -> str:
    return run_cmd(["kubectl", "-n", namespace] + args, capture=capture)


def iso_to_unix_micros(iso: str) -> int:
    return int(datetime.fromisoformat(iso).timestamp() * 1_000_000)


def save_json(path: Path, data: object) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def append_jsonl(path: Path, records: list[dict]) -> None:
    with path.open("a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")


def load_service_catalog() -> list[dict]:
    if SERVICE_CATALOG_TEMPLATE.exists():
        with SERVICE_CATALOG_TEMPLATE.open(encoding="utf-8") as f:
            return json.load(f)
    # Fallback minimal catalog
    return [{"service_name": svc, "service_role": "backend", "service_tier": "backend",
              "criticality": "high", "is_entrypoint": int(svc == "frontend"), "is_stateful": 0}
            for svc in ["frontend", "checkoutservice", "cartservice", "paymentservice",
                        "productcatalogservice", "recommendationservice", "emailservice"]]


def split_sequence(n: int) -> list[str]:
    """Assign train/val/test splits: ~60% train, ~20% val, ~20% test."""
    splits = []
    for i in range(n):
        ratio = i / n
        if ratio < 0.6:
            splits.append("train")
        elif ratio < 0.8:
            splits.append("val")
        else:
            splits.append("test")
    return splits


def severity_for_profile(profile: str) -> str:
    low_profiles  = {"idle", "very_low", "low"}
    high_profiles = {"very_high", "burst"}
    if profile in low_profiles:
        return "low"
    if profile in high_profiles:
        return "high"
    return "medium"


# ── Locust control ─────────────────────────────────────────────────────────

def disable_builtin_loadgen(namespace: str) -> None:
    subprocess.run(
        ["kubectl", "-n", namespace, "scale", "deployment", "loadgenerator", "--replicas=0"],
        check=False, text=True, capture_output=True,
    )


def start_loadgen(namespace: str, users: int, rate: int) -> None:
    # Xoa pod cu neu co
    subprocess.run(
        ["kubectl", "-n", namespace, "delete", "pod", LOADGEN_NAME, "--ignore-not-found"],
        check=False, text=True, capture_output=True,
    )
    kubectl([
        "run", LOADGEN_NAME,
        "--image", LOADGEN_IMAGE,
        "--restart=Never",
        "--env", "FRONTEND_ADDR=frontend:80",
        "--env", f"USERS={users}",
        "--env", f"RATE={rate}",
    ], namespace=namespace)
    kubectl(["wait", "--for=condition=Ready", f"pod/{LOADGEN_NAME}", "--timeout=120s"], namespace=namespace)
    print(f"  [loadgen] started USERS={users} RATE={rate}")


def stop_loadgen(namespace: str) -> None:
    subprocess.run(
        ["kubectl", "-n", namespace, "delete", "pod", LOADGEN_NAME, "--ignore-not-found"],
        check=False, text=True, capture_output=True,
    )
    print("  [loadgen] stopped")


# ── Jaeger export ──────────────────────────────────────────────────────────

def fetch_jaeger_traces(jaeger_url: str, start_micros: int, end_micros: int, limit: int) -> tuple[dict, float]:
    params = urlencode({
        "service": "frontend",
        "start": start_micros,
        "end": end_micros,
        "limit": limit,
    })
    url = f"{jaeger_url.rstrip('/')}/api/traces?{params}"
    t0 = time.perf_counter()
    with urlopen(url, timeout=120) as resp:
        payload = json.loads(resp.read().decode("utf-8-sig"))
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return payload, elapsed_ms


def summarize_payload(payload: dict) -> dict:
    traces = payload.get("data", []) or []
    span_total = 0
    services: set[str] = set()
    for tr in traces:
        procs = tr.get("processes", {}) or {}
        spans = tr.get("spans", []) or []
        span_total += len(spans)
        for sp in spans:
            svc = procs.get(sp.get("processID", ""), {}).get("serviceName", "")
            if svc:
                services.add(svc)
    n = len(traces)
    return {
        "trace_count": n,
        "span_count_total": span_total,
        "avg_spans_per_trace": round(span_total / n, 2) if n else 0.0,
        "unique_service_count": len(services),
        "unique_services": ";".join(sorted(services)),
        "root_cause_trace_hits": 0,
        "health_trace_count": 0,
        "otel_export_trace_count": 0,
        "business_trace_count": n,
    }


# ── Run folder management ──────────────────────────────────────────────────

def find_next_seq(profile: str) -> int:
    """Tim so thu tu tiep theo cho ob_norm_{profile}_NNN."""
    existing = [
        int(p.name.rsplit("_", 1)[-1])
        for p in RAW_ROOT.glob(f"ob_norm_{profile}_???")
        if p.is_dir() and p.name.rsplit("_", 1)[-1].isdigit()
    ]
    return max(existing, default=0) + 1


def build_run_meta(run_id: str, profile: str, split_tag: str, timing: dict,
                   start_time: str, end_time: str, stats: dict, export_ms: float) -> dict:
    sev = severity_for_profile(profile)
    users, rate = LOAD_PROFILES[profile]
    return {
        "run_id": run_id,
        "system_id": SYSTEM_ID,
        "system_family": SYSTEM_FAMILY,
        "topology_version": TOPOLOGY_VERSION,
        "scenario_name": f"normal_{profile}_load",
        "trace_file": f"{run_id}.json",
        "label": 0,
        "fault_type": "none",
        "fault_family": "none",
        "root_cause_service": "none",
        "fault_target_service": "none",
        "fault_target_role": "none",
        "source_service": "frontend",
        "source_service_role": "entrypoint",
        "sample_class": "normal",
        "phase_policy": "steady",
        "split_tag": split_tag,
        "start_time": start_time,
        "fault_start_time": None,
        "fault_end_time": None,
        "end_time": end_time,
        "notes": (
            f"baseline {profile} load; extended normal batch; "
            f"users={users}; spawn_rate={rate}; severity={sev}; "
            f"load={profile}; query_limit={timing['query_limit']}"
        ),
        "legacy_metadata": {
            "run_id": run_id,
            "trace_file": f"{run_id}.json",
            "label": 0,
            "fault_family": "none",
            "fault_type": "none",
            "root_cause_service": "none",
            "fault_target_service": "none",
            "fault_target_role": "none",
            "source_service": "frontend",
            "source_service_role": "entrypoint",
            "system_id": SYSTEM_ID,
            "system_family": SYSTEM_FAMILY,
            "topology_version": TOPOLOGY_VERSION,
            "experiment_group": "normal",
            "chaos_name": "none",
            "chaos_kind": "none",
            "target_service": "none",
            "target_pod": None,
            "target_container": None,
            "severity": sev,
            "load_profile": profile,
            "split_tag": split_tag,
            "start_time": start_time,
            "fault_start_time": None,
            "fault_end_time": None,
            "end_time": end_time,
            "export_duration_ms": round(export_ms, 2),
            "query_limit": timing["query_limit"],
            "query_lookback": "custom-window",
            "warmup_seconds": timing["warmup"],
            "cooldown_seconds": timing["cooldown"],
            **stats,
        },
    }


def build_metadata_row(run_id: str, profile: str, split_tag: str,
                       timing: dict, stats: dict | None = None,
                       start_time: str = "", end_time: str = "",
                       export_ms: float = 0.0) -> dict:
    sev = severity_for_profile(profile)
    st = stats or {}
    return {
        "run_id": run_id,
        "trace_file": f"{run_id}.json",
        "label": "0",
        "sample_class": "normal",
        "phase_policy": "steady",
        "fault_family": "none",
        "fault_type": "none",
        "root_cause_service": "none",
        "fault_target_service": "none",
        "fault_target_role": "none",
        "source_service": "frontend",
        "source_service_role": "entrypoint",
        "system_id": SYSTEM_ID,
        "system_family": SYSTEM_FAMILY,
        "topology_version": TOPOLOGY_VERSION,
        "experiment_group": "normal",
        "chaos_name": "none",
        "chaos_kind": "none",
        "target_service": "none",
        "target_pod": "",
        "target_container": "",
        "severity": sev,
        "load_profile": profile,
        "split_tag": split_tag,
        "start_time": start_time,
        "fault_start_time": "",
        "fault_end_time": "",
        "end_time": end_time,
        "export_duration_ms": str(round(export_ms, 2)) if export_ms else "",
        "query_limit": str(timing["query_limit"]),
        "query_lookback": "custom-window",
        "trace_count": str(st.get("trace_count", "")),
        "span_count_total": str(st.get("span_count_total", "")),
        "avg_spans_per_trace": str(st.get("avg_spans_per_trace", "")),
        "unique_service_count": str(st.get("unique_service_count", "")),
        "unique_services": st.get("unique_services", ""),
        "root_cause_trace_hits": "0",
        "health_trace_count": "0",
        "otel_export_trace_count": "0",
        "business_trace_count": str(st.get("trace_count", "")),
        "notes": (
            f"baseline {profile} load; extended normal batch; "
            f"severity={sev}; load={profile}"
        ),
        "warmup_seconds": str(timing["warmup"]),
        "cooldown_seconds": str(timing["cooldown"]),
        "fault_duration_seconds": "",
        "replica_drop_to": "",
        "cpu_request_m": "",
        "cpu_limit_m": "",
        "memory_request_mib": "",
        "memory_limit_mib": "",
        "latency_delay_seconds": "",
        "pod_kill_repeats": "",
        "pod_kill_interval_seconds": "",
    }


# ── Main collection logic ──────────────────────────────────────────────────

def collect_one_run(
    run_id: str,
    profile: str,
    split_tag: str,
    namespace: str,
    jaeger_url: str,
    timing: dict,
    dry_run: bool = False,
) -> dict | None:
    users, rate = LOAD_PROFILES[profile]
    run_dir = RAW_ROOT / run_id
    windows_dir = run_dir / "windows"

    print(f"\n[run] {run_id}  profile={profile}  split={split_tag}")
    print(f"      users={users} spawn_rate={rate}  "
          f"warmup={timing['warmup']}s  collection={timing['collection']}s  "
          f"cooldown={timing['cooldown']}s  query_limit={timing['query_limit']}")

    if dry_run:
        print("      [DRY RUN] skipping")
        return None

    # Tao thu muc
    windows_dir.mkdir(parents=True, exist_ok=True)

    # Ghi service_catalog.json
    save_json(run_dir / "service_catalog.json", load_service_catalog())

    # Bat loadgen
    disable_builtin_loadgen(namespace)
    start_loadgen(namespace, users, rate)

    # Warmup
    print(f"  [warmup] {timing['warmup']}s ...", flush=True)
    time.sleep(timing["warmup"])

    # Thu thap
    start_time = now_iso()
    print(f"  [collect] start_time={start_time}", flush=True)
    append_jsonl(run_dir / "events.jsonl", [
        {"ts": start_time, "event": "run_started", "run_id": run_id},
    ])

    time.sleep(timing["collection"])

    end_time = now_iso()
    print(f"  [collect] end_time={end_time}", flush=True)
    append_jsonl(run_dir / "events.jsonl", [
        {"ts": end_time, "event": "run_finished", "run_id": run_id},
    ])

    # Export Jaeger
    start_micros = iso_to_unix_micros(start_time)
    end_micros   = iso_to_unix_micros(end_time)
    print(f"  [jaeger] querying traces ...", flush=True)
    try:
        payload, export_ms = fetch_jaeger_traces(
            jaeger_url=jaeger_url,
            start_micros=start_micros,
            end_micros=end_micros,
            limit=timing["query_limit"],
        )
    except Exception as e:
        print(f"  [jaeger] ERROR: {e}", flush=True)
        stop_loadgen(namespace)
        return None

    stats = summarize_payload(payload)
    print(f"  [jaeger] traces={stats['trace_count']}  spans={stats['span_count_total']}  "
          f"services={stats['unique_service_count']}  export={export_ms:.0f}ms", flush=True)

    # Luu traces
    save_json(windows_dir / "traces_0001.json", payload)

    # Luu run_meta.json
    run_meta = build_run_meta(run_id, profile, split_tag, timing,
                               start_time, end_time, stats, export_ms)
    save_json(run_dir / "run_meta.json", run_meta)

    # Dung loadgen
    stop_loadgen(namespace)

    # Cooldown truoc run tiep theo
    print(f"  [cooldown] {timing['cooldown']}s ...", flush=True)
    time.sleep(timing["cooldown"])

    return build_metadata_row(run_id, profile, split_tag, timing, stats,
                               start_time, end_time, export_ms)


# ── CLI ────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect extended normal baseline runs with varied load profiles.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--namespace",      default="default",
                        help="Kubernetes namespace (default: default)")
    parser.add_argument("--jaeger-url",     default="http://127.0.0.1:16686",
                        help="Jaeger base URL (port-forward phai dang chay)")
    parser.add_argument("--runs-per-profile", type=int, default=3,
                        help="So runs thu thap cho moi load profile (default: 3)")
    parser.add_argument("--profiles",       default="",
                        help="Comma-separated profiles. Mac dinh: tat ca profiles moi "
                             "(idle,very_low,low_medium,medium_high,very_high,burst). "
                             "Dung 'all' de bao gom ca existing low/medium/high.")
    parser.add_argument("--dry-run",        action="store_true",
                        help="Hien thi ke hoach ma khong thuc hien collection")
    parser.add_argument("--generate-only",  action="store_true",
                        help="Chi tao metadata CSV, khong chay Locust/Jaeger")
    parser.add_argument("--rebuild",        action="store_true",
                        help="Chay pipeline rebuild sau khi collection xong")
    parser.add_argument("--output-metadata", type=Path, default=METADATA_OUTPUT,
                        help=f"Duong dan file CSV metadata output (default: {METADATA_OUTPUT})")
    return parser.parse_args()


def resolve_profiles(profiles_arg: str) -> list[str]:
    """Tra ve danh sach profiles can collect."""
    if not profiles_arg or profiles_arg.lower() == "new":
        # Mac dinh: chi thu thap profiles MOI (khong co trong existing data)
        return ["idle", "very_low", "low_medium", "medium_high", "very_high", "burst"]
    if profiles_arg.lower() == "all":
        return list(LOAD_PROFILES.keys())
    return [p.strip() for p in profiles_arg.split(",") if p.strip() in LOAD_PROFILES]


def main() -> None:
    args = parse_args()
    profiles = resolve_profiles(args.profiles)

    if not profiles:
        print("ERROR: Khong co profile nao hop le. Cac profiles kha dung:")
        for p in LOAD_PROFILES:
            users, rate = LOAD_PROFILES[p]
            print(f"  {p:<15} USERS={users:<3} RATE={rate}")
        sys.exit(1)

    # -- Lap ke hoach runs ------------------------------------------------
    plan: list[tuple[str, str, str, dict]] = []   # (run_id, profile, split_tag, timing)
    for profile in profiles:
        timing  = TIMING[profile]
        splits  = split_sequence(args.runs_per_profile)
        seq_start = find_next_seq(profile) if not args.generate_only and not args.dry_run else 1
        for i, split_tag in enumerate(splits):
            seq    = seq_start + i
            run_id = f"ob_norm_{profile}_{seq:03d}"
            plan.append((run_id, profile, split_tag, timing))

    # -- Hien thi ke hoach ------------------------------------------------
    total_time_min = sum(
        (t["warmup"] + t["collection"] + t["cooldown"]) / 60
        for _, _, _, t in plan
    )
    print("=" * 65)
    print(f"  NORMAL EXTENDED COLLECTION PLAN")
    print("=" * 65)
    print(f"  Profiles   : {', '.join(profiles)}")
    print(f"  Runs/profile: {args.runs_per_profile}")
    print(f"  Total runs : {len(plan)}")
    print(f"  Est. time  : ~{total_time_min:.0f} minutes")
    print(f"  Output CSV : {args.output_metadata}")
    print("-" * 65)
    print(f"  {'Run ID':<38} {'Profile':<14} {'Split':<6} {'Est min'}")
    print("-" * 65)
    for run_id, profile, split_tag, timing in plan:
        mins = (timing["warmup"] + timing["collection"] + timing["cooldown"]) / 60
        print(f"  {run_id:<38} {profile:<14} {split_tag:<6} {mins:.1f}")
    print("=" * 65)

    if args.dry_run:
        print("\n[DRY RUN] Khong thuc hien collection. Thoat.")
        return

    # -- Generate-only mode -----------------------------------------------
    if args.generate_only:
        rows = [
            build_metadata_row(run_id, profile, split_tag, timing)
            for run_id, profile, split_tag, timing in plan
        ]
        args.output_metadata.parent.mkdir(parents=True, exist_ok=True)
        with args.output_metadata.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=METADATA_HEADER)
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nGenerated {len(rows)} metadata rows -> {args.output_metadata}")
        print("Chay collection voi script 13_run_batch_dataset.py:")
        print(f"  python 13_run_batch_dataset.py --metadata-file {args.output_metadata} --mode all")
        return

    # -- Thuc te collection -----------------------------------------------
    metadata_rows: list[dict] = []
    failed_runs: list[str] = []

    RAW_ROOT.mkdir(parents=True, exist_ok=True)

    for run_id, profile, split_tag, timing in plan:
        try:
            row = collect_one_run(
                run_id=run_id,
                profile=profile,
                split_tag=split_tag,
                namespace=args.namespace,
                jaeger_url=args.jaeger_url,
                timing=timing,
                dry_run=False,
            )
            if row:
                metadata_rows.append(row)
                print(f"  [ok] {run_id}")
        except subprocess.CalledProcessError as e:
            print(f"  [FAIL] {run_id}: {e}", flush=True)
            failed_runs.append(run_id)
            # Dam bao loadgen duoc dung du co loi
            try:
                stop_loadgen(args.namespace)
            except Exception:
                pass

    # -- Ghi metadata CSV -------------------------------------------------
    args.output_metadata.parent.mkdir(parents=True, exist_ok=True)
    with args.output_metadata.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=METADATA_HEADER)
        writer.writeheader()
        writer.writerows(metadata_rows)

    print(f"\nHoan thanh: {len(metadata_rows)}/{len(plan)} runs thanh cong")
    if failed_runs:
        print(f"Loi: {', '.join(failed_runs)}")
    print(f"Metadata CSV: {args.output_metadata}")

    # -- Rebuild dataset --------------------------------------------------
    if args.rebuild and not failed_runs:
        print("\n[rebuild] Chay pipeline rebuild ...")
        metadata_file = args.output_metadata
        rebuild_scripts = [
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
        try:
            subprocess.run(
                [sys.executable, str(SCRIPTS_DIR / "02_import_legacy_dataset.py"),
                 "--metadata", str(metadata_file), "--raw-root", str(RAW_ROOT)],
                check=True,
            )
            for script in rebuild_scripts[1:]:
                print(f"  -> {script}", flush=True)
                subprocess.run([sys.executable, str(SCRIPTS_DIR / script)], check=True)
            print("[rebuild] Xong!")
        except subprocess.CalledProcessError as e:
            print(f"[rebuild] FAIL: {e}")

    print("\nBuoc tiep theo:")
    if failed_runs:
        print("  1. Kiem tra loi va thu lai cac runs bi fail")
    print(f"  2. Chay pipeline rebuild (neu chua dung --rebuild):")
    print(f"     python 13_run_batch_dataset.py \\")
    print(f"       --metadata-file {args.output_metadata} \\")
    print(f"       --mode rebuild-only")


if __name__ == "__main__":
    main()
