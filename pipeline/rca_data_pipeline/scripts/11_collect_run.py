from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


DEFAULT_METADATA_FILE = ROOT / "pipeline" / "rca_data_pipeline" / "templates" / "batch1_fill_only.csv"
LOADGEN_NAME = "simple-loadgen"
LOADGEN_IMAGE = "us-central1-docker.pkg.dev/google-samples/microservices-demo/loadgenerator:v0.10.5"


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def normalized_text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    if text.lower() == "nan":
        return default
    return text or default


def int_from_value(value: object, default: int) -> int:
    text = normalized_text(value)
    if not text:
        return default
    try:
        return int(float(text))
    except ValueError:
        return default


def row_int(row: dict[str, str], key: str, default: int) -> int:
    return int_from_value(row.get(key), default)


def severity_level(row: dict[str, str]) -> str:
    return normalized_text(row.get("severity"), "medium").lower()


def cpu_stress_resources(row: dict[str, str], original_resources: dict) -> dict[str, dict[str, str]]:
    severity = severity_level(row)
    request_default = {"low": "80m", "medium": "40m", "high": "20m"}
    limit_default = {"low": "120m", "medium": "70m", "high": "40m"}
    request_cpu = normalized_text(row.get("cpu_request_m"), request_default.get(severity, "40")).rstrip("m") + "m"
    limit_cpu = normalized_text(row.get("cpu_limit_m"), limit_default.get(severity, "70")).rstrip("m") + "m"
    return {
        "requests": {"cpu": request_cpu, "memory": original_resources.get("requests", {}).get("memory", "64Mi")},
        "limits": {"cpu": limit_cpu, "memory": original_resources.get("limits", {}).get("memory", "128Mi")},
    }


def memory_stress_resources(row: dict[str, str], original_resources: dict) -> dict[str, dict[str, str]]:
    severity = severity_level(row)
    request_default = {"low": "96Mi", "medium": "48Mi", "high": "24Mi"}
    limit_default = {"low": "128Mi", "medium": "80Mi", "high": "48Mi"}
    request_memory = normalized_text(row.get("memory_request_mib"), request_default.get(severity, "48").rstrip("Mi")).rstrip("Mi") + "Mi"
    limit_memory = normalized_text(row.get("memory_limit_mib"), limit_default.get(severity, "80").rstrip("Mi")).rstrip("Mi") + "Mi"
    return {
        "requests": {"cpu": original_resources.get("requests", {}).get("cpu", "100m"), "memory": request_memory},
        "limits": {"cpu": original_resources.get("limits", {}).get("cpu", "200m"), "memory": limit_memory},
    }


def run_cmd(args: list[str], capture_output: bool = False) -> str:
    result = subprocess.run(args, check=True, text=True, capture_output=capture_output)
    return result.stdout.strip() if capture_output else ""


def kubectl(args: list[str], namespace: str | None = None, capture_output: bool = False) -> str:
    cmd = ["kubectl"]
    if namespace:
        cmd.extend(["-n", namespace])
    cmd.extend(args)
    return run_cmd(cmd, capture_output=capture_output)


def kubectl_json(args: list[str], namespace: str | None = None) -> dict:
    output = kubectl([*args, "-o", "json"], namespace=namespace, capture_output=True)
    return json.loads(output)


def load_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return list(reader.fieldnames or []), rows


def save_rows(path: Path, header: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def build_load_profile_env(load_profile: str) -> tuple[str, str]:
    profile = (load_profile or "medium").strip().lower()
    profile_map = {
        "idle": ("2", "1"),
        "very_low": ("3", "1"),
        "low": ("5", "1"),
        "low_medium": ("8", "3"),
        "medium": ("10", "5"),
        "medium_high": ("15", "7"),
        "high": ("20", "10"),
        "very_high": ("30", "15"),
        "burst": ("50", "25"),
    }
    return profile_map.get(profile, profile_map["medium"])


def delete_pod_if_exists(name: str, namespace: str) -> None:
    subprocess.run(["kubectl", "-n", namespace, "delete", "pod", name, "--ignore-not-found"], check=False)


def start_loadgen(namespace: str, load_profile: str) -> None:
    delete_pod_if_exists(LOADGEN_NAME, namespace)
    users, rate = build_load_profile_env(load_profile)
    kubectl(
        [
            "run",
            LOADGEN_NAME,
            "--image",
            LOADGEN_IMAGE,
            "--restart=Never",
            "--env",
            "FRONTEND_ADDR=frontend:80",
            "--env",
            f"USERS={users}",
            "--env",
            f"RATE={rate}",
        ],
        namespace=namespace,
    )
    kubectl(["wait", "--for=condition=Ready", f"pod/{LOADGEN_NAME}", "--timeout=120s"], namespace=namespace)


def stop_loadgen(namespace: str) -> None:
    delete_pod_if_exists(LOADGEN_NAME, namespace)


def disable_builtin_loadgenerator(namespace: str) -> None:
    subprocess.run(
        ["kubectl", "-n", namespace, "scale", "deployment", "loadgenerator", "--replicas=0"],
        check=False,
        text=True,
    )


def scale_deployment(namespace: str, name: str, replicas: int) -> None:
    kubectl(["scale", "deployment", name, f"--replicas={replicas}"], namespace=namespace)
    kubectl(["rollout", "status", f"deployment/{name}", "--timeout=180s"], namespace=namespace)


def get_deployment(namespace: str, name: str) -> dict:
    return kubectl_json(["get", "deployment", name], namespace=namespace)


def get_service(namespace: str, name: str) -> dict:
    return kubectl_json(["get", "service", name], namespace=namespace)


def first_container_name(deployment_obj: dict) -> str:
    return deployment_obj["spec"]["template"]["spec"]["containers"][0]["name"]


def patch_deployment_container(namespace: str, deployment_name: str, container_name: str, fields: dict) -> None:
    patch = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": container_name,
                            **fields,
                        }
                    ]
                }
            }
        }
    }
    kubectl(["patch", "deployment", deployment_name, "--type", "strategic", "-p", json.dumps(patch)], namespace=namespace)
    try:
        kubectl(["rollout", "status", f"deployment/{deployment_name}", "--timeout=180s"], namespace=namespace)
    except subprocess.CalledProcessError:
        print(f"[collect] rollout status timed out for {deployment_name}, falling back to ready-pod check", flush=True)
        wait_for_first_ready_pod(namespace, deployment_name, timeout_seconds=180)


def patch_service_selector(namespace: str, service_name: str, selector: dict[str, str]) -> None:
    patch_ops = [{"op": "replace", "path": "/spec/selector", "value": selector}]
    kubectl(["patch", "service", service_name, "--type", "json", "-p", json.dumps(patch_ops)], namespace=namespace)


def create_fault_proxy_pod(namespace: str, pod_name: str, label_name: str, port: int, mode: str, delay_seconds: int = 3) -> None:
    delete_pod_if_exists(pod_name, namespace)
    if mode == "delay-close":
        command = f"while true; do {{ sleep {max(delay_seconds, 1)}; }} | nc -l -p {port} >/dev/null 2>&1; done"
    else:
        command = f"while true; do nc -l -p {port} >/dev/null 2>&1; done"
    manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
            "labels": {
                "fault-proxy": label_name,
            },
        },
        "spec": {
            "restartPolicy": "Always",
            "containers": [
                {
                    "name": "proxy",
                    "image": "busybox:1.36",
                    "command": ["sh", "-c", command],
                    "ports": [{"containerPort": port}],
                }
            ],
        },
    }
    subprocess.run(["kubectl", "-n", namespace, "apply", "-f", "-"], input=json.dumps(manifest), text=True, check=True)
    kubectl(["wait", "--for=condition=Ready", f"pod/{pod_name}", "--timeout=120s"], namespace=namespace)


def delete_fault_proxy_pod(namespace: str, pod_name: str) -> None:
    delete_pod_if_exists(pod_name, namespace)


def get_first_pod_name(namespace: str, app_label: str) -> str:
    return kubectl(["get", "pod", "-l", f"app={app_label}", "-o", "jsonpath={.items[0].metadata.name}"], namespace=namespace, capture_output=True)


def wait_for_replacement_pod(namespace: str, app_label: str, old_pod: str, timeout_seconds: int = 180) -> str:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        pod_names = kubectl(["get", "pods", "-l", f"app={app_label}", "-o", "jsonpath={.items[*].metadata.name}"], namespace=namespace, capture_output=True)
        for pod_name in pod_names.split():
            if pod_name == old_pod:
                continue
            ready = kubectl(["get", "pod", pod_name, "-o", "jsonpath={.status.containerStatuses[0].ready}"], namespace=namespace, capture_output=True)
            phase = kubectl(["get", "pod", pod_name, "-o", "jsonpath={.status.phase}"], namespace=namespace, capture_output=True)
            if ready == "true" and phase == "Running":
                return pod_name
        time.sleep(3)
    raise TimeoutError(f"Timed out waiting for replacement pod for app={app_label}")


def wait_for_first_ready_pod(namespace: str, app_label: str, timeout_seconds: int = 180) -> str:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        pod_names = kubectl(["get", "pods", "-l", f"app={app_label}", "-o", "jsonpath={.items[*].metadata.name}"], namespace=namespace, capture_output=True)
        for pod_name in pod_names.split():
            ready = kubectl(["get", "pod", pod_name, "-o", "jsonpath={.status.containerStatuses[0].ready}"], namespace=namespace, capture_output=True)
            phase = kubectl(["get", "pod", pod_name, "-o", "jsonpath={.status.phase}"], namespace=namespace, capture_output=True)
            if ready == "true" and phase == "Running":
                return pod_name
        time.sleep(3)
    raise TimeoutError(f"Timed out waiting for ready pod for app={app_label}")


def scale_and_capture_original(namespace: str, deployment_name: str, replicas: int) -> tuple[str, dict, int]:
    deployment_obj = get_deployment(namespace, deployment_name)
    container_name = first_container_name(deployment_obj)
    original_replicas = int(deployment_obj["spec"].get("replicas", 1))
    scale_deployment(namespace, deployment_name, replicas)
    return container_name, deployment_obj, original_replicas


def collect_run(
    row: dict[str, str],
    namespace: str,
    warmup_seconds: int,
    cooldown_seconds: int,
    fault_duration_seconds: int,
    replica_drop_to: int,
) -> dict[str, str]:
    fault_type = (row.get("fault_type") or "none").strip().lower()
    target_service = (row.get("fault_target_service") or "none").strip().lower()
    target_container = target_service if target_service not in ("", "none") else ""
    warmup_seconds = row_int(row, "warmup_seconds", warmup_seconds)
    cooldown_seconds = row_int(row, "cooldown_seconds", cooldown_seconds)
    fault_duration_seconds = row_int(row, "fault_duration_seconds", fault_duration_seconds)
    replica_drop_to = row_int(row, "replica_drop_to", replica_drop_to)
    pod_kill_repeats = max(1, row_int(row, "pod_kill_repeats", 1))
    pod_kill_interval_seconds = max(1, row_int(row, "pod_kill_interval_seconds", max(fault_duration_seconds // max(pod_kill_repeats, 1), 10)))
    latency_delay_seconds = max(1, row_int(row, "latency_delay_seconds", {"low": 1, "medium": 3, "high": 5}.get(severity_level(row), 3)))

    print(f"[collect] run_id={row.get('run_id','')} fault_type={fault_type} target_service={target_service}", flush=True)
    disable_builtin_loadgenerator(namespace)

    if fault_type == "pod-kill" and target_service not in ("", "none"):
        scale_deployment(namespace, target_service, 1)

    print("[collect] starting load generator", flush=True)
    start_loadgen(namespace, row.get("load_profile", "medium"))
    time.sleep(warmup_seconds)
    row["start_time"] = now_iso()
    row["target_container"] = target_container

    if fault_type == "pod-kill" and target_service not in ("", "none"):
        print("[collect] injecting pod-kill", flush=True)
        replacement_pod = ""
        row["fault_start_time"] = now_iso()
        for attempt in range(pod_kill_repeats):
            target_pod = get_first_pod_name(namespace, target_service)
            row["target_pod"] = target_pod
            kubectl(["delete", "pod", target_pod], namespace=namespace)
            replacement_pod = wait_for_replacement_pod(namespace, target_service, target_pod)
            if attempt < pod_kill_repeats - 1:
                time.sleep(pod_kill_interval_seconds)
        row["fault_end_time"] = now_iso()
        row["target_pod"] = replacement_pod
    elif fault_type == "replica-drop" and target_service not in ("", "none"):
        print("[collect] injecting replica-drop", flush=True)
        row["fault_start_time"] = now_iso()
        scale_deployment(namespace, target_service, replica_drop_to)
        time.sleep(fault_duration_seconds)
        scale_deployment(namespace, target_service, 1)
        row["fault_end_time"] = now_iso()
    elif fault_type == "cpu-stress" and target_service not in ("", "none"):
        print("[collect] injecting cpu-stress via low CPU limits", flush=True)
        container_name, deployment_obj, original_replicas = scale_and_capture_original(namespace, target_service, 1)
        original_resources = deployment_obj["spec"]["template"]["spec"]["containers"][0].get("resources", {})
        row["fault_start_time"] = now_iso()
        patch_deployment_container(
            namespace,
            target_service,
            container_name,
            {
                "resources": cpu_stress_resources(row, original_resources)
            },
        )
        row["target_pod"] = wait_for_first_ready_pod(namespace, target_service)
        time.sleep(fault_duration_seconds)
        patch_deployment_container(namespace, target_service, container_name, {"resources": original_resources})
        if original_replicas != 1:
            scale_deployment(namespace, target_service, original_replicas)
        row["fault_end_time"] = now_iso()
    elif fault_type == "memory-stress" and target_service not in ("", "none"):
        print("[collect] injecting memory-stress via low memory limits", flush=True)
        container_name, deployment_obj, original_replicas = scale_and_capture_original(namespace, target_service, 1)
        original_resources = deployment_obj["spec"]["template"]["spec"]["containers"][0].get("resources", {})
        row["fault_start_time"] = now_iso()
        patch_deployment_container(
            namespace,
            target_service,
            container_name,
            {
                "resources": memory_stress_resources(row, original_resources)
            },
        )
        row["target_pod"] = wait_for_first_ready_pod(namespace, target_service)
        time.sleep(fault_duration_seconds)
        patch_deployment_container(namespace, target_service, container_name, {"resources": original_resources})
        if original_replicas != 1:
            scale_deployment(namespace, target_service, original_replicas)
        row["fault_end_time"] = now_iso()
    elif fault_type == "latency-injection" and target_service not in ("", "none"):
        print("[collect] injecting latency via delay proxy service selector swap", flush=True)
        service_obj = get_service(namespace, target_service)
        original_selector = service_obj["spec"].get("selector", {})
        proxy_pod_name = f"{target_service}-delay-proxy"
        proxy_label = f"{target_service}-delay"
        create_fault_proxy_pod(namespace, proxy_pod_name, proxy_label, 50051, "delay-close", delay_seconds=latency_delay_seconds)
        row["fault_start_time"] = now_iso()
        row["target_pod"] = proxy_pod_name
        patch_service_selector(namespace, target_service, {"fault-proxy": proxy_label})
        time.sleep(fault_duration_seconds)
        patch_service_selector(namespace, target_service, original_selector)
        delete_fault_proxy_pod(namespace, proxy_pod_name)
        row["fault_end_time"] = now_iso()
    elif fault_type == "timeout" and target_service not in ("", "none"):
        print("[collect] injecting timeout via blackhole proxy service selector swap", flush=True)
        service_obj = get_service(namespace, target_service)
        original_selector = service_obj["spec"].get("selector", {})
        port = 7000 if target_service == "currencyservice" else 50051
        proxy_pod_name = f"{target_service}-timeout-proxy"
        proxy_label = f"{target_service}-timeout"
        create_fault_proxy_pod(namespace, proxy_pod_name, proxy_label, port, "hang")
        row["fault_start_time"] = now_iso()
        row["target_pod"] = proxy_pod_name
        patch_service_selector(namespace, target_service, {"fault-proxy": proxy_label})
        time.sleep(fault_duration_seconds)
        patch_service_selector(namespace, target_service, original_selector)
        delete_fault_proxy_pod(namespace, proxy_pod_name)
        row["fault_end_time"] = now_iso()
    elif fault_type == "http-500" and target_service == "frontend":
        print("[collect] injecting http-500 via frontend misconfiguration rollout", flush=True)
        deployment_obj = get_deployment(namespace, target_service)
        container_name = first_container_name(deployment_obj)
        original_env = deployment_obj["spec"]["template"]["spec"]["containers"][0].get("env", [])
        row["fault_start_time"] = now_iso()
        kubectl(["set", "env", f"deployment/{target_service}", "CURRENCY_SERVICE_ADDR=currencyservice-bad:7000"], namespace=namespace)
        kubectl(["rollout", "status", f"deployment/{target_service}", "--timeout=180s"], namespace=namespace)
        row["target_pod"] = wait_for_first_ready_pod(namespace, target_service)
        time.sleep(fault_duration_seconds)
        patch_deployment_container(namespace, target_service, container_name, {"env": original_env})
        row["fault_end_time"] = now_iso()
    else:
        row["fault_start_time"] = ""
        row["fault_end_time"] = ""

    time.sleep(cooldown_seconds)
    row["end_time"] = now_iso()
    print("[collect] stopping load generator", flush=True)
    stop_loadgen(namespace)
    return row


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Semi-automate one data-collection run on a local cluster.")
    parser.add_argument("--metadata-file", type=Path, default=DEFAULT_METADATA_FILE)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--warmup-seconds", type=int, default=60)
    parser.add_argument("--cooldown-seconds", type=int, default=45)
    parser.add_argument("--fault-duration-seconds", type=int, default=60)
    parser.add_argument("--replica-drop-to", type=int, default=0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    header, rows = load_rows(args.metadata_file)
    target_row = None
    for row in rows:
        if row.get("run_id") == args.run_id:
            target_row = row
            break

    if target_row is None:
        raise ValueError(f"Run id not found in metadata file: {args.run_id}")

    updated_row = collect_run(
        row=target_row,
        namespace=args.namespace,
        warmup_seconds=args.warmup_seconds,
        cooldown_seconds=args.cooldown_seconds,
        fault_duration_seconds=args.fault_duration_seconds,
        replica_drop_to=args.replica_drop_to,
    )

    for idx, row in enumerate(rows):
        if row.get("run_id") == args.run_id:
            rows[idx] = updated_row
            break
    save_rows(args.metadata_file, header, rows)

    print(f"Updated run metadata for {args.run_id}")
    print(f"metadata_file={args.metadata_file}")
    print(f"start_time={updated_row.get('start_time', '')}")
    print(f"fault_start_time={updated_row.get('fault_start_time', '')}")
    print(f"fault_end_time={updated_row.get('fault_end_time', '')}")
    print(f"end_time={updated_row.get('end_time', '')}")
    print("Next step: export Jaeger traces to the trace_file listed in the metadata row.")


if __name__ == "__main__":
    main()
