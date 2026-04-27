from __future__ import annotations

import os
import shlex
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone


DEFAULT_ALLOWED_ACTIONS = {
    "restart_pod",
    "scale_service",
    "rollback_deployment",
}

DEFAULT_ALLOWED_SERVICES = {
    "frontend",
    "checkoutservice",
    "cartservice",
    "productcatalogservice",
    "currencyservice",
    "paymentservice",
    "emailservice",
    "recommendationservice",
    "shippingservice",
    "adservice",
}


def _split_csv_env(name: str, default_values: set[str]) -> set[str]:
    raw = os.environ.get(name, "")
    if not raw.strip():
        return set(default_values)
    return {item.strip() for item in raw.split(",") if item.strip()}


@dataclass
class ExecutionResult:
    status: str
    action: str
    service: str
    namespace: str
    command: list[str]
    dry_run: bool
    returncode: int | None
    stdout: str
    stderr: str
    executed_at: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def build_command(service: str, action: str) -> list[str]:
    kubectl_bin = os.environ.get("AIOPS_KUBECTL_BIN", "kubectl")
    namespace = os.environ.get("AIOPS_K8S_NAMESPACE", "default")
    scale_replicas = os.environ.get("AIOPS_SCALE_REPLICAS", "2")
    deployment = f"deployment/{service}"

    if action == "restart_pod":
        return [kubectl_bin, "-n", namespace, "rollout", "restart", deployment]
    if action == "scale_service":
        return [kubectl_bin, "-n", namespace, "scale", deployment, f"--replicas={scale_replicas}"]
    if action == "rollback_deployment":
        return [kubectl_bin, "-n", namespace, "rollout", "undo", deployment]
    raise ValueError(f"Unsupported action: {action}")


def execute_action(service: str, action: str, dry_run: bool | None = None) -> ExecutionResult:
    allowed_actions = _split_csv_env("AIOPS_ALLOWED_ACTIONS", DEFAULT_ALLOWED_ACTIONS)
    allowed_services = _split_csv_env("AIOPS_ALLOWED_SERVICES", DEFAULT_ALLOWED_SERVICES)
    namespace = os.environ.get("AIOPS_K8S_NAMESPACE", "default")
    resolved_dry_run = (
        os.environ.get("AIOPS_EXECUTOR_DRY_RUN", "false").lower() == "true"
        if dry_run is None
        else dry_run
    )

    if action not in allowed_actions:
        raise ValueError(f"Action '{action}' is not in the allowed action list.")
    if service not in allowed_services:
        raise ValueError(f"Service '{service}' is not in the allowed service list.")

    command = build_command(service=service, action=action)
    executed_at = datetime.now(timezone.utc).isoformat()

    if resolved_dry_run:
        return ExecutionResult(
            status="dry_run",
            action=action,
            service=service,
            namespace=namespace,
            command=command,
            dry_run=True,
            returncode=None,
            stdout=f"Dry run only. Command not executed: {shlex.join(command)}",
            stderr="",
            executed_at=executed_at,
        )

    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
        timeout=int(os.environ.get("AIOPS_EXECUTOR_TIMEOUT_SEC", "60")),
    )

    return ExecutionResult(
        status="success" if completed.returncode == 0 else "failed",
        action=action,
        service=service,
        namespace=namespace,
        command=command,
        dry_run=False,
        returncode=completed.returncode,
        stdout=completed.stdout.strip(),
        stderr=completed.stderr.strip(),
        executed_at=executed_at,
    )
