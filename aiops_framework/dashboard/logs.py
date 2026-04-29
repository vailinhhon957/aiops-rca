from __future__ import annotations

import re
import subprocess
from typing import Any

from aiops_framework.core.config import load_system_config


IMPORTANT_LOG_RE = re.compile(r"(error|warn|exception|failed|failure|timeout|traceback)", re.IGNORECASE)


def fetch_recent_logs(system_id: str, service_name: str, tail: int = 200, since: str = "10m") -> dict[str, Any]:
    cfg = load_system_config(system_id)
    namespace = str(cfg.get("namespace") or "default")
    service = str(service_name or "").strip()
    if not service:
        raise ValueError("service_name is required")

    tail = max(20, min(int(tail), 1000))
    cmd = [
        "kubectl",
        "-n",
        namespace,
        "logs",
        f"deployment/{service}",
        f"--tail={tail}",
        f"--since={since}",
        "--all-containers=true",
    ]
    result = subprocess.run(cmd, check=True, text=True, capture_output=True)
    lines = result.stdout.splitlines()
    important_lines = [line for line in lines if IMPORTANT_LOG_RE.search(line)]
    return {
        "system_id": system_id,
        "namespace": namespace,
        "service_name": service,
        "tail": tail,
        "since": since,
        "line_count": len(lines),
        "important_count": len(important_lines),
        "important_lines": important_lines[-80:],
        "raw_tail": lines[-80:],
        "provider": "kubectl",
    }
