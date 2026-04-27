from __future__ import annotations

import json
import platform
import subprocess
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def collect_package_versions(packages: list[str]) -> dict[str, str | None]:
    results: dict[str, str | None] = {"python": platform.python_version()}
    for package_name in packages:
        try:
            results[package_name] = version(package_name)
        except PackageNotFoundError:
            results[package_name] = None
    return results


def collect_git_metadata(repo_root: Path) -> dict[str, Any]:
    try:
        commit = subprocess.check_output(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        branch = subprocess.check_output(
            ["git", "-C", str(repo_root), "rev-parse", "--abbrev-ref", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        dirty = bool(
            subprocess.check_output(
                ["git", "-C", str(repo_root), "status", "--porcelain"],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
        )
        return {"commit": commit, "branch": branch, "dirty": dirty}
    except Exception:
        return {"commit": None, "branch": None, "dirty": None}


def build_run_manifest(
    *,
    repo_root: Path,
    task: str,
    output_dir: Path,
    train_script: Path,
    data_root: Path,
    args: dict[str, Any],
    artifacts: dict[str, Any],
    dataset: dict[str, Any],
    package_versions: dict[str, str | None],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "schema_version": "1.0",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "task": task,
        "model_name": output_dir.name,
        "output_dir": str(output_dir),
        "train_script": str(train_script),
        "data_root": str(data_root),
        "git": collect_git_metadata(repo_root),
        "args": _json_safe(args),
        "dataset": _json_safe(dataset),
        "artifacts": _json_safe(artifacts),
        "package_versions": _json_safe(package_versions),
    }
    if extra:
        payload["extra"] = _json_safe(extra)
    return payload


def write_run_manifest(output_dir: Path, payload: dict[str, Any]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "run_manifest.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path
