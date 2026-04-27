from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def framework_root() -> Path:
    return Path(__file__).resolve().parents[1]


def normalize_system_dir(system_id: str) -> str:
    return str(system_id).strip().replace("-", "_").replace(" ", "_").lower()


def system_root(system_id: str) -> Path:
    system_dir = normalize_system_dir(system_id)
    candidates = [
        framework_root() / "systems" / system_dir,
        framework_root() / "system" / system_dir,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def _resolve_path(base_dir: Path, value: Any) -> Path | Any:
    if not isinstance(value, str) or not value.strip():
        return value
    path_value = Path(value)
    if path_value.is_absolute():
        return path_value
    return (base_dir / path_value).resolve()


def load_system_config(system_id: str) -> dict[str, Any]:
    root = system_root(system_id)
    config_path = root / "system.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Missing system config: {config_path}")

    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"System config must be a mapping: {config_path}")

    payload["system_id"] = str(payload.get("system_id") or system_id).strip()
    payload["system_root"] = root
    payload["framework_root"] = framework_root()
    payload["repo_root"] = repo_root()

    if "service_catalog" in payload:
        payload["service_catalog_path"] = _resolve_path(root, payload["service_catalog"])

    default_artifacts = payload.get("default_artifacts", {})
    if isinstance(default_artifacts, dict):
        payload["default_artifacts"] = {
            key: _resolve_path(root, value)
            for key, value in default_artifacts.items()
        }

    return payload
