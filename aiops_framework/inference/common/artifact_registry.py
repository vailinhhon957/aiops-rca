from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_STAGE = "production"
REGISTRY_FILENAME = "model_registry.json"


def registry_path(models_root: Path) -> Path:
    return Path(models_root) / REGISTRY_FILENAME


def load_registry(models_root: Path, task: str | None = None) -> dict[str, Any]:
    path = registry_path(models_root)
    if not path.exists():
        return {
            "schema_version": "1.0",
            "task": task or "unknown",
            "stages": {},
        }
    payload = json.loads(path.read_text(encoding="utf-8")) or {}
    if "stages" not in payload or not isinstance(payload["stages"], dict):
        payload["stages"] = {}
    if task and not payload.get("task"):
        payload["task"] = task
    return payload


def write_registry(models_root: Path, payload: dict[str, Any]) -> Path:
    path = registry_path(models_root)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def set_stage(models_root: Path, task: str, stage: str, model_name: str, notes: str | None = None) -> Path:
    payload = load_registry(models_root, task=task)
    payload["schema_version"] = "1.0"
    payload["task"] = task
    payload["stages"][stage] = {
        "model_name": model_name,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "notes": notes or "",
    }
    return write_registry(models_root, payload)


def promote_stage(
    models_root: Path,
    task: str,
    *,
    source_stage: str = "candidate",
    target_stage: str = "production",
    previous_stage: str = "previous",
) -> Path:
    payload = load_registry(models_root, task=task)
    stages = payload.setdefault("stages", {})
    source = stages.get(source_stage)
    if not source or not source.get("model_name"):
        raise ValueError(f"Stage '{source_stage}' is not defined in {registry_path(models_root)}")

    current_target = stages.get(target_stage)
    if current_target and current_target.get("model_name"):
        stages[previous_stage] = {
            "model_name": current_target["model_name"],
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "notes": f"Auto-copied from {target_stage} during promotion.",
        }

    stages[target_stage] = {
        "model_name": source["model_name"],
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "notes": f"Promoted from {source_stage}.",
    }
    payload["schema_version"] = "1.0"
    payload["task"] = task
    return write_registry(models_root, payload)


def resolve_artifact_dir(models_root: Path, stage: str = DEFAULT_STAGE) -> Path:
    models_root = Path(models_root)
    registry = load_registry(models_root)
    stage_entry = registry.get("stages", {}).get(stage, {})
    model_name = stage_entry.get("model_name")
    if model_name:
        artifact_dir = models_root / str(model_name)
        if artifact_dir.exists():
            return artifact_dir
        raise FileNotFoundError(f"Stage '{stage}' points to missing model directory: {artifact_dir}")

    fallback_dir = models_root / stage
    if fallback_dir.exists():
        return fallback_dir

    raise FileNotFoundError(f"Unable to resolve artifact dir for stage '{stage}' under {models_root}")
