from __future__ import annotations

from pathlib import Path
from typing import Any

from aiops_framework.core.config import framework_root, load_system_config
from aiops_framework.registry.service_catalog import load_service_catalog


def _system_config_paths() -> list[Path]:
    roots = [framework_root() / "systems", framework_root() / "system"]
    paths: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        if not root.exists():
            continue
        for path in sorted(root.glob("*/system.yaml")):
            resolved = path.resolve()
            if resolved not in seen:
                paths.append(path)
                seen.add(resolved)
    return paths


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    return value


def list_system_ids() -> list[str]:
    ids: list[str] = []
    for path in _system_config_paths():
        cfg = load_system_config(path.parent.name)
        ids.append(str(cfg.get("system_id") or path.parent.name))
    return sorted(set(ids))


def get_system(system_id: str) -> dict[str, Any]:
    cfg = load_system_config(system_id)
    catalog_df = load_service_catalog(str(cfg["system_id"]))
    service_names = catalog_df["service_name"].astype(str).tolist() if not catalog_df.empty else []
    payload = {
        "system_id": cfg["system_id"],
        "display_name": cfg.get("display_name", cfg["system_id"]),
        "system_family": cfg.get("system_family", "unknown"),
        "namespace": cfg.get("namespace", "default"),
        "entry_services": cfg.get("entry_services", []),
        "jaeger_services": cfg.get("jaeger_services", cfg.get("entry_services", [])),
        "prometheus_labels": cfg.get("prometheus_labels", {}),
        "model_profile": cfg.get("model_profile", {}),
        "service_count": len(service_names),
        "services": service_names,
        "trace_adapter": cfg.get("trace_adapter", "jaeger"),
        "metric_adapter": cfg.get("metric_adapter", "prometheus"),
        "log_adapter": cfg.get("log_adapter", "kubectl"),
    }
    return _to_jsonable(payload)


def list_systems() -> list[dict[str, Any]]:
    return [get_system(system_id) for system_id in list_system_ids()]
