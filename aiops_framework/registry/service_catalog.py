from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from aiops_framework.core.config import load_system_config


DEFAULT_SERVICE_METADATA = {
    "service_role": "unknown",
    "service_tier": "unknown",
    "criticality": "unknown",
    "is_entrypoint": 0,
    "is_stateful": 0,
}


def _normalize_service_name(service_name: str) -> str:
    return str(service_name or "unknown").strip().lower()


def _read_catalog_json(path: Path) -> list[dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Service catalog must be a JSON array: {path}")
    return payload


def load_service_catalog(system_id: str) -> pd.DataFrame:
    system_cfg = load_system_config(system_id)
    catalog_path = system_cfg.get("service_catalog_path")
    if not catalog_path:
        raise KeyError(f"System '{system_id}' does not define service_catalog in system.yaml")

    catalog_df = pd.DataFrame(_read_catalog_json(Path(catalog_path)))
    if catalog_df.empty:
        return pd.DataFrame(columns=["service_name", *DEFAULT_SERVICE_METADATA.keys()])

    catalog_df["service_name"] = catalog_df["service_name"].astype(str).str.lower().str.strip()
    for key, default_value in DEFAULT_SERVICE_METADATA.items():
        if key not in catalog_df.columns:
            catalog_df[key] = default_value
        catalog_df[key] = catalog_df[key].fillna(default_value)
    return catalog_df


def service_lookup(catalog_df: pd.DataFrame) -> dict[str, dict[str, object]]:
    if catalog_df.empty:
        return {}
    return {
        _normalize_service_name(row["service_name"]): {
            "service_role": row["service_role"],
            "service_tier": row["service_tier"],
            "criticality": row["criticality"],
            "is_entrypoint": int(row["is_entrypoint"]),
            "is_stateful": int(row["is_stateful"]),
        }
        for row in catalog_df.to_dict(orient="records")
    }


def map_service_metadata(service_name: str, lookup: dict[str, dict[str, object]]) -> dict[str, object]:
    normalized_name = _normalize_service_name(service_name)
    metadata = lookup.get(normalized_name, DEFAULT_SERVICE_METADATA)
    return {
        "service_name": normalized_name,
        "service_role": metadata["service_role"],
        "service_tier": metadata["service_tier"],
        "criticality": metadata["criticality"],
        "is_entrypoint": int(metadata["is_entrypoint"]),
        "is_stateful": int(metadata["is_stateful"]),
    }
