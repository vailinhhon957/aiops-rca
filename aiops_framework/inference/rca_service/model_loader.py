from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

import torch

from aiops_framework.inference.common.artifact_registry import DEFAULT_STAGE, resolve_artifact_dir

from .model_def import SimpleGraphAttention


DEFAULT_MODEL_ROOT = Path(
    os.environ.get(
        "AIOPS_RCA_MODEL_ROOT",
        r"D:\HOCTAP\2025-2026\HK2\DACN\microservices-demo\data_rca_balanced_v3\models",
    )
)
DEFAULT_MODEL_STAGE = os.environ.get("AIOPS_RCA_MODEL_STAGE", DEFAULT_STAGE).strip() or DEFAULT_STAGE
DEFAULT_ARTIFACT_DIR = Path(
    os.environ.get(
        "AIOPS_RCA_ARTIFACT_DIR",
        str(resolve_artifact_dir(DEFAULT_MODEL_ROOT, DEFAULT_MODEL_STAGE)),
    )
)


@dataclass
class LoadedRcaArtifacts:
    artifact_dir: Path
    inference_config: dict[str, object]
    model_config: dict[str, object]
    model: SimpleGraphAttention
    device: torch.device


def load_artifacts(artifact_dir: Path | None = None, device: str = "cpu") -> LoadedRcaArtifacts:
    artifact_dir = Path(artifact_dir or DEFAULT_ARTIFACT_DIR)
    inference_config = json.loads((artifact_dir / "inference_config.json").read_text(encoding="utf-8"))
    model_config = json.loads((artifact_dir / "model_config.json").read_text(encoding="utf-8"))
    resolved_device = torch.device(device)
    model = SimpleGraphAttention(
        in_dim=int(model_config["in_dim"]),
        hidden_dim=int(model_config["hidden_dim"]),
        dropout=float(model_config["dropout"]),
    )
    state_dict = torch.load(artifact_dir / str(model_config["state_dict_artifact"]), map_location="cpu")
    model.load_state_dict(state_dict)
    model.to(resolved_device)
    model.eval()
    return LoadedRcaArtifacts(
        artifact_dir=artifact_dir,
        inference_config=inference_config,
        model_config=model_config,
        model=model,
        device=resolved_device,
    )
