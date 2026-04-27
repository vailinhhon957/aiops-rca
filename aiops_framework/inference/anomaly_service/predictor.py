from __future__ import annotations

from typing import Any

import numpy as np

from .model_loader import LoadedAnomalyArtifacts, transform_features


def predict_scores(artifacts: LoadedAnomalyArtifacts, rows: list[dict[str, float]]) -> np.ndarray:
    transformed = transform_features(artifacts, rows)
    if artifacts.model_kind == "ensemble":
        member_probs = [model.predict_proba(transformed)[:, 1] for _, model in artifacts.ensemble_members]
        return np.mean(np.vstack(member_probs), axis=0)
    if artifacts.single_model is None:
        raise RuntimeError("Single-model anomaly artifacts were not loaded.")
    return artifacts.single_model.predict_proba(transformed)[:, 1]


def build_prediction_payload(
    artifacts: LoadedAnomalyArtifacts,
    rows: list[dict[str, float]],
    metadatas: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    scores = predict_scores(artifacts, rows)
    threshold = float(artifacts.inference_config["threshold"])
    metadatas = metadatas or [{} for _ in rows]
    results = []
    for score, metadata in zip(scores, metadatas):
        score_value = float(score)
        results.append(
            {
                "anomaly_score": score_value,
                "threshold": threshold,
                "is_anomaly": bool(score_value >= threshold),
                "model_name": str(artifacts.inference_config.get("model_name", artifacts.artifact_dir.name)),
                "model_kind": str(artifacts.inference_config.get("model_kind", artifacts.model_kind)),
                "optimize_for": str(artifacts.inference_config.get("optimize_for", "anomaly")),
                "metadata": metadata or {},
            }
        )
    return results
