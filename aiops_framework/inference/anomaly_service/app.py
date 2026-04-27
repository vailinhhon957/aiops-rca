from __future__ import annotations

from fastapi import FastAPI

from .model_loader import DEFAULT_ARTIFACT_DIR, load_artifacts
from .predictor import build_prediction_payload
from .schemas import BatchPredictRequest, BatchPredictResponse, WindowPredictRequest, WindowPredictResponse


app = FastAPI(title="AIOps Anomaly Inference Service", version="0.1.0")
ARTIFACTS = load_artifacts(DEFAULT_ARTIFACT_DIR)


@app.get("/health")
def health() -> dict[str, object]:
    return {
        "status": "ok",
        "artifact_dir": str(ARTIFACTS.artifact_dir),
        "model_name": ARTIFACTS.inference_config.get("model_name", ARTIFACTS.artifact_dir.name),
        "model_kind": ARTIFACTS.inference_config.get("model_kind", ARTIFACTS.model_kind),
    }


@app.get("/metadata")
def metadata() -> dict[str, object]:
    return {
        "artifact_dir": str(ARTIFACTS.artifact_dir),
        "feature_columns": ARTIFACTS.feature_columns,
        "inference_config": ARTIFACTS.inference_config,
    }


@app.post("/predict/window", response_model=WindowPredictResponse)
def predict_window(payload: WindowPredictRequest) -> WindowPredictResponse:
    result = build_prediction_payload(ARTIFACTS, [payload.features], [payload.metadata])[0]
    return WindowPredictResponse(**result)


@app.post("/predict/batch", response_model=BatchPredictResponse)
def predict_batch(payload: BatchPredictRequest) -> BatchPredictResponse:
    rows = [item.features for item in payload.items]
    metadatas = [item.metadata for item in payload.items]
    results = build_prediction_payload(ARTIFACTS, rows, metadatas)
    return BatchPredictResponse(predictions=[WindowPredictResponse(**result) for result in results])
