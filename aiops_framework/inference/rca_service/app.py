from __future__ import annotations

import os

import torch
from fastapi import FastAPI

from .model_loader import DEFAULT_ARTIFACT_DIR, load_artifacts
from .predictor import predict_graph
from .schemas import GraphPredictRequest, GraphPredictResponse, RankedNode


device_name = "cuda" if torch.cuda.is_available() and os.environ.get("AIOPS_RCA_DEVICE", "cpu") == "cuda" else "cpu"
ARTIFACTS = load_artifacts(DEFAULT_ARTIFACT_DIR, device=device_name)
app = FastAPI(title="AIOps RCA Inference Service", version="0.1.0")


@app.get("/health")
def health() -> dict[str, object]:
    return {
        "status": "ok",
        "artifact_dir": str(ARTIFACTS.artifact_dir),
        "model_name": ARTIFACTS.inference_config.get("model_name", ARTIFACTS.artifact_dir.name),
        "model_type": ARTIFACTS.model_type,
        "device": str(ARTIFACTS.device),
        "feature_count": len(ARTIFACTS.feature_cols),
    }


@app.get("/metadata")
def metadata() -> dict[str, object]:
    return {
        "artifact_dir": str(ARTIFACTS.artifact_dir),
        "model_config": ARTIFACTS.model_config,
        "inference_config": ARTIFACTS.inference_config,
        "model_type": ARTIFACTS.model_type,
        "feature_count": len(ARTIFACTS.feature_cols),
        "feature_cols": ARTIFACTS.feature_cols,
    }


@app.post("/predict/graph", response_model=GraphPredictResponse)
def predict(payload: GraphPredictRequest) -> GraphPredictResponse:
    result = predict_graph(
        artifacts=ARTIFACTS,
        x_rows=payload.x,
        edge_index_rows=payload.edge_index,
        node_names=payload.node_names,
        top_k=payload.top_k,
        graph_id=payload.graph_id,
        metadata=payload.metadata,
    )
    return GraphPredictResponse(
        graph_id=result["graph_id"],
        top1=RankedNode(**result["top1"]),
        topk=[RankedNode(**item) for item in result["topk"]],
        model_name=result["model_name"],
        model_type=result["model_type"],
        metadata=result["metadata"],
    )
