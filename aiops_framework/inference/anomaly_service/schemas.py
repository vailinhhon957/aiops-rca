from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class WindowPredictRequest(BaseModel):
    features: dict[str, float] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class BatchPredictRequest(BaseModel):
    items: list[WindowPredictRequest] = Field(default_factory=list)


class WindowPredictResponse(BaseModel):
    anomaly_score: float
    threshold: float
    is_anomaly: bool
    model_name: str
    model_kind: str
    optimize_for: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class BatchPredictResponse(BaseModel):
    predictions: list[WindowPredictResponse] = Field(default_factory=list)
