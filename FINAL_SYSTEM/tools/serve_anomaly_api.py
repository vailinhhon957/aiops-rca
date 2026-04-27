import argparse
import json
import pickle
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


class TransformerAutoencoder(nn.Module):
    def __init__(
        self,
        input_dim,
        d_model=64,
        nhead=4,
        num_layers=2,
        seq_len=10,
        latent_dim=24,
        dropout=0.15,
    ):
        super().__init__()
        self.seq_len = seq_len
        self.d_model = d_model
        self.embedding = nn.Linear(input_dim, d_model)
        self.pos_encoding = nn.Embedding(seq_len, d_model)

        enc_layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=nhead,
            dim_feedforward=d_model * 4,
            dropout=dropout,
            batch_first=True,
        )
        self.encoder = nn.TransformerEncoder(enc_layer, num_layers=num_layers)

        self.to_latent = nn.Sequential(
            nn.Linear(d_model * seq_len, d_model),
            nn.ReLU(),
            nn.Linear(d_model, latent_dim),
        )
        self.from_latent = nn.Sequential(
            nn.Linear(latent_dim, d_model),
            nn.ReLU(),
            nn.Linear(d_model, d_model * seq_len),
        )

        dec_layer = nn.TransformerDecoderLayer(
            d_model=d_model,
            nhead=nhead,
            dim_feedforward=d_model * 4,
            dropout=dropout,
            batch_first=True,
        )
        self.decoder = nn.TransformerDecoder(dec_layer, num_layers=num_layers)
        self.output_layer = nn.Linear(d_model, input_dim)

    def forward(self, x):
        batch_size, steps, _ = x.shape
        pos = torch.arange(steps, device=x.device).unsqueeze(0).expand(batch_size, -1)
        x_emb = self.embedding(x) + self.pos_encoding(pos)
        encoded = self.encoder(x_emb)
        latent = self.to_latent(encoded.reshape(batch_size, -1))
        decoded_seed = self.from_latent(latent).reshape(batch_size, steps, self.d_model)
        decoded = self.decoder(decoded_seed, encoded)
        return self.output_layer(decoded)


class TraceRecord(BaseModel):
    trace_id: str
    timestamp: Optional[str] = None
    source_file: Optional[str] = None
    row_order: Optional[int] = None
    span_count: float
    service_count: float
    app_service_count: float
    avg_latency: float
    max_latency: float
    std_latency: float
    trace_latency: float
    error_rate: float
    http_5xx_rate: float
    depth: float
    latency_zscore: float = 0.0
    duration_ratio: float = 0.0
    # Per-service features
    adservice_avg_latency: float = 0.0
    adservice_error_rate: float = 0.0
    cartservice_avg_latency: float = 0.0
    cartservice_error_rate: float = 0.0
    checkoutservice_avg_latency: float = 0.0
    checkoutservice_error_rate: float = 0.0
    currencyservice_avg_latency: float = 0.0
    currencyservice_error_rate: float = 0.0
    emailservice_avg_latency: float = 0.0
    emailservice_error_rate: float = 0.0
    frontend_avg_latency: float = 0.0
    frontend_error_rate: float = 0.0
    paymentservice_avg_latency: float = 0.0
    paymentservice_error_rate: float = 0.0
    productcatalogservice_avg_latency: float = 0.0
    productcatalogservice_error_rate: float = 0.0
    recommendationservice_avg_latency: float = 0.0
    recommendationservice_error_rate: float = 0.0
    shippingservice_avg_latency: float = 0.0
    shippingservice_error_rate: float = 0.0


class DetectRequest(BaseModel):
    traces: list[TraceRecord] = Field(..., min_length=1)
    group_id: str = Field(default="api_batch")


class BaseDetector:
    model_name: str
    threshold: float

    def health(self):
        return {
            "status": "ok",
            "model": self.model_name,
            "threshold": self.threshold,
        }


class AnomalyDetector:
    def __init__(self, artifact_dir: Path):
        self.artifact_dir = artifact_dir
        with (artifact_dir / "transformer_ae_metrics.json").open("r", encoding="utf-8") as f:
            self.metrics = json.load(f)
        with (artifact_dir / "transformer_ae_scaler.pkl").open("rb") as f:
            self.scaler = pickle.load(f)

        self.features = self.metrics["features"]
        self.log_features = self.metrics["log_features"]
        self.seq_len = int(self.metrics["seq_len"])
        self.threshold = float(self.metrics["threshold"])
        self.tail_weight = float(self.metrics.get("tail_weight", 2.0))
        self.model_name = self.metrics.get("model_name", "anomaly_final")
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        self.model = TransformerAutoencoder(
            input_dim=len(self.features),
            seq_len=self.seq_len,
        ).to(self.device)
        state = torch.load(artifact_dir / "transformer_ae.pt", map_location=self.device)
        self.model.load_state_dict(state)
        self.model.eval()

    def _prepare_dataframe(self, request: DetectRequest) -> pd.DataFrame:
        rows = []
        for idx, trace in enumerate(request.traces):
            row = trace.model_dump()
            row["source_file"] = row.get("source_file") or request.group_id
            row["row_order"] = idx if row.get("row_order") is None else row["row_order"]
            rows.append(row)

        df = pd.DataFrame(rows)
        if len(df) < self.seq_len:
            raise HTTPException(
                status_code=400,
                detail=f"Can it nhat {self.seq_len} traces de tao 1 sequence.",
            )

        missing_cols = [col for col in self.features if col not in df.columns]
        if missing_cols:
            raise HTTPException(
                status_code=400,
                detail=f"Thieu feature: {', '.join(missing_cols)}",
            )

        for col in self.log_features:
            df[col] = np.log1p(df[col].clip(lower=0))

        df.loc[:, self.features] = self.scaler.transform(df[self.features])
        return df.sort_values(["source_file", "row_order"]).reset_index(drop=True)

    def _make_sequences(self, df: pd.DataFrame):
        sequences = []
        last_rows = []
        for _, group in df.groupby("source_file"):
            group = group.sort_values("row_order").reset_index(drop=True)
            if len(group) < self.seq_len:
                continue
            values = group[self.features].values.astype(np.float32)
            for start in range(len(group) - self.seq_len + 1):
                end = start + self.seq_len
                sequences.append(values[start:end])
                last_rows.append(group.iloc[end - 1].to_dict())

        if not sequences:
            raise HTTPException(status_code=400, detail="Khong tao duoc sequence hop le tu input.")

        return np.asarray(sequences, dtype=np.float32), last_rows

    def _reconstruction_errors(self, inputs: np.ndarray) -> np.ndarray:
        with torch.no_grad():
            tensor = torch.tensor(inputs, dtype=torch.float32, device=self.device)
            preds = self.model(tensor)
            steps = tensor.shape[1]
            weights = torch.linspace(1.0, self.tail_weight, steps, device=self.device)
            weights = weights / weights.sum()
            per_step = ((preds - tensor) ** 2).mean(dim=2)
            err = (per_step * weights.unsqueeze(0)).sum(dim=1).cpu().numpy()
        return err

    def detect(self, request: DetectRequest):
        df = self._prepare_dataframe(request)
        sequences, last_rows = self._make_sequences(df)
        errors = self._reconstruction_errors(sequences)

        predictions = []
        anomaly_count = 0
        for score, row in zip(errors, last_rows):
            is_anomaly = bool(score > self.threshold)
            anomaly_count += int(is_anomaly)
            predictions.append(
                {
                    "trace_id": row.get("trace_id"),
                    "timestamp": row.get("timestamp"),
                    "source_file": row.get("source_file"),
                    "row_order": int(row.get("row_order", 0)),
                    "anomaly_score": float(score),
                    "threshold": self.threshold,
                    "is_anomaly": is_anomaly,
                }
            )

        return {
            "model": self.model_name,
            "seq_len": self.seq_len,
            "threshold": self.threshold,
            "total_windows": len(predictions),
            "anomaly_windows": anomaly_count,
            "predictions": predictions,
        }


class SupervisedAnomalyDetector(BaseDetector):
    def __init__(self, artifact_dir: Path):
        self.artifact_dir = artifact_dir
        with (artifact_dir / "supervised_anomaly_metrics.json").open("r", encoding="utf-8") as f:
            self.metrics = json.load(f)
        with (artifact_dir / "supervised_anomaly_model.pkl").open("rb") as f:
            self.model = pickle.load(f)

        self.model_name = self.metrics.get("model_name", "anomaly_final")
        self.features = self.metrics["features"]
        self.threshold = float(self.metrics["threshold"])
        self.seq_len = 1

    def _prepare_dataframe(self, request: DetectRequest) -> pd.DataFrame:
        rows = []
        for idx, trace in enumerate(request.traces):
            row = trace.model_dump()
            row["source_file"] = row.get("source_file") or request.group_id
            row["row_order"] = idx if row.get("row_order") is None else row["row_order"]
            rows.append(row)

        df = pd.DataFrame(rows)
        for feature in self.features:
            if feature not in df.columns:
                df[feature] = 0.0
        return df.sort_values(["source_file", "row_order"]).reset_index(drop=True)

    def detect(self, request: DetectRequest):
        df = self._prepare_dataframe(request)
        scores = self.model.predict_proba(df[self.features])[:, 1]

        predictions = []
        anomaly_count = 0
        for score, (_, row) in zip(scores, df.iterrows()):
            is_anomaly = bool(score >= self.threshold)
            anomaly_count += int(is_anomaly)
            predictions.append(
                {
                    "trace_id": row.get("trace_id"),
                    "timestamp": row.get("timestamp"),
                    "source_file": row.get("source_file"),
                    "row_order": int(row.get("row_order", 0)),
                    "anomaly_score": float(score),
                    "threshold": self.threshold,
                    "is_anomaly": is_anomaly,
                }
            )

        return {
            "model": self.model_name,
            "seq_len": self.seq_len,
            "threshold": self.threshold,
            "total_windows": len(predictions),
            "anomaly_windows": anomaly_count,
            "predictions": predictions,
        }


def create_app(artifact_dir: Path) -> FastAPI:
    if (artifact_dir / "supervised_anomaly_model.pkl").exists():
        detector = SupervisedAnomalyDetector(artifact_dir)
    else:
        detector = AnomalyDetector(artifact_dir)
    app = FastAPI(title="AIOps Anomaly Detection API", version="1.0.0")

    @app.get("/health")
    def health():
        payload = detector.health()
        payload["seq_len"] = detector.seq_len
        return payload

    @app.post("/detect")
    def detect(request: DetectRequest):
        return detector.detect(request)

    return app


def parse_args():
    parser = argparse.ArgumentParser(description="Serve anomaly detection API.")
    parser.add_argument(
        "--artifact-dir",
        default=str(
            Path(__file__).resolve().parents[1]
            / "artifacts"
            / "anomaly_final"
        ),
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    app = create_app(Path(args.artifact_dir))
    uvicorn.run(app, host=args.host, port=args.port)


