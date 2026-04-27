import argparse
import json
import math
from pathlib import Path
from typing import Optional

import numpy as np
import torch
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from rca_graph_utils import summarize_service_features
from train_gat_rca import (
    GATRootCauseRanker,
    augment_edge_index,
    compute_normal_service_stats,
    enrich_record,
    split_fault_records,
)


class GraphRecord(BaseModel):
    trace_id: str = Field(default="unknown_trace")
    scenario: Optional[str] = Field(default="online")
    x: list[list[float]]
    edge_index: list[list[int]] = Field(default_factory=list)


class RankRequest(BaseModel):
    graph: GraphRecord
    top_k: int = Field(default=3, ge=1, le=10)
    anomaly_context: Optional[dict] = None


class RankFromTraceRequest(BaseModel):
    trace_id: str = Field(default="unknown_trace")
    scenario: Optional[str] = Field(default="online")
    spans: list[dict] = Field(..., min_length=1)
    top_k: int = Field(default=3, ge=1, le=10)
    anomaly_context: Optional[dict] = None


class RCAPredictor:
    def __init__(self, artifact_dir: Path, graph_path: Optional[Path], metadata_path: Optional[Path]):
        self.artifact_dir = artifact_dir
        with (artifact_dir / "gat_rca_metrics.json").open("r", encoding="utf-8") as f:
            self.metrics = json.load(f)

        self.preprocessing, self.preprocessing_source = self._load_preprocessing(graph_path, metadata_path)
        self.services = self.preprocessing["services"]
        self.num_services = len(self.services)
        self.node_feature_names = self.preprocessing.get(
            "node_feature_names",
            self.metrics.get("node_feature_names", []),
        )
        self.numeric_dim = int(self.preprocessing["numeric_dim"])
        self.normal_means = np.asarray(self.preprocessing["normal_means"], dtype=np.float32)
        self.normal_stds = np.asarray(self.preprocessing["normal_stds"], dtype=np.float32)
        self.scaler_mean = np.asarray(self.preprocessing["scaler_mean"], dtype=np.float32)
        self.scaler_std = np.asarray(self.preprocessing["scaler_std"], dtype=np.float32)
        if self.scaler_mean.ndim == 1:
            self.scaler_mean = self.scaler_mean.reshape(1, -1)
        if self.scaler_std.ndim == 1:
            self.scaler_std = self.scaler_std.reshape(1, -1)

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = GATRootCauseRanker(
            input_dim=int(self.preprocessing["model_input_dim"]),
            hidden_dim=int(self.metrics.get("hidden_dim", 72)),
            heads=int(self.metrics.get("heads", 4)),
            dropout=float(self.metrics.get("dropout", 0.25)),
        ).to(self.device)
        state = torch.load(artifact_dir / "gat_rca.pt", map_location=self.device)
        self.model.load_state_dict(state)
        self.model.eval()

    def _load_preprocessing(self, graph_path: Optional[Path], metadata_path: Optional[Path]):
        preprocessing_path = self.artifact_dir / "gat_rca_preprocessing.json"
        if preprocessing_path.exists():
            with preprocessing_path.open("r", encoding="utf-8") as f:
                return json.load(f), "artifact"

        if graph_path is None or metadata_path is None:
            raise ValueError(
                "Thieu gat_rca_preprocessing.json va cung chua truyen graph-path/metadata-path de khoi phuc preprocessing."
            )

        with graph_path.open("r", encoding="utf-8") as f:
            records = json.load(f)
        with metadata_path.open("r", encoding="utf-8") as f:
            metadata = json.load(f)

        services = metadata["services"]
        num_services = len(services)
        normal_records = [record for record in records if int(record["graph_label"]) == 0]
        fault_records = [record for record in records if int(record["graph_label"]) == 1 and sum(record["y"]) > 0]
        if len(normal_records) == 0 or len(fault_records) < 10:
            raise ValueError("Khong du du lieu de khoi phuc preprocessing cho GAT RCA.")

        seed = int(self.metrics.get("seed", 42))
        normal_means, normal_stds = compute_normal_service_stats(normal_records, num_services)
        train_records, _, _ = split_fault_records(fault_records, seed)

        train_numeric = []
        numeric_dim = None
        for record in train_records:
            features, numeric_dim = enrich_record(record, num_services, normal_means, normal_stds)
            train_numeric.append(features[:, :numeric_dim])

        train_numeric = np.concatenate(train_numeric, axis=0)
        scaler_mean = train_numeric.mean(axis=0, keepdims=True)
        scaler_std = train_numeric.std(axis=0, keepdims=True) + 1e-8
        sample_features, _ = enrich_record(train_records[0], num_services, normal_means, normal_stds)

        preprocessing = {
            "model_name": self.metrics.get("model_name", "rca_final"),
            "services": services,
            "node_feature_names": metadata.get("node_feature_names"),
            "seed": seed,
            "normal_means": normal_means.tolist(),
            "normal_stds": normal_stds.tolist(),
            "scaler_mean": scaler_mean.tolist(),
            "scaler_std": scaler_std.tolist(),
            "numeric_dim": int(numeric_dim),
            "model_input_dim": int(sample_features.shape[1]),
            "make_undirected": True,
        }
        return preprocessing, "reconstructed_from_dataset"

    def _validate_graph(self, graph: dict):
        if len(graph["x"]) != self.num_services:
            raise HTTPException(
                status_code=400,
                detail=f"So node phai bang {self.num_services} services, nhan duoc {len(graph['x'])}.",
            )
        expected_dim = len(self.node_feature_names) if self.node_feature_names else len(graph["x"][0])
        for idx, row in enumerate(graph["x"]):
            if len(row) != expected_dim:
                raise HTTPException(
                    status_code=400,
                    detail=f"Node feature tai index {idx} co {len(row)} chieu, can {expected_dim}.",
                )

    def _prepare_inputs(self, graph: dict):
        self._validate_graph(graph)
        features, numeric_dim = enrich_record(
            graph,
            self.num_services,
            self.normal_means,
            self.normal_stds,
        )
        features[:, :numeric_dim] = (features[:, :numeric_dim] - self.scaler_mean) / self.scaler_std
        edge_index = augment_edge_index(
            graph["edge_index"],
            self.num_services,
            make_undirected=bool(self.preprocessing.get("make_undirected", True)),
        )
        active_mask = (np.asarray(graph["x"], dtype=np.float32)[:, 0] > 0).tolist()
        return (
            torch.tensor(features, dtype=torch.float32, device=self.device),
            edge_index.to(self.device),
            active_mask,
        )

    def build_graph_from_trace(self, trace_id: str, scenario: Optional[str], spans: list[dict]) -> dict:
        x, edge_index, active_services = summarize_service_features(spans)
        if not active_services:
            raise HTTPException(status_code=400, detail="Trace khong co app service hop le de RCA.")
        return {
            "trace_id": trace_id,
            "scenario": scenario or "online",
            "x": x,
            "edge_index": edge_index,
        }

    def rank_graph(
        self,
        graph: dict,
        top_k: int,
        anomaly_context: Optional[dict] = None,
        source: str = "graph",
    ):
        graph_tensor, edge_index, active_mask = self._prepare_inputs(graph)
        with torch.no_grad():
            scores = self.model(graph_tensor, edge_index).detach().cpu().numpy()

        ranking_indices = np.argsort(scores)[::-1].tolist()
        active_services = [
            service_name for service_name, is_active in zip(self.services, active_mask) if is_active
        ]
        top_scores = [float(scores[idx]) for idx in ranking_indices[:top_k]]
        confidences = self._softmax(top_scores)

        ranking = []
        for rank, (service_idx, confidence) in enumerate(
            zip(ranking_indices[:top_k], confidences),
            start=1,
        ):
            ranking.append(
                {
                    "rank": rank,
                    "service_name": self.services[service_idx],
                    "service_index": service_idx,
                    "score": float(scores[service_idx]),
                    "confidence": float(confidence),
                    "is_active": bool(active_mask[service_idx]),
                }
            )

        return {
            "model": self.metrics.get("model_name", "rca_final"),
            "trace_id": graph.get("trace_id"),
            "scenario": graph.get("scenario"),
            "source": source,
            "top_k": top_k,
            "input_summary": {
                "active_services": active_services,
                "edge_count": len(graph["edge_index"]),
                "node_count": len(graph["x"]),
            },
            "root_cause_service": ranking[0]["service_name"],
            "confidence": ranking[0]["confidence"],
            "root_causes": [
                {
                    "service": item["service_name"],
                    "confidence": item["confidence"],
                }
                for item in ranking
            ],
            # Action selection belongs to the platform/decision module.
            "recommended_action": None,
            "recommended_action_owner": "platform",
            "predicted_root_cause": ranking[0],
            "ranking": ranking,
            "platform_handoff": {
                "target_service": ranking[0]["service_name"],
                "candidate_services": [item["service_name"] for item in ranking],
                "rca_model": self.metrics.get("model_name", "rca_final"),
            },
            "anomaly_context": anomaly_context,
        }

    @staticmethod
    def _softmax(values: list[float]) -> list[float]:
        if not values:
            return []
        max_value = max(values)
        exps = [math.exp(value - max_value) for value in values]
        total = sum(exps) or 1.0
        return [value / total for value in exps]


def create_app(artifact_dir: Path, graph_path: Optional[Path], metadata_path: Optional[Path]) -> FastAPI:
    predictor = RCAPredictor(artifact_dir, graph_path, metadata_path)
    app = FastAPI(title="AIOps GAT RCA API", version="1.0.0")

    @app.get("/health")
    def health():
        return {
            "status": "ok",
            "model": predictor.metrics.get("model_name", "rca_final"),
            "services": predictor.services,
            "preprocessing_source": predictor.preprocessing_source,
        }

    @app.post("/rank")
    def rank(request: RankRequest):
        return predictor.rank_graph(
            request.graph.model_dump(),
            top_k=request.top_k,
            anomaly_context=request.anomaly_context,
            source="graph_api",
        )

    @app.post("/rank-from-trace")
    def rank_from_trace(request: RankFromTraceRequest):
        graph = predictor.build_graph_from_trace(
            trace_id=request.trace_id,
            scenario=request.scenario,
            spans=request.spans,
        )
        return predictor.rank_graph(
            graph,
            top_k=request.top_k,
            anomaly_context=request.anomaly_context,
            source="trace_api",
        )

    return app


def parse_args():
    base = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description="Serve GAT RCA inference API.")
    parser.add_argument(
        "--artifact-dir",
        default=str(base / "rca" / "artifacts" / "rca_final"),
    )
    parser.add_argument(
        "--graph-path",
        default=str(base / "rca" / "dataset" / "graph_dataset_final.json"),
    )
    parser.add_argument(
        "--metadata-path",
        default=str(base / "rca" / "dataset" / "graph_metadata_final.json"),
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8100)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    app = create_app(
        Path(args.artifact_dir),
        Path(args.graph_path) if args.graph_path else None,
        Path(args.metadata_path) if args.metadata_path else None,
    )
    uvicorn.run(app, host=args.host, port=args.port)
