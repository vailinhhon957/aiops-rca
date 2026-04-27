from __future__ import annotations

from typing import Any

import torch

from .model_def import build_adjacency
from .model_loader import LoadedRcaArtifacts


def predict_graph(
    artifacts: LoadedRcaArtifacts,
    x_rows: list[list[float]],
    edge_index_rows: list[list[int]],
    node_names: list[str],
    top_k: int = 3,
    graph_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    x = torch.tensor(x_rows, dtype=torch.float32)
    if edge_index_rows:
        edge_index = torch.tensor(edge_index_rows, dtype=torch.long).t().contiguous()
    else:
        edge_index = torch.empty((2, 0), dtype=torch.long)
    adj = build_adjacency(edge_index, x.size(0))

    with torch.no_grad():
        logits = artifacts.model(x.to(artifacts.device), adj.to(artifacts.device)).cpu()
        probs = torch.softmax(logits, dim=0)
        ranked = torch.argsort(probs, descending=True).tolist()

    top_k = max(1, min(int(top_k), len(node_names)))
    ranked_items = []
    for node_index in ranked[:top_k]:
        ranked_items.append(
            {
                "node_index": int(node_index),
                "service_name": str(node_names[node_index]),
                "score": float(probs[node_index].item()),
            }
        )

    return {
        "graph_id": graph_id,
        "top1": ranked_items[0],
        "topk": ranked_items,
        "model_name": str(artifacts.inference_config.get("model_name", artifacts.artifact_dir.name)),
        "model_type": str(artifacts.inference_config.get("model_type", "simple_graph_attention")),
        "metadata": metadata or {},
    }
