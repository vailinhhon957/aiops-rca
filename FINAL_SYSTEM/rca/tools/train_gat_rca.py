import argparse
import json
import random
from collections import defaultdict
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from sklearn.model_selection import train_test_split
from torch_geometric.data import Data
from torch_geometric.nn import GATConv


def set_seed(seed: int):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)


def split_fault_records(records, seed):
    root_labels = [int(np.argmax(record["y"])) for record in records]
    train_records, temp_records, root_train, root_temp = train_test_split(
        records,
        root_labels,
        test_size=0.3,
        random_state=seed,
        stratify=root_labels,
    )
    val_records, test_records = train_test_split(
        temp_records,
        test_size=0.5,
        random_state=seed,
        stratify=root_temp,
    )
    return train_records, val_records, test_records


def extract_run_id(record):
    source_file = str(record.get("source_file", "")).strip()
    if source_file:
        source_path = Path(source_file)
        path_parts = source_path.parts
        if "windows" in path_parts:
            windows_idx = path_parts.index("windows")
            if windows_idx >= 1:
                return path_parts[windows_idx - 1]
        if source_path.parent.name == "windows" and source_path.parent.parent.name:
            return source_path.parent.parent.name
        if source_path.stem:
            return source_path.stem
    trace_id = str(record.get("trace_id", "")).strip()
    if "::" in trace_id:
        return trace_id.split("::", 1)[0]
    if "|" in trace_id:
        return trace_id.split("|", 1)[0]
    if "__" in trace_id:
        return trace_id.split("__", 1)[0]
    return trace_id


def load_run_ids(path):
    with Path(path).open("r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def split_records_by_runs(records, train_run_ids, val_run_ids, test_run_ids):
    train_records = []
    val_records = []
    test_records = []

    for record in records:
        run_id = extract_run_id(record)
        if run_id in train_run_ids:
            train_records.append(record)
        elif run_id in val_run_ids:
            val_records.append(record)
        elif run_id in test_run_ids:
            test_records.append(record)

    return train_records, val_records, test_records


def augment_edge_index(edge_pairs, num_nodes, make_undirected=True):
    edges = set()
    for src, dst in edge_pairs:
        edges.add((int(src), int(dst)))
        if make_undirected:
            edges.add((int(dst), int(src)))
    for node_idx in range(num_nodes):
        edges.add((node_idx, node_idx))
    return torch.tensor(sorted(edges), dtype=torch.long).t().contiguous()


def transform_base_features(x):
    base = np.asarray(x, dtype=np.float32).copy()
    for col_idx in [0, 1, 2, 3, 4]:
        base[:, col_idx] = np.log1p(np.clip(base[:, col_idx], 0, None))
    return base


def compute_normal_service_stats(records, num_services):
    service_buckets = [[] for _ in range(num_services)]
    for record in records:
        base = transform_base_features(record["x"])
        for idx in range(num_services):
            service_buckets[idx].append(base[idx])

    means = []
    stds = []
    for bucket in service_buckets:
        bucket_arr = np.asarray(bucket, dtype=np.float32)
        means.append(bucket_arr.mean(axis=0))
        stds.append(bucket_arr.std(axis=0) + 1e-8)

    return np.asarray(means, dtype=np.float32), np.asarray(stds, dtype=np.float32)


def enrich_record(record, num_services, normal_means, normal_stds):
    base = transform_base_features(record["x"])
    active = (np.asarray(record["x"], dtype=np.float32)[:, 0] > 0).astype(np.float32).reshape(-1, 1)

    indeg = np.zeros((num_services, 1), dtype=np.float32)
    outdeg = np.zeros((num_services, 1), dtype=np.float32)
    for src, dst in record["edge_index"]:
        outdeg[int(src), 0] += 1.0
        indeg[int(dst), 0] += 1.0
    total_deg = indeg + outdeg
    deg_feats = np.concatenate(
        [
            indeg / max(num_services, 1),
            outdeg / max(num_services, 1),
            total_deg / max(num_services * 2, 1),
        ],
        axis=1,
    )

    deviation = (base - normal_means) / normal_stds
    one_hot = np.eye(num_services, dtype=np.float32)

    enriched = np.concatenate([base, deviation, deg_feats, active, one_hot], axis=1)
    numeric_dim = base.shape[1] + deviation.shape[1] + deg_feats.shape[1] + active.shape[1]
    return enriched, numeric_dim


def to_graph(record, num_services, normal_means, normal_stds, scaler_mean, scaler_std):
    features, numeric_dim = enrich_record(record, num_services, normal_means, normal_stds)
    features[:, :numeric_dim] = (features[:, :numeric_dim] - scaler_mean) / scaler_std
    edge_index = augment_edge_index(record["edge_index"], num_services, make_undirected=True)
    root_idx = int(np.argmax(record["y"]))
    return Data(
        x=torch.tensor(features, dtype=torch.float32),
        edge_index=edge_index,
        root_idx=torch.tensor(root_idx, dtype=torch.long),
        trace_id=record["trace_id"],
        scenario=record["scenario"],
    )


class GATRootCauseRanker(nn.Module):
    def __init__(self, input_dim, hidden_dim=72, heads=4, dropout=0.25):
        super().__init__()
        self.input_proj = nn.Linear(input_dim, hidden_dim)
        self.gat1 = GATConv(hidden_dim, hidden_dim, heads=heads, dropout=dropout, concat=True)
        self.gat2 = GATConv(hidden_dim * heads, hidden_dim, heads=1, dropout=dropout, concat=False)
        self.gat3 = GATConv(hidden_dim, hidden_dim, heads=1, dropout=dropout, concat=False)
        self.scorer = nn.Sequential(
            nn.Linear(hidden_dim, 32),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(32, 1),
        )
        self.dropout = nn.Dropout(dropout)

    def forward(self, x, edge_index):
        x0 = self.input_proj(x)
        x = self.gat1(x0, edge_index)
        x = F.elu(x)
        x = self.dropout(x)
        x = self.gat2(x, edge_index)
        x = F.elu(x)
        x = self.dropout(x)
        x = self.gat3(x, edge_index)
        x = F.elu(x)
        x = x + x0
        return self.scorer(x).squeeze(-1)


def graph_rank_loss(scores, target_idx, margin=0.4):
    target_score = scores[target_idx]
    negative_scores = torch.cat([scores[:target_idx], scores[target_idx + 1:]])
    hardest_negative = negative_scores.max()
    return F.relu(margin - target_score + hardest_negative)


def evaluate_ranker(model, graphs, device, services):
    model.eval()
    top1_hits = 0
    top3_hits = 0
    reciprocal_ranks = []
    per_service = defaultdict(lambda: {"total": 0, "top1_hits": 0, "top3_hits": 0})

    with torch.no_grad():
        for graph in graphs:
            scores = model(graph.x.to(device), graph.edge_index.to(device))
            ranking = torch.argsort(scores, descending=True).cpu().tolist()
            true_idx = int(graph.root_idx.item())
            rank = ranking.index(true_idx) + 1
            reciprocal_ranks.append(1.0 / rank)

            svc_name = services[true_idx]
            per_service[svc_name]["total"] += 1
            if rank == 1:
                top1_hits += 1
                per_service[svc_name]["top1_hits"] += 1
            if rank <= 3:
                top3_hits += 1
                per_service[svc_name]["top3_hits"] += 1

    total = max(len(graphs), 1)
    per_service_metrics = {}
    for service, stats in per_service.items():
        svc_total = max(stats["total"], 1)
        per_service_metrics[service] = {
            "count": stats["total"],
            "top1_acc": stats["top1_hits"] / svc_total,
            "top3_acc": stats["top3_hits"] / svc_total,
        }

    return {
        "graphs": len(graphs),
        "top1_acc": top1_hits / total,
        "top3_acc": top3_hits / total,
        "mrr": float(sum(reciprocal_ranks) / max(len(reciprocal_ranks), 1)),
        "per_service": per_service_metrics,
    }


def main():
    parser = argparse.ArgumentParser(description="Upgraded GAT ranker with normal-baseline deviations.")
    parser.add_argument("--graph-path", required=True)
    parser.add_argument("--metadata-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--epochs", type=int, default=140)
    parser.add_argument("--lr", type=float, default=8e-4)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--hidden-dim", type=int, default=72)
    parser.add_argument("--heads", type=int, default=4)
    parser.add_argument("--dropout", type=float, default=0.25)
    parser.add_argument("--edge-drop-rate", type=float, default=0.1)
    parser.add_argument("--train-runs-path")
    parser.add_argument("--val-runs-path")
    parser.add_argument("--test-runs-path")
    args = parser.parse_args()

    set_seed(args.seed)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    with Path(args.graph_path).open("r", encoding="utf-8") as f:
        records = json.load(f)
    with Path(args.metadata_path).open("r", encoding="utf-8") as f:
        metadata = json.load(f)

    services = metadata["services"]
    num_services = len(services)
    normal_records = [record for record in records if int(record["graph_label"]) == 0]
    fault_records = [record for record in records if int(record["graph_label"]) == 1 and sum(record["y"]) > 0]

    if len(normal_records) == 0 or len(fault_records) < 10:
        raise ValueError("Khong du du lieu normal/fault de train GAT RCA.")

    if args.train_runs_path or args.val_runs_path or args.test_runs_path:
        if not (args.train_runs_path and args.val_runs_path and args.test_runs_path):
            raise ValueError("Can cung cap day du --train-runs-path, --val-runs-path, --test-runs-path.")

        train_run_ids = load_run_ids(args.train_runs_path)
        val_run_ids = load_run_ids(args.val_runs_path)
        test_run_ids = load_run_ids(args.test_runs_path)
        train_records, val_records, test_records = split_records_by_runs(
            fault_records,
            train_run_ids,
            val_run_ids,
            test_run_ids,
        )
        if len(train_records) == 0 or len(val_records) == 0 or len(test_records) == 0:
            raise ValueError(
                "Split fault records theo run bi rong. Hay kiem tra train/val/test run files va graph dataset."
            )
        normal_train_records = [record for record in normal_records if extract_run_id(record) in train_run_ids]
        if len(normal_train_records) == 0:
            normal_train_records = normal_records
    else:
        train_records, val_records, test_records = split_fault_records(fault_records, args.seed)
        normal_train_records = normal_records

    normal_means, normal_stds = compute_normal_service_stats(normal_train_records, num_services)

    train_numeric = []
    numeric_dim = None
    for record in train_records:
        features, numeric_dim = enrich_record(record, num_services, normal_means, normal_stds)
        train_numeric.append(features[:, :numeric_dim])
    train_numeric = np.concatenate(train_numeric, axis=0)
    scaler_mean = train_numeric.mean(axis=0, keepdims=True)
    scaler_std = train_numeric.std(axis=0, keepdims=True) + 1e-8

    train_graphs = [
        to_graph(record, num_services, normal_means, normal_stds, scaler_mean, scaler_std)
        for record in train_records
    ]
    val_graphs = [
        to_graph(record, num_services, normal_means, normal_stds, scaler_mean, scaler_std)
        for record in val_records
    ]
    test_graphs = [
        to_graph(record, num_services, normal_means, normal_stds, scaler_mean, scaler_std)
        for record in test_records
    ]

    model = GATRootCauseRanker(
        input_dim=train_graphs[0].x.shape[1],
        hidden_dim=args.hidden_dim,
        heads=args.heads,
        dropout=args.dropout,
    ).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=args.lr, weight_decay=1e-4)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
        optimizer, T_max=args.epochs, eta_min=args.lr * 0.01
    )

    history = []
    best_state = None
    best_score = -1.0
    patience = 30
    stagnant_epochs = 0
    warmup_epochs = 15  # skip model selection during early unstable training

    print("Bat dau train GAT RCA...")
    for epoch in range(args.epochs):
        model.train()
        random.shuffle(train_graphs)
        total_loss = 0.0

        for graph in train_graphs:
            optimizer.zero_grad()
            edge_index = graph.edge_index.to(device)
            if args.edge_drop_rate > 0:
                num_edges = edge_index.size(1)
                keep_mask = torch.rand(num_edges, device=device) > args.edge_drop_rate
                # Always preserve self-loops added by augment_edge_index
                keep_mask |= edge_index[0] == edge_index[1]
                edge_index = edge_index[:, keep_mask]
            scores = model(graph.x.to(device), edge_index)
            target = graph.root_idx.to(device).unsqueeze(0)
            ce_loss = F.cross_entropy(scores.unsqueeze(0), target, label_smoothing=0.03)
            rank_loss = graph_rank_loss(scores, int(graph.root_idx.item()), margin=0.8)
            loss = ce_loss + 0.35 * rank_loss
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            total_loss += float(loss.item())

        scheduler.step()
        avg_loss = total_loss / max(len(train_graphs), 1)
        val_metrics = evaluate_ranker(model, val_graphs, device, services)
        ranking_score = val_metrics["top1_acc"] + 0.30 * val_metrics["top3_acc"] + 0.20 * val_metrics["mrr"]
        history.append(
            {
                "loss": avg_loss,
                "val_top1": val_metrics["top1_acc"],
                "val_top3": val_metrics["top3_acc"],
                "val_mrr": val_metrics["mrr"],
            }
        )

        if epoch >= warmup_epochs and ranking_score > best_score:
            best_score = ranking_score
            best_state = {k: v.detach().cpu() for k, v in model.state_dict().items()}
            stagnant_epochs = 0
            print(
                f"  [best] Epoch {epoch + 1:03d}: ranking_score={ranking_score:.4f} "
                f"top1={val_metrics['top1_acc']*100:.0f}% mrr={val_metrics['mrr']:.3f}"
            )
        elif epoch >= warmup_epochs:
            stagnant_epochs += 1

        if (epoch + 1) % 10 == 0 or epoch == 0:
            print(
                f"Epoch {epoch + 1:03d}/{args.epochs} | Loss: {avg_loss:.4f} "
                f"| Val Top-1: {val_metrics['top1_acc']*100:.1f}% "
                f"| Val Top-3: {val_metrics['top3_acc']*100:.1f}% "
                f"| Val MRR: {val_metrics['mrr']:.3f}"
            )

        if stagnant_epochs >= patience:
            print(f"Early stopping at epoch {epoch + 1}.")
            break

    if best_state is not None:
        model.load_state_dict(best_state)

    val_metrics = evaluate_ranker(model, val_graphs, device, services)
    test_metrics = evaluate_ranker(model, test_graphs, device, services)

    print("Validation metrics:")
    print(json.dumps(val_metrics, indent=2, ensure_ascii=True))
    print("Test metrics:")
    print(json.dumps(test_metrics, indent=2, ensure_ascii=True))

    metrics = {
        "model_name": output_dir.name or "rca_final",
        "services": services,
        "node_feature_names": metadata.get("node_feature_names"),
        "epochs_requested": args.epochs,
        "epochs_trained": len(history),
        "train_graphs": len(train_graphs),
        "val_graphs": len(val_graphs),
        "test_graphs": len(test_graphs),
        "seed": args.seed,
        "hidden_dim": args.hidden_dim,
        "heads": args.heads,
        "dropout": args.dropout,
        "val_metrics": val_metrics,
        "test_metrics": test_metrics,
    }

    preprocessing = {
        "model_name": output_dir.name or "rca_final",
        "services": services,
        "node_feature_names": metadata.get("node_feature_names"),
        "seed": args.seed,
        "normal_means": normal_means.tolist(),
        "normal_stds": normal_stds.tolist(),
        "scaler_mean": scaler_mean.tolist(),
        "scaler_std": scaler_std.tolist(),
        "numeric_dim": int(numeric_dim),
        "model_input_dim": int(train_graphs[0].x.shape[1]),
        "make_undirected": True,
    }

    torch.save(model.state_dict(), output_dir / "gat_rca.pt")
    with (output_dir / "gat_rca_metrics.json").open("w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=True)
    with (output_dir / "gat_rca_history.json").open("w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, ensure_ascii=True)
    with (output_dir / "gat_rca_preprocessing.json").open("w", encoding="utf-8") as f:
        json.dump(preprocessing, f, indent=2, ensure_ascii=True)


if __name__ == "__main__":
    main()
