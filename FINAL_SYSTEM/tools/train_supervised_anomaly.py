import argparse
import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import ExtraTreesClassifier, HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    average_precision_score,
    balanced_accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


DEFAULT_FEATURES = [
    "span_count",
    "service_count",
    "app_service_count",
    "avg_latency",
    "max_latency",
    "std_latency",
    "trace_latency",
    "error_rate",
    "http_5xx_rate",
    "depth",
    "latency_zscore",
    "duration_ratio",
    "adservice_avg_latency",
    "adservice_error_rate",
    "cartservice_avg_latency",
    "cartservice_error_rate",
    "checkoutservice_avg_latency",
    "checkoutservice_error_rate",
    "currencyservice_avg_latency",
    "currencyservice_error_rate",
    "emailservice_avg_latency",
    "emailservice_error_rate",
    "frontend_avg_latency",
    "frontend_error_rate",
    "paymentservice_avg_latency",
    "paymentservice_error_rate",
    "productcatalogservice_avg_latency",
    "productcatalogservice_error_rate",
    "recommendationservice_avg_latency",
    "recommendationservice_error_rate",
    "shippingservice_avg_latency",
    "shippingservice_error_rate",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Train supervised anomaly model on trace-level features.")
    parser.add_argument("--csv-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--model-type", choices=["hgb", "rf", "et"], default="hgb")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-depth", type=int, default=6)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    parser.add_argument("--max-iter", type=int, default=400)
    parser.add_argument("--min-samples-leaf", type=int, default=20)
    parser.add_argument("--l2-regularization", type=float, default=0.0)
    parser.add_argument("--class-weight", choices=["none", "balanced"], default="none")
    return parser.parse_args()


def build_model(args):
    model_type = args.model_type
    seed = args.seed
    balanced_tree_weight = "balanced_subsample" if args.class_weight == "balanced" else None

    if model_type == "rf":
        clf = RandomForestClassifier(
            n_estimators=500,
            max_depth=None,
            min_samples_leaf=2,
            class_weight=balanced_tree_weight,
            random_state=seed,
            n_jobs=-1,
        )
        return Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("classifier", clf),
            ]
        )

    if model_type == "et":
        clf = ExtraTreesClassifier(
            n_estimators=500,
            max_depth=None,
            min_samples_leaf=2,
            class_weight=balanced_tree_weight,
            random_state=seed,
            n_jobs=-1,
        )
        return Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("classifier", clf),
            ]
        )

    clf = HistGradientBoostingClassifier(
        max_depth=args.max_depth,
        learning_rate=args.learning_rate,
        max_iter=args.max_iter,
        min_samples_leaf=args.min_samples_leaf,
        l2_regularization=args.l2_regularization,
        random_state=seed,
    )
    return Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("classifier", clf),
        ]
    )


def compute_sample_weight(y: np.ndarray, class_weight: str):
    if class_weight != "balanced":
        return None
    classes, counts = np.unique(y, return_counts=True)
    total = counts.sum()
    weights = {cls: total / (len(classes) * count) for cls, count in zip(classes, counts)}
    return np.asarray([weights[int(label)] for label in y], dtype=np.float64)


def choose_threshold(y_true: np.ndarray, scores: np.ndarray):
    best_threshold = 0.5
    best_score = -1.0
    best_stats = None
    search_rows = []

    for threshold in np.linspace(0.05, 0.95, 181):
        pred = (scores >= threshold).astype(int)
        precision = precision_score(y_true, pred, zero_division=0)
        recall = recall_score(y_true, pred, zero_division=0)
        f1 = f1_score(y_true, pred, zero_division=0)
        balanced_acc = balanced_accuracy_score(y_true, pred)
        tn = int(((y_true == 0) & (pred == 0)).sum())
        fp = int(((y_true == 0) & (pred == 1)).sum())
        specificity = tn / max(tn + fp, 1)
        search_score = balanced_acc - 0.5 * max(0.0, 0.5 - specificity)
        stats = {
            "threshold": float(threshold),
            "balanced_acc": float(balanced_acc),
            "f1": float(f1),
            "precision": float(precision),
            "recall": float(recall),
        }
        search_rows.append(stats)
        if search_score > best_score:
            best_score = search_score
            best_threshold = float(threshold)
            best_stats = stats

    return best_threshold, best_stats, search_rows


def evaluate(y_true: np.ndarray, scores: np.ndarray, threshold: float):
    pred = (scores >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, pred).ravel()
    return {
        "precision_anomaly": float(precision_score(y_true, pred, zero_division=0)),
        "recall_anomaly": float(recall_score(y_true, pred, zero_division=0)),
        "specificity": float(tn / max(tn + fp, 1)),
        "balanced_accuracy": float(balanced_accuracy_score(y_true, pred)),
        "f1_anomaly": float(f1_score(y_true, pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_true, scores)),
        "average_precision": float(average_precision_score(y_true, scores)),
        "confusion_matrix": {
            "tp": int(tp),
            "tn": int(tn),
            "fp": int(fp),
            "fn": int(fn),
        },
        "classification_report": classification_report(
            y_true,
            pred,
            target_names=["Binh thuong", "Bat thuong"],
            digits=4,
            zero_division=0,
        ),
    }


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.csv_path)
    if "split_tag" not in df.columns:
        raise ValueError("CSV can cot split_tag de train supervised anomaly.")

    features = [feature for feature in DEFAULT_FEATURES if feature in df.columns]
    train_df = df[df["split_tag"] == "train"].copy()
    val_df = df[df["split_tag"] == "val"].copy()
    test_df = df[df["split_tag"] == "test"].copy()

    X_train = train_df[features]
    y_train = train_df["label"].astype(int).to_numpy()
    X_val = val_df[features]
    y_val = val_df["label"].astype(int).to_numpy()
    X_test = test_df[features]
    y_test = test_df["label"].astype(int).to_numpy()

    model = build_model(args)
    fit_kwargs = {}
    sample_weight = compute_sample_weight(y_train, args.class_weight)
    if sample_weight is not None and args.model_type == "hgb":
        fit_kwargs["classifier__sample_weight"] = sample_weight
    model.fit(X_train, y_train, **fit_kwargs)

    val_scores = model.predict_proba(X_val)[:, 1]
    threshold, threshold_stats, threshold_search = choose_threshold(y_val, val_scores)
    test_scores = model.predict_proba(X_test)[:, 1]
    test_metrics = evaluate(y_test, test_scores, threshold)

    metrics = {
        "model_name": Path(args.output_dir).name or f"supervised_anomaly_{args.model_type}",
        "features": features,
        "seed": args.seed,
        "threshold": threshold,
        "threshold_validation_stats": threshold_stats,
        "train_rows": int(len(train_df)),
        "val_rows": int(len(val_df)),
        "test_rows": int(len(test_df)),
        "model_type": args.model_type,
        "model_params": {
            "max_depth": args.max_depth,
            "learning_rate": args.learning_rate,
            "max_iter": args.max_iter,
            "min_samples_leaf": args.min_samples_leaf,
            "l2_regularization": args.l2_regularization,
            "class_weight": args.class_weight,
        },
        "test_metrics": {
            key: value for key, value in test_metrics.items() if key != "classification_report"
        },
        "classification_report": test_metrics["classification_report"],
    }

    with (output_dir / "supervised_anomaly_model.pkl").open("wb") as f:
        pickle.dump(model, f)
    with (output_dir / "supervised_anomaly_metrics.json").open("w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=True)
    with (output_dir / "supervised_anomaly_threshold_search.json").open("w", encoding="utf-8") as f:
        json.dump(threshold_search, f, indent=2, ensure_ascii=True)

    print(json.dumps(metrics["test_metrics"], indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
