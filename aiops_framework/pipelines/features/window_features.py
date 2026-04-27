from __future__ import annotations

from pipeline.rca_data_pipeline.feature_engineering import (
    WINDOW_FEATURE_COLUMNS,
    build_window_features as _build_window_features,
    label_window_features as _label_window_features,
)


def build_window_features(spans_df, run_catalog_df=None):
    return _build_window_features(spans_df, run_catalog_df)


def label_window_features(window_features_df, run_catalog_df):
    return _label_window_features(window_features_df, run_catalog_df)


__all__ = [
    "WINDOW_FEATURE_COLUMNS",
    "build_window_features",
    "label_window_features",
]
