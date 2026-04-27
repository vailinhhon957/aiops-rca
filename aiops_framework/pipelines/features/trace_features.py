from __future__ import annotations

from pipeline.rca_data_pipeline.feature_engineering import (
    TRACE_FEATURE_COLUMNS,
    build_trace_features as _build_trace_features,
    label_trace_features as _label_trace_features,
)


def build_trace_features(spans_df, run_catalog_df=None):
    return _build_trace_features(spans_df, run_catalog_df)


def label_trace_features(trace_features_df, run_catalog_df):
    return _label_trace_features(trace_features_df, run_catalog_df)


__all__ = [
    "TRACE_FEATURE_COLUMNS",
    "build_trace_features",
    "label_trace_features",
]
