from __future__ import annotations

from pipeline.rca_data_pipeline.feature_engineering import build_service_graphs as _build_service_graphs


def build_service_graphs(spans_df, run_catalog_df):
    return _build_service_graphs(spans_df, run_catalog_df)


__all__ = ["build_service_graphs"]
