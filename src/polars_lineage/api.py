from __future__ import annotations

from typing import Any

import polars as pl

from polars_lineage.config import MappingConfig
from polars_lineage.pipeline import extract_lineage_payloads_from_lazyframe

LineagePayload = dict[str, Any]


def _normalize_mapping(mapping: MappingConfig | dict[str, Any]) -> MappingConfig:
    if isinstance(mapping, MappingConfig):
        return mapping
    return MappingConfig.model_validate(mapping)


def extract_lazyframe_lineage(
    lazyframe: pl.LazyFrame, mapping: MappingConfig | dict[str, Any]
) -> list[LineagePayload]:
    """Extract OpenMetadata-shaped lineage payloads from a Polars LazyFrame.

    Args:
        lazyframe: Polars LazyFrame to analyze via explain tree parsing.
        mapping: MappingConfig or dict containing `sources` and `destination_table`.

    Returns:
        Deterministically sorted list of OpenMetadata AddLineageRequest-shaped payloads.

    Raises:
        pydantic.ValidationError: If mapping input is invalid.
        ValueError: If lineage extraction cannot resolve required source columns.
    """
    normalized_mapping = _normalize_mapping(mapping)
    return extract_lineage_payloads_from_lazyframe(lazyframe, normalized_mapping)
