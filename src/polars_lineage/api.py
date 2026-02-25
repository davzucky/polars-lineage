from __future__ import annotations

from typing import Any

import polars as pl

from polars_lineage.config import MappingConfig
from polars_lineage.exporter import OutputFormat, RenderedLineage
from polars_lineage.exporter.models import LineageDocument
from polars_lineage.pipeline import (
    extract_lineage_output_from_lazyframe,
    extract_lineage_payloads_from_lazyframe,
)

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


def extract_lazyframe_lineage_formatted(
    lazyframe: pl.LazyFrame,
    mapping: MappingConfig | dict[str, Any],
    output_format: OutputFormat,
) -> RenderedLineage:
    normalized_mapping = _normalize_mapping(mapping)
    return extract_lineage_output_from_lazyframe(
        lazyframe,
        normalized_mapping,
        output_format=output_format,
    )


def extract_lazyframe_lineage_document(
    lazyframe: pl.LazyFrame, mapping: MappingConfig | dict[str, Any]
) -> LineageDocument:
    output = extract_lazyframe_lineage_formatted(lazyframe, mapping, output_format="json")
    if not isinstance(output, LineageDocument):  # pragma: no cover - defensive typing guard
        raise TypeError("json export must return a typed LineageDocument")
    return output
