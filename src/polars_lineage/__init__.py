from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import polars as pl

    from polars_lineage.config import MappingConfig
    from polars_lineage.exporter import OutputFormat, RenderedLineage
    from polars_lineage.exporter.models import LineageDocument
    from polars_lineage.lineage_lazyframe import LineageLazyFrame

from polars_lineage.exporter.models import LineageDocument
from polars_lineage.lineage_lazyframe import LineageLazyFrame, register_lazyframe_metadata_method

__all__ = [
    "__version__",
    "LineageDocument",
    "extract_lazyframe_lineage",
    "extract_lazyframe_lineage_document",
    "extract_lazyframe_lineage_formatted",
    "with_lineage",
    "LineageLazyFrame",
]

__version__ = "0.1.0"

register_lazyframe_metadata_method()


def extract_lazyframe_lineage(
    lazyframe: "pl.LazyFrame", mapping: "MappingConfig | dict[str, Any]"
) -> list[dict[str, Any]]:
    from polars_lineage.api import extract_lazyframe_lineage as _extract_lazyframe_lineage

    return _extract_lazyframe_lineage(lazyframe, mapping)


def extract_lazyframe_lineage_formatted(
    lazyframe: "pl.LazyFrame",
    mapping: "MappingConfig | dict[str, Any]",
    output_format: "OutputFormat",
) -> "RenderedLineage":
    from polars_lineage.api import (
        extract_lazyframe_lineage_formatted as _extract_lazyframe_lineage_formatted,
    )

    return _extract_lazyframe_lineage_formatted(lazyframe, mapping, output_format)


def extract_lazyframe_lineage_document(
    lazyframe: "pl.LazyFrame", mapping: "MappingConfig | dict[str, Any]"
) -> "LineageDocument":
    from polars_lineage.api import (
        extract_lazyframe_lineage_document as _extract_lazyframe_lineage_document,
    )

    return _extract_lazyframe_lineage_document(lazyframe, mapping)


def with_lineage(
    lazyframe: "pl.LazyFrame", mapping: "MappingConfig | dict[str, Any]"
) -> "LineageLazyFrame":
    from polars_lineage.lineage_lazyframe import with_lineage as _with_lineage

    return _with_lineage(lazyframe, mapping)
