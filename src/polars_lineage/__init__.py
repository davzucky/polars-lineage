from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import polars as pl

    from polars_lineage.config import MappingConfig
    from polars_lineage.lineage_lazyframe import LineageLazyFrame

from polars_lineage.lineage_lazyframe import LineageLazyFrame, register_lazyframe_metadata_method

__all__ = ["__version__", "extract_lazyframe_lineage", "with_lineage", "LineageLazyFrame"]

__version__ = "0.1.0"

register_lazyframe_metadata_method()


def extract_lazyframe_lineage(
    lazyframe: "pl.LazyFrame", mapping: "MappingConfig | dict[str, Any]"
) -> list[dict[str, Any]]:
    from polars_lineage.api import extract_lazyframe_lineage as _extract_lazyframe_lineage

    return _extract_lazyframe_lineage(lazyframe, mapping)


def with_lineage(
    lazyframe: "pl.LazyFrame", mapping: "MappingConfig | dict[str, Any]"
) -> "LineageLazyFrame":
    from polars_lineage.lineage_lazyframe import with_lineage as _with_lineage

    return _with_lineage(lazyframe, mapping)
