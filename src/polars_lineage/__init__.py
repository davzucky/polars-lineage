from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import polars as pl

    from polars_lineage.config import MappingConfig

__all__ = ["__version__", "extract_lazyframe_lineage"]

__version__ = "0.1.0"


def extract_lazyframe_lineage(
    lazyframe: "pl.LazyFrame", mapping: "MappingConfig | dict[str, Any]"
) -> list[dict[str, Any]]:
    from polars_lineage.api import extract_lazyframe_lineage as _extract_lazyframe_lineage

    return _extract_lazyframe_lineage(lazyframe, mapping)
