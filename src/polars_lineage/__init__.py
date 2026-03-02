from __future__ import annotations

from polars_lineage.exporter.models import LineageDocument
from polars_lineage.lineage_lazyframe import LineageLazyFrame, register_lazyframe_metadata_method

__all__ = [
    "__version__",
    "LineageDocument",
    "LineageLazyFrame",
]

__version__ = "0.1.0"

register_lazyframe_metadata_method()
