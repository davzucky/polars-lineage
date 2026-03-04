from __future__ import annotations

from polars_lineage.exporter.models import LineageDocument
from polars_lineage.lineage_namespace import register_lineage_namespace

__all__ = [
    "__version__",
    "LineageDocument",
]

__version__ = "0.1.0"

register_lineage_namespace()
