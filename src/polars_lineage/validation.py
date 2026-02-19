from __future__ import annotations

from polars_lineage.ir import ColumnLineage


def validate_lineage(lineage: list[ColumnLineage]) -> None:
    if not lineage:
        raise ValueError("no lineage entries were extracted")

    for item in lineage:
        if not item.to_column.column:
            raise ValueError("destination column is required")
        if not item.function:
            raise ValueError("lineage function text is required")
