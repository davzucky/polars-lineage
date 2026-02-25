from __future__ import annotations

from collections import defaultdict

from polars_lineage.exporter.models import LineageColumn, LineageDocument, LineageEdge
from polars_lineage.ir import ColumnLineage


def export_lineage_document(
    lineage: list[ColumnLineage], destination_table: str
) -> LineageDocument:
    grouped: dict[tuple[str, str], dict[tuple[str, str, str], set[str]]] = defaultdict(dict)

    for entry in lineage:
        destination_dataset_fqn = entry.to_column.dataset.fqn
        columns_by_source: dict[str, set[str]] = defaultdict(set)
        for source in entry.from_columns:
            columns_by_source[source.dataset.fqn].add(source.column)

        for source_fqn, source_columns in columns_by_source.items():
            edge_key = (source_fqn, destination_dataset_fqn)
            column_key = (entry.to_column.column, entry.function, entry.confidence)
            grouped[edge_key].setdefault(column_key, set()).update(source_columns)

    edges: list[LineageEdge] = []
    for (source_fqn, destination_fqn), columns_lineage in grouped.items():
        columns = [
            LineageColumn(
                to_column=to_column,
                from_columns=sorted(from_columns),
                function=function,
                confidence=confidence,
            )
            for (to_column, function, confidence), from_columns in columns_lineage.items()
        ]
        edges.append(
            LineageEdge(
                source_table=source_fqn,
                destination_table=destination_fqn,
                columns=columns,
            )
        )

    return LineageDocument(destination_table=destination_table, edges=edges)
