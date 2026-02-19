from __future__ import annotations

from collections import defaultdict
from typing import Any

from polars_lineage.ir import ColumnLineage


def _dataset_entity(dataset_fqn: str) -> dict[str, str]:
    return {"type": "table", "fullyQualifiedName": dataset_fqn}


def export_openmetadata_requests(lineage: list[ColumnLineage]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[tuple[str, str], set[str]]] = defaultdict(dict)

    for entry in lineage:
        destination_dataset_fqn = entry.to_column.dataset.fqn
        columns_by_source: dict[str, set[str]] = defaultdict(set)
        for source in entry.from_columns:
            columns_by_source[source.dataset.fqn].add(source.column)

        for source_fqn, source_columns in columns_by_source.items():
            edge_key = (source_fqn, destination_dataset_fqn)
            column_key = (entry.to_column.column, entry.function)
            grouped[edge_key].setdefault(column_key, set()).update(source_columns)

    payloads: list[dict[str, Any]] = []
    for (source_fqn, destination_fqn), columns_lineage in sorted(grouped.items()):
        serialized_columns = [
            {
                "fromColumns": sorted(list(from_columns)),
                "toColumn": to_column,
                "function": function,
            }
            for (to_column, function), from_columns in columns_lineage.items()
        ]
        payloads.append(
            {
                "edge": {
                    "fromEntity": _dataset_entity(source_fqn),
                    "toEntity": _dataset_entity(destination_fqn),
                    "lineageDetails": {
                        "source": "PipelineLineage",
                        "columnsLineage": sorted(
                            serialized_columns,
                            key=lambda item: (item["toColumn"], item["fromColumns"]),
                        ),
                    },
                }
            }
        )

    return payloads
