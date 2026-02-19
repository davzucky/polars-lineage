from __future__ import annotations

from polars_lineage.ir import ColumnLineage, ColumnRef


def _key(column: ColumnRef) -> str:
    return f"{column.dataset.fqn}.{column.column}"


def resolve_transitive_lineage(lineage: list[ColumnLineage]) -> list[ColumnLineage]:
    lineage_by_target = {_key(item.to_column): item for item in lineage}

    def resolve_sources(column: ColumnRef, seen: set[str]) -> tuple[ColumnRef, ...]:
        column_key = _key(column)
        if column_key in seen:
            return (column,)
        current = lineage_by_target.get(column_key)
        if current is None:
            return (column,)
        if not current.from_columns:
            return ()

        next_seen = seen | {column_key}
        resolved: set[tuple[str, str]] = set()
        flattened: list[ColumnRef] = []
        for source in current.from_columns:
            for leaf in resolve_sources(source, next_seen):
                if _key(leaf) == column_key:
                    leaf = source
                dedupe_key = (leaf.dataset.fqn, leaf.column)
                if dedupe_key in resolved:
                    continue
                resolved.add(dedupe_key)
                flattened.append(leaf)
        return tuple(
            sorted(
                flattened,
                key=lambda item: (item.dataset.fqn, item.column),
            )
        )

    resolved_lineage: list[ColumnLineage] = []
    for item in lineage:
        resolved_lineage.append(
            item.model_copy(update={"from_columns": resolve_sources(item.to_column, set())})
        )

    return sorted(
        resolved_lineage,
        key=lambda item: (item.to_column.dataset.fqn, item.to_column.column),
    )
