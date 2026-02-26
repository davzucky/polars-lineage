from __future__ import annotations

from collections import defaultdict

from polars_lineage.config import MappingConfig
from polars_lineage.exporter.models import LineageDocument


def _escape_cell(value: str) -> str:
    return value.replace("\r", "").replace("\n", " ").replace("|", "\\|")


def _source_node_id(index: int) -> str:
    return f"source_{index}"


def _render_mermaid_flow(document: LineageDocument, mapping: MappingConfig | None) -> list[str]:
    sorted_sources = sorted({edge.source_table for edge in document.edges})
    lines: list[str] = [
        "```mermaid",
        "flowchart LR",
        '  destination["Destination\\n' + document.destination_table + '"]',
    ]
    source_roles = {fqn: alias for alias, fqn in mapping.sources.items()} if mapping else {}
    left_source = next(
        (source for source in sorted_sources if source_roles.get(source) == "left"), None
    )
    right_source = next(
        (source for source in sorted_sources if source_roles.get(source) == "right"), None
    )
    uses_join_node = left_source is not None and right_source is not None

    if uses_join_node:
        lines.append('  join_node{"JOIN"}')

    for index, source_table in enumerate(sorted_sources):
        source_node = _source_node_id(index)
        lines.append(f'  {source_node}["Source\\n{source_table}"]')
        if uses_join_node:
            role = source_roles.get(source_table)
            if role in {"left", "right"}:
                lines.append(f"  {source_node} -->|{role}| join_node")
            else:
                lines.append(f"  {source_node} --> join_node")
        else:
            lines.append(f"  {source_node} --> destination")

    if uses_join_node:
        lines.append("  join_node --> destination")

    lines.append("```")
    return lines


def _render_destination_column_table(document: LineageDocument) -> list[str]:
    from_columns_by_destination: dict[str, set[str]] = defaultdict(set)

    for edge in document.edges:
        for column in edge.columns:
            for source_column in column.from_columns:
                from_columns_by_destination[column.to_column].add(
                    f"{edge.source_table}.{source_column}"
                )

    lines: list[str] = [
        "| destination_column | source_columns |",
        "| --- | --- |",
    ]
    for destination_column in sorted(from_columns_by_destination):
        source_columns = ", ".join(sorted(from_columns_by_destination[destination_column]))
        lines.append(
            "| "
            + " | ".join([_escape_cell(destination_column), _escape_cell(source_columns)])
            + " |"
        )
    return lines


def export_lineage_markdown(document: LineageDocument, mapping: MappingConfig | None = None) -> str:
    lines: list[str] = ["# Lineage", "", f"Destination table: `{document.destination_table}`"]
    lines.extend(["", "## Data Flow", ""])
    lines.extend(_render_mermaid_flow(document, mapping))
    lines.extend(["", "## Destination Column Lineage", ""])
    lines.extend(_render_destination_column_table(document))

    for edge in document.edges:
        lines.extend(
            [
                "",
                f"## `{edge.source_table}` -> `{edge.destination_table}`",
                "",
                "| to_column | from_columns | function | confidence |",
                "| --- | --- | --- | --- |",
            ]
        )
        for column in edge.columns:
            from_columns = ", ".join(column.from_columns)
            lines.append(
                "| "
                + " | ".join(
                    [
                        _escape_cell(column.to_column),
                        _escape_cell(from_columns),
                        _escape_cell(column.function),
                        _escape_cell(column.confidence),
                    ]
                )
                + " |"
            )

    return "\n".join(lines) + "\n"
