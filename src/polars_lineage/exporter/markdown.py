from __future__ import annotations

from polars_lineage.exporter.models import LineageDocument


def _escape_cell(value: str) -> str:
    return value.replace("\r", "").replace("\n", " ").replace("|", "\\|")


def _source_node_id(index: int) -> str:
    return f"source_{index}"


def _render_mermaid_flow(document: LineageDocument) -> list[str]:
    sorted_sources = sorted({edge.source_table for edge in document.edges})
    lines: list[str] = [
        "```mermaid",
        "flowchart LR",
        '  destination["Destination\\n' + document.destination_table + '"]',
    ]
    for index, source_table in enumerate(sorted_sources):
        source_node = _source_node_id(index)
        lines.append(f'  {source_node}["Source\\n{source_table}"]')
        lines.append(f"  {source_node} --> destination")
    lines.append("```")
    return lines


def export_lineage_markdown(document: LineageDocument) -> str:
    lines: list[str] = ["# Lineage", "", f"Destination table: `{document.destination_table}`"]
    lines.extend(["", "## Data Flow", ""])
    lines.extend(_render_mermaid_flow(document))

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
