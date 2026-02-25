from __future__ import annotations

from polars_lineage.exporter.models import LineageDocument


def _escape_cell(value: str) -> str:
    return value.replace("\r", "").replace("\n", " ").replace("|", "\\|")


def export_lineage_markdown(document: LineageDocument) -> str:
    lines: list[str] = ["# Lineage", "", f"Destination table: `{document.destination_table}`"]

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
