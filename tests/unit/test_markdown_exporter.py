from polars_lineage.exporter.markdown import export_lineage_markdown
from polars_lineage.exporter.models import LineageColumn, LineageDocument, LineageEdge


def test_markdown_exporter_renders_table_sections() -> None:
    document = LineageDocument(
        destination_table="svc.db.public.metrics",
        edges=[
            LineageEdge(
                source_table="svc.db.public.orders",
                destination_table="svc.db.public.metrics",
                columns=[
                    LineageColumn(
                        to_column="sum",
                        from_columns=["a", "b"],
                        function='[(col("a")) + (col("b"))]',
                        confidence="exact",
                    ),
                    LineageColumn(
                        to_column="x",
                        from_columns=["a"],
                        function='col("a")',
                        confidence="exact",
                    ),
                ],
            )
        ],
    )

    markdown = export_lineage_markdown(document)

    assert "# Lineage" in markdown
    assert "Destination table: `svc.db.public.metrics`" in markdown
    assert "## `svc.db.public.orders` -> `svc.db.public.metrics`" in markdown
    assert "| to_column | from_columns | function | confidence |" in markdown
    assert '| sum | a, b | [(col("a")) + (col("b"))] | exact |' in markdown


def test_markdown_exporter_escapes_pipe_characters() -> None:
    document = LineageDocument(
        destination_table="svc.db.public.metrics",
        edges=[
            LineageEdge(
                source_table="svc.db.public.orders",
                destination_table="svc.db.public.metrics",
                columns=[
                    LineageColumn(
                        to_column="flag",
                        from_columns=["a"],
                        function="when(a|b)",
                        confidence="inferred",
                    )
                ],
            )
        ],
    )

    markdown = export_lineage_markdown(document)

    assert "when(a\\|b)" in markdown


def test_markdown_exporter_sanitizes_newlines_in_cells() -> None:
    document = LineageDocument(
        destination_table="svc.db.public.metrics",
        edges=[
            LineageEdge(
                source_table="svc.db.public.orders",
                destination_table="svc.db.public.metrics",
                columns=[
                    LineageColumn(
                        to_column="flag",
                        from_columns=["a"],
                        function="line1\nline2\r",
                        confidence="inferred",
                    )
                ],
            )
        ],
    )

    markdown = export_lineage_markdown(document)

    assert "line1 line2" in markdown
