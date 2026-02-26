from polars_lineage.config import MappingConfig
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
    assert "## Data Flow" in markdown
    assert "```mermaid" in markdown
    assert 'source_0["Source\\nsvc.db.public.orders"]' in markdown
    assert "source_0 --> destination" in markdown
    assert "## Destination Column Lineage" in markdown
    assert "| destination_column | source_columns |" in markdown
    assert "| sum | svc.db.public.orders.a, svc.db.public.orders.b |" in markdown
    assert "to_column | from_columns | function | confidence" not in markdown


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
                        from_columns=["a|b"],
                        function='col("a|b")',
                        confidence="inferred",
                    )
                ],
            )
        ],
    )

    markdown = export_lineage_markdown(document)

    assert "svc.db.public.orders.a\\|b" in markdown


def test_markdown_exporter_sanitizes_newlines_in_cells() -> None:
    document = LineageDocument(
        destination_table="svc.db.public.metrics",
        edges=[
            LineageEdge(
                source_table="svc.db.public.orders",
                destination_table="svc.db.public.metrics",
                columns=[
                    LineageColumn(
                        to_column="flag\nnew",
                        from_columns=["a"],
                        function='col("a")',
                        confidence="inferred",
                    )
                ],
            )
        ],
    )

    markdown = export_lineage_markdown(document)

    assert "flag new" in markdown


def test_markdown_exporter_renders_join_information_in_mermaid_flow() -> None:
    document = LineageDocument(
        destination_table="svc.db.public.joined",
        edges=[
            LineageEdge(
                source_table="svc.db.public.left",
                destination_table="svc.db.public.joined",
                columns=[
                    LineageColumn(
                        to_column="total",
                        from_columns=["a"],
                        function='col("a")',
                        confidence="exact",
                    )
                ],
            ),
            LineageEdge(
                source_table="svc.db.public.right",
                destination_table="svc.db.public.joined",
                columns=[
                    LineageColumn(
                        to_column="total",
                        from_columns=["b"],
                        function='col("b")',
                        confidence="exact",
                    )
                ],
            ),
        ],
    )
    mapping = MappingConfig(
        sources={"left": "svc.db.public.left", "right": "svc.db.public.right"},
        destination_table="svc.db.public.joined",
    )

    markdown = export_lineage_markdown(document, mapping)

    assert 'join_node{"JOIN"}' in markdown
    assert "-->|left| join_node" in markdown
    assert "-->|right| join_node" in markdown
    assert "join_node --> destination" in markdown


def test_markdown_exporter_accepts_mapping_dict_for_join_mermaid_flow() -> None:
    document = LineageDocument(
        destination_table="svc.db.public.joined",
        edges=[
            LineageEdge(
                source_table="svc.db.public.left",
                destination_table="svc.db.public.joined",
                columns=[
                    LineageColumn(
                        to_column="total",
                        from_columns=["a"],
                        function='col("a")',
                        confidence="exact",
                    )
                ],
            ),
            LineageEdge(
                source_table="svc.db.public.right",
                destination_table="svc.db.public.joined",
                columns=[
                    LineageColumn(
                        to_column="total",
                        from_columns=["b"],
                        function='col("b")',
                        confidence="exact",
                    )
                ],
            ),
        ],
    )
    mapping = {
        "sources": {"left": "svc.db.public.left", "right": "svc.db.public.right"},
        "destination_table": "svc.db.public.joined",
    }

    markdown = export_lineage_markdown(document, mapping)

    assert 'join_node{"JOIN"}' in markdown
    assert "-->|left| join_node" in markdown
    assert "-->|right| join_node" in markdown
    assert "join_node --> destination" in markdown


def test_markdown_exporter_renders_destination_with_empty_source_columns() -> None:
    document = LineageDocument(
        destination_table="svc.db.public.metrics",
        edges=[
            LineageEdge(
                source_table="svc.db.public.orders",
                destination_table="svc.db.public.metrics",
                columns=[
                    LineageColumn(
                        to_column="constant_value",
                        from_columns=[],
                        function="lit(1)",
                        confidence="unknown",
                    )
                ],
            )
        ],
    )

    markdown = export_lineage_markdown(document)

    assert "| constant_value |  |" in markdown


def test_markdown_exporter_renders_self_join_roles_in_mermaid_flow() -> None:
    document = LineageDocument(
        destination_table="svc.db.public.joined",
        edges=[
            LineageEdge(
                source_table="svc.db.public.events",
                destination_table="svc.db.public.joined",
                columns=[
                    LineageColumn(
                        to_column="value",
                        from_columns=["left_value", "right_value"],
                        function="coalesce",
                        confidence="inferred",
                    )
                ],
            )
        ],
    )
    mapping = MappingConfig(
        sources={"left": "svc.db.public.events", "right": "svc.db.public.events"},
        destination_table="svc.db.public.joined",
    )

    markdown = export_lineage_markdown(document, mapping)

    assert 'join_node{"JOIN"}' in markdown
    assert "source_0 -->|left| join_node" in markdown
    assert "source_1 -->|right| join_node" in markdown
    assert 'source_0["Source\\nsvc.db.public.events"]' in markdown
    assert 'source_1["Source\\nsvc.db.public.events"]' in markdown
