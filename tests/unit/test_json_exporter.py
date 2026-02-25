import pytest

from polars_lineage.exporter.json import export_lineage_document
from polars_lineage.ir import ColumnLineage, ColumnRef, DatasetRef


def _dataset(table: str) -> DatasetRef:
    return DatasetRef(service="svc", database="db", schema="public", table=table)


def test_json_exporter_groups_by_edge_and_sorts_entries() -> None:
    source = _dataset("orders")
    destination = _dataset("metrics")
    lineage = [
        ColumnLineage(
            from_columns=(ColumnRef(dataset=source, column="b"),),
            to_column=ColumnRef(dataset=destination, column="sum"),
            function='col("b")',
            confidence="exact",
        ),
        ColumnLineage(
            from_columns=(ColumnRef(dataset=source, column="a"),),
            to_column=ColumnRef(dataset=destination, column="sum"),
            function='col("a")',
            confidence="exact",
        ),
        ColumnLineage(
            from_columns=(ColumnRef(dataset=source, column="a"),),
            to_column=ColumnRef(dataset=destination, column="x"),
            function='col("a")',
            confidence="exact",
        ),
    ]

    document = export_lineage_document(lineage, destination_table="svc.db.public.metrics")

    assert document.destination_table == "svc.db.public.metrics"
    assert len(document.edges) == 1
    assert document.edges[0].source_table == "svc.db.public.orders"
    assert [column.to_column for column in document.edges[0].columns] == ["sum", "sum", "x"]
    sum_columns = [column for column in document.edges[0].columns if column.to_column == "sum"]
    assert sum_columns[0].from_columns == ["a"]
    assert sum_columns[1].from_columns == ["b"]


def test_json_exporter_splits_source_tables_and_merges_from_columns() -> None:
    left = _dataset("left")
    right = _dataset("right")
    destination = _dataset("joined")
    lineage = [
        ColumnLineage(
            from_columns=(
                ColumnRef(dataset=left, column="a"),
                ColumnRef(dataset=right, column="b"),
            ),
            to_column=ColumnRef(dataset=destination, column="total"),
            function="add",
            confidence="inferred",
        ),
        ColumnLineage(
            from_columns=(ColumnRef(dataset=left, column="c"),),
            to_column=ColumnRef(dataset=destination, column="total"),
            function="add",
            confidence="inferred",
        ),
    ]

    document = export_lineage_document(lineage, destination_table="svc.db.public.joined")

    assert [edge.source_table for edge in document.edges] == [
        "svc.db.public.left",
        "svc.db.public.right",
    ]
    left_columns = document.edges[0].columns
    assert left_columns[0].from_columns == ["a", "c"]


def test_json_exporter_rejects_mismatched_destination_table() -> None:
    source = _dataset("orders")
    destination = _dataset("metrics")
    lineage = [
        ColumnLineage(
            from_columns=(ColumnRef(dataset=source, column="a"),),
            to_column=ColumnRef(dataset=destination, column="x"),
            function='col("a")',
            confidence="exact",
        )
    ]

    with pytest.raises(ValueError, match="destination_table"):
        export_lineage_document(lineage, destination_table="svc.db.public.other")
