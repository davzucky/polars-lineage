from polars_lineage.exporter.openmetadata import export_openmetadata_requests
from polars_lineage.ir import ColumnLineage, ColumnRef, DatasetRef


def _dataset(table: str) -> DatasetRef:
    return DatasetRef(service="svc", database="db", schema="public", table=table)


def test_exporter_groups_by_table_edge() -> None:
    source = _dataset("orders")
    destination = _dataset("metrics")
    lineage = [
        ColumnLineage(
            from_columns=(ColumnRef(dataset=source, column="a"),),
            to_column=ColumnRef(dataset=destination, column="x"),
            function='col("a")',
            confidence="exact",
        ),
        ColumnLineage(
            from_columns=(ColumnRef(dataset=source, column="b"),),
            to_column=ColumnRef(dataset=destination, column="y"),
            function='col("b")',
            confidence="exact",
        ),
    ]

    payloads = export_openmetadata_requests(lineage)

    assert len(payloads) == 1
    columns_lineage = payloads[0]["edge"]["lineageDetails"]["columnsLineage"]
    assert [item["toColumn"] for item in columns_lineage] == ["x", "y"]


def test_exporter_splits_multiple_source_tables() -> None:
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
            confidence="exact",
        )
    ]

    payloads = export_openmetadata_requests(lineage)

    assert len(payloads) == 2
    from_entities = {item["edge"]["fromEntity"]["fullyQualifiedName"] for item in payloads}
    assert from_entities == {"svc.db.public.left", "svc.db.public.right"}


def test_exporter_combines_from_columns_for_same_edge_and_destination() -> None:
    source = _dataset("orders")
    destination = _dataset("metrics")
    lineage = [
        ColumnLineage(
            from_columns=(
                ColumnRef(dataset=source, column="a"),
                ColumnRef(dataset=source, column="b"),
            ),
            to_column=ColumnRef(dataset=destination, column="sum"),
            function="add",
            confidence="exact",
        )
    ]

    payloads = export_openmetadata_requests(lineage)

    assert len(payloads) == 1
    columns_lineage = payloads[0]["edge"]["lineageDetails"]["columnsLineage"]
    assert columns_lineage[0]["fromColumns"] == ["a", "b"]
