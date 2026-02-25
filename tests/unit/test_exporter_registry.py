import pytest

from polars_lineage.config import MappingConfig
from polars_lineage.exporter.models import LineageDocument
from polars_lineage.exporter.registry import export_lineage
from polars_lineage.ir import ColumnLineage, ColumnRef, DatasetRef


def _dataset(table: str) -> DatasetRef:
    return DatasetRef(service="svc", database="db", schema="public", table=table)


def test_exporter_registry_routes_to_json_document() -> None:
    source = _dataset("orders")
    destination = _dataset("metrics")
    mapping = MappingConfig(
        sources={"orders": "svc.db.public.orders"},
        destination_table="svc.db.public.metrics",
    )
    lineage = [
        ColumnLineage(
            from_columns=(ColumnRef(dataset=source, column="a"),),
            to_column=ColumnRef(dataset=destination, column="x"),
            function='col("a")',
            confidence="exact",
        )
    ]

    output = export_lineage(lineage, mapping, output_format="json")

    assert isinstance(output, LineageDocument)
    assert output.destination_table == "svc.db.public.metrics"


def test_exporter_registry_routes_to_markdown() -> None:
    source = _dataset("orders")
    destination = _dataset("metrics")
    mapping = MappingConfig(
        sources={"orders": "svc.db.public.orders"},
        destination_table="svc.db.public.metrics",
    )
    lineage = [
        ColumnLineage(
            from_columns=(ColumnRef(dataset=source, column="a"),),
            to_column=ColumnRef(dataset=destination, column="x"),
            function='col("a")',
            confidence="exact",
        )
    ]

    output = export_lineage(lineage, mapping, output_format="markdown")

    assert isinstance(output, str)
    assert "# Lineage" in output


def test_exporter_registry_rejects_unknown_format() -> None:
    source = _dataset("orders")
    destination = _dataset("metrics")
    mapping = MappingConfig(
        sources={"orders": "svc.db.public.orders"},
        destination_table="svc.db.public.metrics",
    )
    lineage = [
        ColumnLineage(
            from_columns=(ColumnRef(dataset=source, column="a"),),
            to_column=ColumnRef(dataset=destination, column="x"),
            function='col("a")',
            confidence="exact",
        )
    ]

    with pytest.raises(ValueError, match="unsupported output format"):
        export_lineage(lineage, mapping, output_format="openlineage")  # type: ignore[arg-type]
