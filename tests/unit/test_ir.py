import pytest
from pydantic import ValidationError

from polars_lineage.ir import ColumnLineage, ColumnRef, DatasetRef


def test_dataset_ref_fqn() -> None:
    dataset = DatasetRef(
        service="local",
        database="analytics",
        schema="public",
        table="orders",
    )

    assert dataset.fqn == "local.analytics.public.orders"


def test_dataset_ref_serializes_schema_alias() -> None:
    dataset = DatasetRef(
        service="local",
        database="analytics",
        schema="public",
        table="orders",
    )

    assert dataset.model_dump(by_alias=True)["schema"] == "public"


def test_column_lineage_normalizes_from_column_order() -> None:
    dataset = DatasetRef(
        service="local",
        database="analytics",
        schema="public",
        table="orders",
    )
    destination = ColumnRef(dataset=dataset, column="total")
    source_b = ColumnRef(dataset=dataset, column="price")
    source_a = ColumnRef(dataset=dataset, column="discount")

    lineage = ColumnLineage(
        from_columns=(source_b, source_a),
        to_column=destination,
        function="subtract",
        confidence="exact",
    )

    dumped_columns = [item.column for item in lineage.from_columns]
    assert dumped_columns == ["discount", "price"]


def test_dataset_ref_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        DatasetRef.model_validate(
            {
                "service": "local",
                "database": "analytics",
                "schema": "public",
                "table": "orders",
                "unexpected": "value",
            }
        )


def test_dataset_ref_rejects_duplicate_schema_keys() -> None:
    with pytest.raises(ValidationError):
        DatasetRef.model_validate(
            {
                "service": "local",
                "database": "analytics",
                "schema": "public",
                "schema_name": "other",
                "table": "orders",
            }
        )


def test_column_lineage_accepts_dict_payload_and_sorts() -> None:
    lineage = ColumnLineage.model_validate(
        {
            "from_columns": [
                {
                    "dataset": {
                        "service": "local",
                        "database": "analytics",
                        "schema": "public",
                        "table": "orders",
                    },
                    "column": "price",
                },
                {
                    "dataset": {
                        "service": "local",
                        "database": "analytics",
                        "schema": "public",
                        "table": "orders",
                    },
                    "column": "discount",
                },
            ],
            "to_column": {
                "dataset": {
                    "service": "local",
                    "database": "analytics",
                    "schema": "public",
                    "table": "orders",
                },
                "column": "total",
            },
            "function": "subtract",
            "confidence": "exact",
        }
    )

    assert [item.column for item in lineage.from_columns] == ["discount", "price"]
