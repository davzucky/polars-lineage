import pytest

from polars_lineage.ir import ColumnLineage, ColumnRef, DatasetRef
from polars_lineage.validation import validate_lineage


def _dataset() -> DatasetRef:
    return DatasetRef(service="svc", database="db", schema="public", table="orders")


def test_validate_lineage_accepts_valid_entries() -> None:
    dataset = _dataset()
    validate_lineage(
        [
            ColumnLineage(
                from_columns=(ColumnRef(dataset=dataset, column="a"),),
                to_column=ColumnRef(dataset=dataset, column="b"),
                function='col("a")',
                confidence="exact",
            )
        ]
    )


def test_validate_lineage_rejects_empty_list() -> None:
    with pytest.raises(ValueError):
        validate_lineage([])
