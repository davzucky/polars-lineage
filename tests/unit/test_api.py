import polars as pl
import pytest
from pydantic import ValidationError

from polars_lineage import extract_lazyframe_lineage as extract_from_root
from polars_lineage.api import extract_lazyframe_lineage
from polars_lineage.config import MappingConfig


def test_extract_lazyframe_lineage_accepts_mapping_config() -> None:
    lazyframe = (
        pl.DataFrame({"a": [1, 2], "b": [3, 4]})
        .lazy()
        .select([pl.col("a").alias("x"), (pl.col("a") + pl.col("b")).alias("sum")])
    )
    mapping = MappingConfig(
        sources={"orders": "svc.db.raw.orders"},
        destination_table="svc.db.curated.metrics",
    )

    payloads = extract_lazyframe_lineage(lazyframe, mapping)

    assert len(payloads) == 1
    assert payloads[0]["edge"]["toEntity"]["fullyQualifiedName"] == "svc.db.curated.metrics"


def test_extract_lazyframe_lineage_accepts_mapping_dict() -> None:
    lazyframe = (
        pl.DataFrame({"a": [1], "b": [2]}).lazy().select([(pl.col("a") + pl.col("b")).alias("sum")])
    )

    payloads = extract_lazyframe_lineage(
        lazyframe,
        {
            "sources": {"orders": "svc.db.raw.orders"},
            "destination_table": "svc.db.curated.metrics",
        },
    )

    assert payloads[0]["edge"]["lineageDetails"]["columnsLineage"][0]["toColumn"] == "sum"


def test_extract_lazyframe_lineage_rejects_invalid_mapping_dict() -> None:
    lazyframe = pl.DataFrame({"a": [1]}).lazy().select([pl.col("a")])

    with pytest.raises(ValidationError):
        extract_lazyframe_lineage(
            lazyframe,
            {
                "sources": {"orders": "svc.db.raw.orders"},
            },
        )


def test_extract_lazyframe_lineage_is_available_from_package_root() -> None:
    lazyframe = pl.DataFrame({"a": [1]}).lazy().select([pl.col("a")])

    payloads = extract_from_root(
        lazyframe,
        {
            "sources": {"orders": "svc.db.raw.orders"},
            "destination_table": "svc.db.curated.metrics",
        },
    )

    assert len(payloads) == 1
