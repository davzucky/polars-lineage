from pathlib import Path

import polars as pl

from polars_lineage.config import MappingConfig
from polars_lineage.pipeline import extract_lineage_payloads_from_lazyframe, run_extraction_to_file


def test_e2e_extracts_payloads_from_lazyframe_projection() -> None:
    lazyframe = (
        pl.DataFrame({"a": [1, 2], "b": [3, 4]})
        .lazy()
        .select([pl.col("a").alias("x"), (pl.col("a") + pl.col("b")).alias("sum")])
    )
    mapping = MappingConfig(
        sources={"orders": "svc.db.raw.orders"},
        destination_table="svc.db.curated.metrics",
    )

    payloads = extract_lineage_payloads_from_lazyframe(lazyframe, mapping)

    assert len(payloads) == 1
    columns_lineage = payloads[0]["edge"]["lineageDetails"]["columnsLineage"]
    assert [item["toColumn"] for item in columns_lineage] == ["sum", "x"]
    assert columns_lineage[0]["fromColumns"] == ["a", "b"]


def test_e2e_writes_json_artifact(tmp_path: Path) -> None:
    lazyframe = pl.DataFrame({"a": [1], "b": [2]}).lazy().select([pl.col("a").alias("x")])
    mapping = MappingConfig(
        sources={"orders": "svc.db.raw.orders"},
        destination_table="svc.db.curated.metrics",
    )
    output_path = tmp_path / "lineage.json"

    run_extraction_to_file(lazyframe, mapping, output_path)

    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8").strip().startswith("[")


def test_e2e_extracts_join_and_aggregation_columns() -> None:
    left = pl.DataFrame({"id": [1, 2], "a": [10, 20]}).lazy()
    right = pl.DataFrame({"id": [1, 2], "b": [3, 4]}).lazy()
    lazyframe = (
        left.join(right, on="id", how="left")
        .with_columns((pl.col("a") + pl.col("b")).alias("total"))
        .group_by("id")
        .agg(pl.col("total").sum().alias("sum_total"))
    )
    mapping = MappingConfig(
        sources={"left": "svc.db.raw.left", "right": "svc.db.raw.right"},
        destination_table="svc.db.curated.final",
    )

    payloads = extract_lineage_payloads_from_lazyframe(lazyframe, mapping)

    to_columns = {
        item["toColumn"]
        for payload in payloads
        for item in payload["edge"]["lineageDetails"]["columnsLineage"]
    }
    assert "sum_total" in to_columns


def test_e2e_does_not_emit_self_lineage_for_literal_columns() -> None:
    lazyframe = pl.DataFrame({"a": [1, 2]}).lazy().select([pl.col("a"), pl.lit(1).alias("one")])
    mapping = MappingConfig(
        sources={"orders": "svc.db.raw.orders"},
        destination_table="svc.db.curated.metrics",
    )

    payloads = extract_lineage_payloads_from_lazyframe(lazyframe, mapping)

    for payload in payloads:
        for entry in payload["edge"]["lineageDetails"]["columnsLineage"]:
            assert not (entry["toColumn"] == "one" and entry["fromColumns"] == ["one"])
