import polars as pl

import polars_lineage

_ = polars_lineage.__version__


def _lineage(lazyframe: pl.LazyFrame):
    return getattr(lazyframe, "lineage")


def test_e2e_extracts_payloads_from_lazyframe_projection() -> None:
    lazyframe = _lineage(pl.DataFrame({"a": [1, 2], "b": [3, 4]}).lazy()).add_source(
        name="orders",
        uri="postgres://warehouse/svc.db.raw.orders",
        destination_table="svc.db.curated.metrics",
    )
    projected = lazyframe.select([pl.col("a").alias("x"), (pl.col("a") + pl.col("b")).alias("sum")])

    payloads = _lineage(projected).extract()

    assert len(payloads) == 1
    columns_lineage = payloads[0]["edge"]["lineageDetails"]["columnsLineage"]
    assert [item["toColumn"] for item in columns_lineage] == ["sum", "x"]
    assert columns_lineage[0]["fromColumns"] == ["a", "b"]


def test_e2e_extracts_join_and_aggregation_columns() -> None:
    left = _lineage(pl.DataFrame({"id": [1, 2], "a": [10, 20]}).lazy()).add_source(
        name="left",
        uri="postgres://warehouse/svc.db.raw.left",
    )
    right = _lineage(pl.DataFrame({"id": [1, 2], "b": [3, 4]}).lazy()).add_source(
        name="right",
        uri="postgres://warehouse/svc.db.raw.right",
    )
    lazyframe = (
        left.join(right, on="id", how="left")
        .with_columns((pl.col("a") + pl.col("b")).alias("total"))
        .group_by("id")
        .agg(pl.col("total").sum().alias("sum_total"))
    )

    payloads = _lineage(lazyframe).extract()

    to_columns = {
        item["toColumn"]
        for payload in payloads
        for item in payload["edge"]["lineageDetails"]["columnsLineage"]
    }
    assert "sum_total" in to_columns


def test_e2e_does_not_emit_self_lineage_for_literal_columns() -> None:
    lazyframe = _lineage(pl.DataFrame({"a": [1, 2]}).lazy()).add_source(
        name="orders",
        uri="postgres://warehouse/svc.db.raw.orders",
    )
    projected = lazyframe.select([pl.col("a"), pl.lit(1).alias("one")])

    payloads = _lineage(projected).extract()

    for payload in payloads:
        for entry in payload["edge"]["lineageDetails"]["columnsLineage"]:
            assert not (entry["toColumn"] == "one" and entry["fromColumns"] == ["one"])
