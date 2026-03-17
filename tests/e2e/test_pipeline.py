from datetime import date
import json

import polars as pl
import pytest

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


def _assert_static_today_scan_lineage(
    lazyframe: pl.LazyFrame, source_name: str, source_uri: str
) -> None:
    lazyframe = _lineage(lazyframe).add_source(
        name=source_name,
        uri=source_uri,
        destination_table="svc.db.curated.orders_parquet",
    )
    transformed = lazyframe.with_columns(pl.lit(date.today()).alias("loaded_at"))
    assert transformed.collect_schema().names() == ["id", "sku", "qty", "loaded_at"]
    payloads = _lineage(transformed).extract()
    assert len(payloads) == 1
    columns_lineage = payloads[0]["edge"]["lineageDetails"]["columnsLineage"]
    to_columns = {item["toColumn"] for item in columns_lineage}
    assert to_columns == {"id", "sku", "qty"}
    assert "loaded_at" not in to_columns


@pytest.mark.parametrize("scan_kind", ["csv", "parquet", "ndjson", "json"])
def test_e2e_scan_inputs_with_static_today_column_preserve_source_lineage(
    tmp_path,
    scan_kind: str,
) -> None:
    frame = pl.DataFrame({"id": [1], "sku": ["A"], "qty": [2]})

    if scan_kind == "csv":
        source_path = tmp_path / "orders.csv"
        source_path.write_text("id,sku,qty\n1,A,2\n", encoding="utf-8")
        lazyframe = pl.scan_csv(str(source_path))
    elif scan_kind == "parquet":
        source_path = tmp_path / "orders.parquet"
        frame.write_parquet(source_path)
        lazyframe = pl.scan_parquet(str(source_path))
    elif scan_kind == "ndjson":
        source_path = tmp_path / "orders.ndjson"
        frame.write_ndjson(source_path)
        lazyframe = pl.scan_ndjson(str(source_path))
    else:
        if not hasattr(pl, "scan_json"):
            pytest.skip("polars scan_json is not available in this version")
        source_path = tmp_path / "orders.json"
        source_path.write_text(json.dumps([{"id": 1, "sku": "A", "qty": 2}]), encoding="utf-8")
        lazyframe = pl.scan_json(str(source_path))

    _assert_static_today_scan_lineage(
        lazyframe=lazyframe,
        source_name=f"orders_{scan_kind}",
        source_uri=f"s3://warehouse/raw/orders.{scan_kind}",
    )
