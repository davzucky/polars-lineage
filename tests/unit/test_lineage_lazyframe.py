import polars as pl

import polars_lineage

_ = polars_lineage.__version__


def _lineage(lazyframe: pl.LazyFrame):
    return getattr(lazyframe, "lineage")


def test_lazyframe_lineage_add_source_sets_mapping() -> None:
    lazyframe = pl.DataFrame({"a": [1], "b": [2]}).lazy()
    with_source = _lineage(lazyframe).add_source(
        name="orders",
        uri="postgres://myserver/svc.db.raw.orders",
        destination_table="svc.db.curated.metrics",
    )

    assert isinstance(with_source, pl.LazyFrame)


def test_lineage_namespace_preserves_metadata_across_transformations() -> None:
    lazyframe = _lineage(pl.DataFrame({"a": [1], "b": [2]}).lazy()).add_source(
        name="orders",
        uri="postgres://myserver/svc.db.raw.orders",
    )

    lineage_frame = lazyframe.select(
        [
            pl.col("a").alias("x"),
            (pl.col("a") + pl.col("b")).alias("sum"),
        ]
    )
    lineage = _lineage(lineage_frame).extract()

    assert len(lineage) == 1
    columns = lineage[0]["edge"]["lineageDetails"]["columnsLineage"]
    assert [item["toColumn"] for item in columns] == ["sum", "x"]


def test_lineage_namespace_supports_join_workflow() -> None:
    df_order = _lineage(pl.DataFrame({"a": [1], "b": [2]}).lazy()).add_source(
        name="orders",
        uri="postgres://myserver/svc.db.raw.orders",
    )
    df_account = _lineage(pl.DataFrame({"a": [1], "c": [3]}).lazy()).add_source(
        name="account",
        uri="https://account/list",
    )

    lineage_frame = df_account.join(df_order, on="a", how="inner").select(
        [(pl.col("a") + pl.col("b")).alias("sum")]
    )
    lineage = _lineage(lineage_frame).extract()

    assert len(lineage) == 2
    from_entities = {item["edge"]["fromEntity"]["fullyQualifiedName"] for item in lineage}
    assert "svc.db.raw.orders" in from_entities
    assert "https.account.public.list" in from_entities


def test_lineage_add_source_rejects_blank_name_or_uri() -> None:
    lazyframe = pl.DataFrame({"a": [1]}).lazy()

    name_error = False
    try:
        _ = _lineage(lazyframe).add_source(name="   ", uri="postgres://warehouse/orders")
    except ValueError:
        name_error = True

    uri_error = False
    try:
        _ = _lineage(lazyframe).add_source(name="orders", uri="   ")
    except ValueError:
        uri_error = True

    assert name_error
    assert uri_error


def test_lineage_extract_requires_metadata() -> None:
    lazyframe = pl.DataFrame({"a": [1]}).lazy()

    error = False
    try:
        _ = _lineage(lazyframe.select(pl.col("a"))).extract()
    except ValueError:
        error = True

    assert error
