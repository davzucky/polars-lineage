import polars as pl

from polars_lineage.lineage_lazyframe import LineageLazyFrame


def test_lazyframe_add_metadata_method_returns_lineage_wrapper() -> None:
    lazyframe = pl.DataFrame({"a": [1], "b": [2]}).lazy()
    wrapped = getattr(lazyframe, "add_metadata")(
        name="orders",
        uri="postgres://myserver/svc.db.raw.orders",
        destination_table="svc.db.curated.metrics",
    )

    assert isinstance(wrapped, LineageLazyFrame)
    assert wrapped.mapping.sources == {"orders": "svc.db.raw.orders"}
    assert wrapped.mapping.destination_table == "svc.db.curated.metrics"


def test_lineage_wrapper_preserves_metadata_across_transformations() -> None:
    wrapped = getattr(pl.DataFrame({"a": [1], "b": [2]}).lazy(), "add_metadata")(
        name="orders",
        uri="postgres://myserver/svc.db.raw.orders",
    )

    next_wrapped = wrapped.select(
        [
            pl.col("a").alias("x"),
            (pl.col("a") + pl.col("b")).alias("sum"),
        ]
    )

    assert isinstance(next_wrapped, LineageLazyFrame)
    assert next_wrapped.mapping == wrapped.mapping


def test_lazyframe_metadata_mode_supports_join_workflow() -> None:
    df_order = getattr(pl.DataFrame({"a": [1], "b": [2]}).lazy(), "add_metadata")(
        name="orders",
        uri="postgres://myserver/svc.db.raw.orders",
    )
    df_account = getattr(pl.DataFrame({"a": [1], "c": [3]}).lazy(), "add_metadata")(
        name="account",
        uri="https://account/list",
    )

    lineage = (
        df_account.join(df_order, on="a", how="inner")
        .select([(pl.col("a") + pl.col("b")).alias("sum")])
        .extract_lineage()
    )

    assert len(lineage) == 2
    from_entities = {item["edge"]["fromEntity"]["fullyQualifiedName"] for item in lineage}
    assert "svc.db.raw.orders" in from_entities
    assert "https.account.public.list" in from_entities
