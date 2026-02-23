import polars as pl
from pydantic import ValidationError

from polars_lineage.lineage_lazyframe import LineageLazyFrame, with_lineage


def test_with_lineage_wraps_lazyframe_and_keeps_mapping() -> None:
    lazyframe = pl.DataFrame({"a": [1], "b": [2]}).lazy()

    wrapped = with_lineage(
        lazyframe,
        {
            "sources": {"orders": "svc.db.raw.orders"},
            "destination_table": "svc.db.curated.metrics",
        },
    )

    assert isinstance(wrapped, LineageLazyFrame)
    assert wrapped.mapping.destination_table == "svc.db.curated.metrics"


def test_lineage_wrapper_preserves_metadata_across_transformations() -> None:
    wrapped = with_lineage(
        pl.DataFrame({"a": [1], "b": [2]}).lazy(),
        {
            "sources": {"orders": "svc.db.raw.orders"},
            "destination_table": "svc.db.curated.metrics",
        },
    )

    next_wrapped = wrapped.select(
        [
            pl.col("a").alias("x"),
            (pl.col("a") + pl.col("b")).alias("sum"),
        ]
    )

    assert isinstance(next_wrapped, LineageLazyFrame)
    assert next_wrapped.mapping == wrapped.mapping


def test_lineage_wrapper_extract_returns_payloads() -> None:
    wrapped = with_lineage(
        pl.DataFrame({"a": [1], "b": [2]})
        .lazy()
        .select([(pl.col("a") + pl.col("b")).alias("sum")]),
        {
            "sources": {"orders": "svc.db.raw.orders"},
            "destination_table": "svc.db.curated.metrics",
        },
    )

    payloads = wrapped.extract_lineage()

    assert len(payloads) == 1
    assert payloads[0]["edge"]["toEntity"]["fullyQualifiedName"] == "svc.db.curated.metrics"


def test_with_lineage_rejects_invalid_mapping() -> None:
    with_error = False
    try:
        with_lineage(
            pl.DataFrame({"a": [1]}).lazy(),
            {
                "sources": {"orders": "svc.db.raw.orders"},
            },
        )
    except ValidationError:
        with_error = True

    assert with_error


def test_lazyframe_add_metadata_method_returns_lineage_wrapper() -> None:
    lazyframe = pl.DataFrame({"a": [1], "b": [2]}).lazy()
    wrapped = getattr(lazyframe, "add_metadata")(
        source="svc.db.raw.orders",
        destination_table="svc.db.curated.metrics",
    )

    assert isinstance(wrapped, LineageLazyFrame)
    assert wrapped.mapping.sources == {"source": "svc.db.raw.orders"}


def test_lazyframe_add_metadata_supports_multiple_sources() -> None:
    lazyframe = pl.DataFrame({"a": [1], "b": [2]}).lazy()
    wrapped = getattr(lazyframe, "add_metadata")(
        sources={
            "left": "svc.db.raw.left_table",
            "right": "svc.db.raw.right_table",
        },
        destination_table="svc.db.curated.metrics",
    )

    assert wrapped.mapping.sources["left"] == "svc.db.raw.left_table"


def test_lazyframe_metadata_mode_supports_join_workflow() -> None:
    df_order = getattr(
        pl.DataFrame({"a": [1], "b": [2]}).lazy(),
        "add_metadata",
    )(
        name="orders",
        source_type="postgress",
        source_url="postgress://myserver/svc.db.raw.orders",
    )
    df_account = getattr(
        pl.DataFrame({"a": [1], "c": [3]}).lazy(),
        "add_metadata",
    )(
        name="account",
        source_type="rest",
        source_url="https://account/list",
    )

    lineage = (
        df_account.join(df_order, on="a", how="inner")
        .select([(pl.col("a") + pl.col("b")).alias("sum")])
        .extract_lineage()
    )

    assert len(lineage) == 2
    from_entities = {item["edge"]["fromEntity"]["fullyQualifiedName"] for item in lineage}
    assert "svc.db.raw.orders" in from_entities
    assert "rest.account.public.list" in from_entities
