import polars as pl

import polars_lineage
from polars_lineage.exporter.models import LineageDocument

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


def test_lineage_render_supports_markdown_and_json_outputs() -> None:
    lazyframe = _lineage(pl.DataFrame({"a": [1], "b": [2]}).lazy()).add_source(
        name="orders",
        uri="postgres://warehouse/svc.db.raw.orders",
        destination_table="svc.db.curated.metrics",
    )
    projected = lazyframe.select([(pl.col("a") + pl.col("b")).alias("sum")])

    markdown_output = _lineage(projected).render(format="markdown")
    json_output = _lineage(projected).render(format="json")

    assert isinstance(markdown_output, str)
    assert "svc.db.curated.metrics" in markdown_output
    assert "svc.db.raw.orders" in markdown_output

    assert isinstance(json_output, LineageDocument)
    assert json_output.destination_table == "svc.db.curated.metrics"


def test_join_requires_metadata_on_both_sides() -> None:
    left = _lineage(pl.DataFrame({"id": [1], "a": [10]}).lazy()).add_source(
        name="left",
        uri="postgres://warehouse/svc.db.raw.left",
    )
    right = pl.DataFrame({"id": [1], "b": [20]}).lazy()

    error = False
    try:
        _ = left.join(right, on="id", how="inner")
    except ValueError:
        error = True

    assert error


def test_join_rejects_multi_join_mapping() -> None:
    left = _lineage(pl.DataFrame({"id": [1], "a": [10]}).lazy()).add_source(
        name="left",
        uri="postgres://warehouse/svc.db.raw.left",
    )
    middle = _lineage(pl.DataFrame({"id": [1], "b": [20]}).lazy()).add_source(
        name="middle",
        uri="postgres://warehouse/svc.db.raw.middle",
    )
    right = _lineage(pl.DataFrame({"id": [1], "c": [30]}).lazy()).add_source(
        name="right",
        uri="postgres://warehouse/svc.db.raw.right",
    )

    error = False
    try:
        _ = left.join(middle, on="id", how="inner").join(right, on="id", how="inner")
    except ValueError:
        error = True

    assert error
