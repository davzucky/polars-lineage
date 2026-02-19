from polars_lineage.ir import ColumnLineage, ColumnRef, DatasetRef
from polars_lineage.resolver import resolve_transitive_lineage


def _dataset(name: str) -> DatasetRef:
    return DatasetRef(service="svc", database="db", schema="curated", table=name)


def test_resolver_flattens_transitive_dependencies() -> None:
    dataset = _dataset("orders")
    a = ColumnRef(dataset=dataset, column="a")
    b = ColumnRef(dataset=dataset, column="b")
    c = ColumnRef(dataset=dataset, column="c")
    d = ColumnRef(dataset=dataset, column="d")

    lineage = [
        ColumnLineage(from_columns=(a, b), to_column=c, function="add", confidence="exact"),
        ColumnLineage(from_columns=(c,), to_column=d, function="identity", confidence="exact"),
    ]

    resolved = resolve_transitive_lineage(lineage)
    assert [item.column for item in resolved[1].from_columns] == ["a", "b"]


def test_resolver_stops_on_cycles() -> None:
    dataset = _dataset("orders")
    a = ColumnRef(dataset=dataset, column="a")
    b = ColumnRef(dataset=dataset, column="b")

    lineage = [
        ColumnLineage(from_columns=(b,), to_column=a, function="f", confidence="exact"),
        ColumnLineage(from_columns=(a,), to_column=b, function="g", confidence="exact"),
    ]

    resolved = resolve_transitive_lineage(lineage)
    assert [item.column for item in resolved[0].from_columns] == ["b"]
    assert [item.column for item in resolved[1].from_columns] == ["a"]


def test_resolver_keeps_literal_columns_without_sources() -> None:
    dataset = _dataset("orders")
    literal_lineage = [
        ColumnLineage(
            from_columns=(),
            to_column=ColumnRef(dataset=dataset, column="one"),
            function="dyn int: 1",
            confidence="exact",
        )
    ]

    resolved = resolve_transitive_lineage(literal_lineage)
    assert resolved[0].from_columns == ()
