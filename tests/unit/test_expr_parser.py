from polars_lineage.extractor.expr_parser import parse_expression


def test_parse_expression_handles_direct_column() -> None:
    parsed = parse_expression('col("amount")')

    assert parsed.columns == ("amount",)
    assert parsed.function == 'col("amount")'
    assert parsed.confidence == "exact"


def test_parse_expression_handles_arithmetic_expression() -> None:
    parsed = parse_expression('[(col("a")) + (col("b"))]')

    assert parsed.columns == ("a", "b")
    assert parsed.function == '[(col("a")) + (col("b"))]'
    assert parsed.confidence == "exact"


def test_parse_expression_handles_literals() -> None:
    parsed = parse_expression("dyn int: 1")

    assert parsed.columns == ()
    assert parsed.confidence == "exact"


def test_parse_expression_marks_unknown_patterns() -> None:
    parsed = parse_expression('some_custom_udf(col("a"))')

    assert parsed.columns == ("a",)
    assert parsed.confidence == "inferred"


def test_parse_expression_marks_unknown_when_no_columns() -> None:
    parsed = parse_expression("opaque_udf()")

    assert parsed.columns == ()
    assert parsed.confidence == "unknown"


def test_parse_expression_handles_when_then_otherwise() -> None:
    parsed = parse_expression('when(col("a") > dyn int: 0).then(col("b")).otherwise(col("c"))')

    assert parsed.columns == ("a", "b", "c")
    assert parsed.confidence == "exact"
