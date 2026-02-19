from polars_lineage.config import MappingConfig
from polars_lineage.extractor.explain_tree import extract_plan_lineage

SELECT_PLAN = """
              0                        1                             2                          3
   ┌────────────────────────────────────────────────────────────────────────────────────────────────────────
   │
   │      ╭────────╮
 0 │      │ SELECT │
   │      ╰───┬┬───╯
   │          ││
   │          │╰───────────────────────┬─────────────────────────────┬──────────────────────────╮
   │          │                        │                             │                          │
   │  ╭───────┴───────╮  ╭─────────────┴─────────────╮  ╭────────────┴────────────╮  ╭──────────┴──────────╮
   │  │ expression:   │  │ expression:               │  │ expression:             │  │ FROM:               │
 1 │  │ col("a")      │  │ [(col("a")) + (col("b"))] │  │ dyn int: 1.alias("one") │  │ DF ["a", "b"]       │
   │  │   .alias("x") │  │   .alias("sum")           │  ╰─────────────────────────╯  │ PROJECT */2 COLUMNS │
   │  ╰───────────────╯  ╰───────────────────────────╯                               ╰─────────────────────╯
"""


JOIN_PLAN = """
                           0                              1                   2                   3                   4
   ┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   │
   │                ╭──────────────╮
 0 │                │ WITH_COLUMNS │
   │                ╰──────┬┬──────╯
   │                       ││
   │                       │╰─────────────────────────────╮
   │                       │                              │
   │  ╭────────────────────┴─────────────────────╮        │
   │  │ expression:                              │  ╭─────┴─────╮
 1 │  │ [(col("a")) + (col("b").fill_null([0]))] │  │ LEFT JOIN │
   │  │   .alias("total")                        │  ╰─────┬┬────╯
   │  ╰──────────────────────────────────────────╯        ││
   │                                                      ││
   │                                                      │╰──────────────────┬───────────────────┬───────────────────╮
   │                                                      │                   │                   │                   │
   │                                                ╭─────┴─────╮  ╭──────────┴──────────╮  ╭─────┴─────╮  ╭──────────┴──────────╮
   │                                                │ left on:  │  │ LEFT PLAN:          │  │ right on: │  │ RIGHT PLAN:         │
 2 │                                                │ col("id") │  │ DF ["id", "a"]      │  │ col("id") │  │ DF ["id", "b"]      │
   │                                                ╰───────────╯  │ PROJECT */2 COLUMNS │  ╰───────────╯  │ PROJECT */2 COLUMNS │
   │                                                               ╰─────────────────────╯                 ╰─────────────────────╯
"""


AGG_PLAN = """
                       0                              1                     2                   3                     4
   ┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   │
   │  ╭──────────────────────────────────╮
 0 │  │ AGGREGATE[maintain_order: false] │
   │  ╰────────────────┬┬────────────────╯
   │                   ││
   │                   │╰─────────────────────────────┬─────────────────────┬───────────────────┬─────────────────────╮
   │                   │                              │                     │                   │                     │
   │         ╭─────────┴─────────╮          ╭─────────┴─────────╮           │                   │                     │
   │         │ expression:       │          │ expression:       │  ╭────────┴────────╮  ╭───────┴───────╮  ╭──────────┴──────────╮
   │         │ col("v")          │          │ col("w")          │  │ expression:     │  │ aggregate by: │  │ FROM:               │
 1 │         │   .sum()          │          │   .mean()         │  │ len()           │  │ col("k")      │  │ DF ["k", "v", "w"]  │
   │         │   .alias("sum_v") │          │   .alias("avg_w") │  │   .alias("cnt") │  ╰───────────────╯  │ PROJECT */3 COLUMNS │
   │         ╰───────────────────╯          ╰───────────────────╯  ╰─────────────────╯                     ╰─────────────────────╯
"""


OVERLAP_JOIN_PLAN = """
                           0                              1                   2                   3                   4
   ┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   │
   │                ╭──────────────╮
 0 │                │ WITH_COLUMNS │
   │                ╰──────┬┬──────╯
   │                       ││
   │                       │╰─────────────────────────────╮
   │                       │                              │
   │  ╭────────────────────┴─────────────────────╮        │
   │  │ expression:                              │  ╭─────┴─────╮
 1 │  │ col("id")                               │  │ LEFT JOIN │
   │  │   .alias("joined_id")                   │  ╰─────┬┬────╯
   │  ╰──────────────────────────────────────────╯        ││
   │                                                      ││
   │                                                      │╰──────────────────┬───────────────────┬───────────────────╮
   │                                                      │                   │                   │                   │
   │                                                ╭─────┴─────╮  ╭──────────┴──────────╮  ╭─────┴─────╮  ╭──────────┴──────────╮
   │                                                │ left on:  │  │ LEFT PLAN:          │  │ right on: │  │ RIGHT PLAN:         │
 2 │                                                │ col("id") │  │ DF ["id", "a"]      │  │ col("id") │  │ DF ["id", "b"]      │
   │                                                ╰───────────╯  │ PROJECT */2 COLUMNS │  ╰───────────╯  │ PROJECT */2 COLUMNS │
   │                                                               ╰─────────────────────╯                 ╰─────────────────────╯
"""


AMBIGUOUS_JOIN_PLAN = """
                           0                              1                   2                   3                   4
   ┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   │
   │                ╭──────────────╮
 0 │                │ WITH_COLUMNS │
   │                ╰──────┬┬──────╯
   │                       ││
   │                       │╰─────────────────────────────╮
   │                       │                              │
   │  ╭────────────────────┴─────────────────────╮        │
   │  │ expression:                              │  ╭─────┴─────╮
 1 │  │ col("value")                            │  │ LEFT JOIN │
   │  │   .alias("picked")                      │  ╰─────┬┬────╯
   │  ╰──────────────────────────────────────────╯        ││
   │                                                      ││
   │                                                      │╰──────────────────┬───────────────────┬───────────────────╮
   │                                                      │                   │                   │                   │
   │                                                ╭─────┴─────╮  ╭──────────┴──────────╮  ╭─────┴─────╮  ╭──────────┴──────────╮
   │                                                │ left on:  │  │ LEFT PLAN:          │  │ right on: │  │ RIGHT PLAN:         │
 2 │                                                │ col("id") │  │ DF ["id", "value"]  │  │ col("id") │  │ DF ["id", "value"]  │
   │                                                ╰───────────╯  │ PROJECT */2 COLUMNS │  ╰───────────╯  │ PROJECT */2 COLUMNS │
   │                                                               ╰─────────────────────╯                 ╰─────────────────────╯
"""


MULTIKEY_OVERLAP_JOIN_PLAN = """
                           0                              1                   2                   3                   4
   ┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   │
   │                ╭──────────────╮
 0 │                │ WITH_COLUMNS │
   │                ╰──────┬┬──────╯
   │                       ││
   │                       │╰─────────────────────────────╮
   │                       │                              │
   │  ╭────────────────────┴─────────────────────╮        │
   │  │ expression:                              │  ╭─────┴─────╮
 1 │  │ col("id")                               │  │ LEFT JOIN │
   │  │   .alias("joined_id")                   │  ╰─────┬┬────╯
   │  ╰──────────────────────────────────────────╯        ││
   │                                                      ││
   │                                                      │╰──────────────────┬───────────────────┬───────────────────╮
   │                                                      │                   │                   │                   │
   │                                                ╭─────┴─────╮  ╭──────────┴──────────╮  ╭─────┴─────╮  ╭──────────┴──────────╮
   │                                                │ left on:  │  │ LEFT PLAN:          │  │ right on: │  │ RIGHT PLAN:         │
 2 │                                                │ [col("id"), col("dt")] │  │ DF ["id", "dt", "a"] │  │ [col("id"), col("dt")] │  │ DF ["id", "dt", "b"] │
   │                                                ╰───────────╯  │ PROJECT */3 COLUMNS │  ╰───────────╯  │ PROJECT */3 COLUMNS │
   │                                                               ╰─────────────────────╯                 ╰─────────────────────╯
"""


ASYMMETRIC_JOIN_KEY_PLAN = """
                           0                              1                   2                   3                   4
   ┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   │
   │                ╭──────────────╮
 0 │                │ WITH_COLUMNS │
   │                ╰──────┬┬──────╯
   │                       ││
   │                       │╰─────────────────────────────╮
   │                       │                              │
   │  ╭────────────────────┴─────────────────────╮        │
   │  │ expression:                              │  ╭─────┴─────╮
 1 │  │ col("id_r")                             │  │ LEFT JOIN │
   │  │   .alias("chosen")                      │  ╰─────┬┬────╯
   │  ╰──────────────────────────────────────────╯        ││
   │                                                      ││
   │                                                      │╰──────────────────┬───────────────────┬───────────────────╮
   │                                                      │                   │                   │                   │
   │                                                ╭─────┴─────╮  ╭──────────┴──────────╮  ╭─────┴─────╮  ╭──────────┴──────────╮
   │                                                │ left on:  │  │ LEFT PLAN:          │  │ right on: │  │ RIGHT PLAN:         │
 2 │                                                │ col("id_l") │  │ DF ["id_l", "id_r", "a"] │  │ col("id_r") │  │ DF ["id_l", "id_r", "b"] │
   │                                                ╰───────────╯  │ PROJECT */3 COLUMNS │  ╰───────────╯  │ PROJECT */3 COLUMNS │
   │                                                               ╰─────────────────────╯                 ╰─────────────────────╯
"""


CHAINED_JOIN_PLAN = """
                           0                              1                   2                   3                   4                   5
   ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   │
   │                ╭──────────────╮
 0 │                │ WITH_COLUMNS │
   │                ╰──────┬┬──────╯
   │                       ││
   │                       │╰─────────────────────────────╮
   │                       │                              │
   │  ╭────────────────────┴─────────────────────╮        │
   │  │ expression:                              │  ╭─────┴─────╮
 1 │  │ [(col("a")) + (col("b"))]               │  │ LEFT JOIN │
   │  │   .alias("total")                        │  ╰─────┬┬────╯
   │  ╰──────────────────────────────────────────╯        ││
   │                                                      ││
   │                                                      │╰───────────────────────╮
   │                                                      │                        │
   │                                                ╭─────┴─────╮           ╭──────┴───────╮
 2 │                                                │ LEFT JOIN │           │ LEFT PLAN:   │
   │                                                ╰─────┬┬────╯           │ DF ["id", "a"] │
   │                                                      ││                 ╰──────────────╯
   │                                                      │╰──────────────────┬───────────────────┬───────────────────╮
   │                                                      │                   │                   │                   │
   │                                                ╭─────┴─────╮  ╭──────────┴──────────╮  ╭─────┴─────╮  ╭──────────┴──────────╮
   │                                                │ left on:  │  │ LEFT PLAN:          │  │ right on: │  │ RIGHT PLAN:         │
 3 │                                                │ col("id") │  │ DF ["id", "a"]      │  │ col("id") │  │ DF ["id", "b"]      │
   │                                                ╰───────────╯  │ PROJECT */2 COLUMNS │  ╰───────────╯  │ PROJECT */2 COLUMNS │
   │                                                               ╰─────────────────────╯                 ╰─────────────────────╯
"""


COMPACT_DF_JOIN_PLAN = """
0 │ │ WITH_COLUMNS │
1 │ │ expression: col("b").alias("picked") LEFT JOIN left on: col("id") right on: col("id") LEFT PLAN: DF ["id", "a"] RIGHT PLAN: DF ["id", "b"]
"""


def test_extract_select_plan_lineage() -> None:
    mapping = MappingConfig(
        sources={"orders": "svc.db.raw.orders"},
        destination_table="svc.db.curated.metrics",
    )

    lineage = extract_plan_lineage(SELECT_PLAN, mapping)

    by_to = {item.to_column.column: item for item in lineage}
    assert set(by_to) == {"one", "sum", "x"}
    assert [ref.column for ref in by_to["x"].from_columns] == ["a"]
    assert [ref.column for ref in by_to["sum"].from_columns] == ["a", "b"]
    assert by_to["one"].from_columns == ()


def test_extract_join_plan_uses_left_and_right_provenance() -> None:
    mapping = MappingConfig(
        sources={"left": "svc.db.raw.left_table", "right": "svc.db.raw.right_table"},
        destination_table="svc.db.curated.joined",
    )

    lineage = extract_plan_lineage(JOIN_PLAN, mapping)

    assert len(lineage) == 1
    source_datasets = {item.dataset.table for item in lineage[0].from_columns}
    assert source_datasets == {"left_table", "right_table"}
    assert lineage[0].to_column.column == "total"


def test_extract_aggregate_plan_lineage() -> None:
    mapping = MappingConfig(
        sources={"orders": "svc.db.raw.orders"},
        destination_table="svc.db.curated.order_stats",
    )

    lineage = extract_plan_lineage(AGG_PLAN, mapping)

    by_to = {item.to_column.column: item for item in lineage}
    assert set(by_to) == {"avg_w", "cnt", "k", "sum_v"}
    assert [item.column for item in by_to["sum_v"].from_columns] == ["v"]
    assert [item.column for item in by_to["avg_w"].from_columns] == ["w"]
    assert by_to["cnt"].from_columns == ()
    assert [item.column for item in by_to["k"].from_columns] == ["k"]


def test_extract_plan_raises_for_unresolved_source_column() -> None:
    mapping = MappingConfig(
        sources={"left": "svc.db.raw.left_table", "right": "svc.db.raw.right_table"},
        destination_table="svc.db.curated.joined",
    )
    unresolved_plan = JOIN_PLAN.replace('col("b")', 'col("missing")')

    try:
        extract_plan_lineage(unresolved_plan, mapping)
    except ValueError as exc:
        assert "unresolved source column" in str(exc)
    else:
        raise AssertionError("expected unresolved source column error")


def test_extract_join_prefers_left_for_overlapping_column_names() -> None:
    mapping = MappingConfig(
        sources={"left": "svc.db.raw.left_table", "right": "svc.db.raw.right_table"},
        destination_table="svc.db.curated.joined",
    )

    lineage = extract_plan_lineage(OVERLAP_JOIN_PLAN, mapping)

    assert lineage[0].from_columns[0].dataset.table == "left_table"


def test_extract_join_uses_alias_names_not_mapping_order() -> None:
    mapping = MappingConfig(
        sources={"right": "svc.db.raw.right_table", "left": "svc.db.raw.left_table"},
        destination_table="svc.db.curated.joined",
    )

    lineage = extract_plan_lineage(JOIN_PLAN, mapping)

    source_datasets = {item.dataset.table for item in lineage[0].from_columns}
    assert source_datasets == {"left_table", "right_table"}


def test_extract_join_raises_for_ambiguous_non_join_overlap() -> None:
    mapping = MappingConfig(
        sources={"left": "svc.db.raw.left_table", "right": "svc.db.raw.right_table"},
        destination_table="svc.db.curated.joined",
    )

    with_error = False
    try:
        extract_plan_lineage(AMBIGUOUS_JOIN_PLAN, mapping)
    except ValueError as exc:
        with_error = True
        assert "ambiguous source column" in str(exc)

    assert with_error


def test_extract_join_handles_multikey_join_overlap() -> None:
    mapping = MappingConfig(
        sources={"left": "svc.db.raw.left_table", "right": "svc.db.raw.right_table"},
        destination_table="svc.db.curated.joined",
    )

    lineage = extract_plan_lineage(MULTIKEY_OVERLAP_JOIN_PLAN, mapping)

    assert lineage[0].from_columns[0].dataset.table == "left_table"


def test_extract_join_handles_asymmetric_join_keys() -> None:
    mapping = MappingConfig(
        sources={"left": "svc.db.raw.left_table", "right": "svc.db.raw.right_table"},
        destination_table="svc.db.curated.joined",
    )

    lineage = extract_plan_lineage(ASYMMETRIC_JOIN_KEY_PLAN, mapping)

    assert lineage[0].from_columns[0].dataset.table == "right_table"


def test_extract_join_requires_left_right_aliases() -> None:
    mapping = MappingConfig(
        sources={"source1": "svc.db.raw.left_table", "source2": "svc.db.raw.right_table"},
        destination_table="svc.db.curated.joined",
    )

    with_error = False
    try:
        extract_plan_lineage(JOIN_PLAN, mapping)
    except ValueError as exc:
        with_error = True
        assert "left/right" in str(exc)

    assert with_error


def test_extract_rejects_multi_join_plan() -> None:
    mapping = MappingConfig(
        sources={"left": "svc.db.raw.left_table", "right": "svc.db.raw.right_table"},
        destination_table="svc.db.curated.joined",
    )

    with_error = False
    try:
        extract_plan_lineage(CHAINED_JOIN_PLAN, mapping)
    except ValueError as exc:
        with_error = True
        assert "multiple joins" in str(exc)

    assert with_error


def test_extract_join_requires_two_explicit_aliases() -> None:
    mapping = MappingConfig(
        sources={"orders": "svc.db.raw.orders"},
        destination_table="svc.db.curated.joined",
    )

    with_error = False
    try:
        extract_plan_lineage(JOIN_PLAN, mapping)
    except ValueError as exc:
        with_error = True
        assert "left/right" in str(exc)

    assert with_error


def test_extract_join_parses_multiple_df_matches_in_same_block() -> None:
    mapping = MappingConfig(
        sources={"left": "svc.db.raw.left_table", "right": "svc.db.raw.right_table"},
        destination_table="svc.db.curated.joined",
    )

    lineage = extract_plan_lineage(COMPACT_DF_JOIN_PLAN, mapping)

    assert lineage[0].from_columns[0].dataset.table == "right_table"
