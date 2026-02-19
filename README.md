# polars-lineage

Extract column-level lineage from Polars `LazyFrame` transformations and emit deterministic OpenMetadata-shaped JSON payloads.

## Install and Setup

```bash
uv sync --dev
```

## Python API (LazyFrame First)

```python
import polars as pl
from polars_lineage import extract_lazyframe_lineage

lazyframe = pl.DataFrame({"a": [1, 2], "b": [3, 4]}).lazy().select(
    [
        pl.col("a").alias("x"),
        (pl.col("a") + pl.col("b")).alias("sum"),
    ]
)

payloads = extract_lazyframe_lineage(
    lazyframe,
    {
        "sources": {"orders": "svc.db.raw.orders"},
        "destination_table": "svc.db.curated.metrics",
    },
)

print(payloads)
```

The `mapping` argument can be either:

- a `MappingConfig`
- a `dict` with `sources` and `destination_table`

## CLI Usage

```bash
uv run polars-lineage extract --mapping mapping.yml --out lineage.json
```

`mapping.yml` example:

```yaml
sources:
  left: svc.db.raw.left_table
  right: svc.db.raw.right_table
destination_table: svc.db.curated.final_table
plan_path: ./plan.txt
```

Notes:

- `plan_path` can be relative to the mapping file location.
- CLI reads a pre-generated Polars explain plan from disk.

## Current Capabilities

- Projection lineage (`select`, `with_columns`)
- Literals and aliases
- Basic expression dependency extraction (arithmetic, casts, conditional-like patterns)
- Transitive dependency resolution
- Join-aware attribution with explicit `left`/`right` mapping aliases
- Group-by aggregation expression and key coverage
- Deterministic OpenMetadata payload export

## Current Constraints

- Multiple joins in one parsed plan are rejected.
- Join mappings must include `left` and `right` source aliases.
- Ambiguous non-join overlapping columns are rejected with clear errors.

## Development

```bash
uv run pytest
uv run ruff check .
uv run mypy
uv build
```
