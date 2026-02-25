# polars-lineage

Extract column-level lineage from Polars `LazyFrame` transformations and emit deterministic lineage artifacts in multiple formats.

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

`extract_lazyframe_lineage(...)` stays backward-compatible and returns OpenMetadata payloads by default.

For format-aware output, use `extract_lazyframe_lineage_formatted(...)`:

```python
import polars as pl
from polars_lineage import extract_lazyframe_lineage_formatted

lazyframe = pl.DataFrame({"a": [1], "b": [2]}).lazy().select(
    [(pl.col("a") + pl.col("b")).alias("sum")]
)

json_document = extract_lazyframe_lineage_formatted(
    lazyframe,
    {
        "sources": {"orders": "svc.db.raw.orders"},
        "destination_table": "svc.db.curated.metrics",
    },
    output_format="json",
)

markdown_report = extract_lazyframe_lineage_formatted(
    lazyframe,
    {
        "sources": {"orders": "svc.db.raw.orders"},
        "destination_table": "svc.db.curated.metrics",
    },
    output_format="markdown",
)

print(json_document.model_dump())
print(markdown_report)
```

If you want a strongly typed Pydantic model for consumer code, use:

```python
import polars as pl
from polars_lineage import LineageDocument, extract_lazyframe_lineage_document

lazyframe = pl.DataFrame({"a": [1]}).lazy().select([pl.col("a").alias("x")])

document: LineageDocument = extract_lazyframe_lineage_document(
    lazyframe,
    {
        "sources": {"orders": "svc.db.raw.orders"},
        "destination_table": "svc.db.curated.metrics",
    },
)

print(document.model_dump())
```

Example with multiple input sources (join):

```python
import polars as pl
from polars_lineage import extract_lazyframe_lineage

left = pl.DataFrame({"id": [1, 2], "a": [10, 20]}).lazy()
right = pl.DataFrame({"id": [1, 2], "b": [3, 4]}).lazy()

lazyframe = left.join(right, on="id", how="left").with_columns(
    (pl.col("a") + pl.col("b")).alias("total")
)

payloads = extract_lazyframe_lineage(
    lazyframe,
    {
        "sources": {
            "left": "svc.db.raw.left_table",
            "right": "svc.db.raw.right_table",
        },
        "destination_table": "svc.db.curated.joined_metrics",
    },
)

print(payloads)
```

The `mapping` argument can be either:

- a `MappingConfig`
- a `dict` with `sources` and `destination_table`

### Metadata-on-LazyFrame Pattern

After importing `polars_lineage`, `pl.LazyFrame` gets an `add_metadata(...)` helper.

Supported forms:

- explicit lineage mapping:
  - `source="svc.db.raw.orders"` (single source), or
  - `sources={"left": "...", "right": "..."}` (multi-source)
  - optional `destination_table="svc.db.curated.result"`
- metadata mode:
  - `name="orders"`, `source_type="postgres"`, `source_url="postgres://..."`
  - destination table is auto-derived unless provided

Example with metadata attached directly to `LazyFrame` definitions:

```python
import polars as pl
import polars_lineage  # registers LazyFrame.add_metadata

df_order = (
    pl.DataFrame({"a": [1], "b": [2]})
    .lazy()
    .add_metadata(
        name="orders",
        source_type="postgres",
        source_url="postgres://myserver/svc.db.raw.orders",
    )
)

df_account = (
    pl.DataFrame({"a": [1], "b": [2]})
    .lazy()
    .add_metadata(
        name="account",
        source_type="rest",
        source_url="https://account/list",
    )
)

lineage = (
    df_account.join(df_order, on="a", how="inner")
    .select([(pl.col("a") + pl.col("b")).alias("sum")])
    .extract_lineage()
)

print(lineage)
```

Equivalent single-source style with explicit `source`:

```python
import polars as pl
import polars_lineage

lineage = (
    pl.DataFrame({"a": [1], "b": [2]})
    .lazy()
    .add_metadata(
        source="svc.db.raw.orders",
        destination_table="svc.db.curated.order_metrics",
    )
    .select([(pl.col("a") + pl.col("b")).alias("sum")])
    .extract_lineage()
)

print(lineage)
```

Example with group-by aggregation lineage:

```python
import polars as pl
from polars_lineage import extract_lazyframe_lineage

lazyframe = (
    pl.DataFrame({"customer_id": [1, 1, 2], "amount": [10, 15, 8]})
    .lazy()
    .group_by("customer_id")
    .agg(pl.col("amount").sum().alias("total_amount"))
)

payloads = extract_lazyframe_lineage(
    lazyframe,
    {
        "sources": {"payments": "svc.db.raw.payments"},
        "destination_table": "svc.db.curated.customer_totals",
    },
)

print(payloads)
```

## CLI Usage

```bash
uv run polars-lineage extract --mapping mapping.yml --out lineage.json
```

Choose an output format with `--format`:

```bash
# Existing default behavior
uv run polars-lineage extract --mapping mapping.yml --out lineage-openmetadata.json --format openmetadata

# Strongly typed custom JSON document
uv run polars-lineage extract --mapping mapping.yml --out lineage.json --format json

# Human-readable report
uv run polars-lineage extract --mapping mapping.yml --out lineage.md --format markdown
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
- For join plans, `mapping.sources` must include `left` and `right` aliases.

## Wrapper Notes

- `LineageLazyFrame` preserves metadata through most chained `LazyFrame` operations.
- Joining two wrapped frames merges source metadata automatically (`left`/`right`).
- `extract_lineage()` runs the same extraction pipeline as `extract_lazyframe_lineage(...)`.

## Current Capabilities

- Projection lineage (`select`, `with_columns`)
- Literals and aliases
- Basic expression dependency extraction (arithmetic, casts, conditional-like patterns)
- Transitive dependency resolution
- Join-aware attribution with explicit `left`/`right` mapping aliases
- Group-by aggregation expression and key coverage
- Deterministic OpenMetadata payload export
- Deterministic custom JSON export via typed `LineageDocument` model
- Deterministic Markdown lineage rendering

## Output Formats

- `openmetadata`: existing OpenMetadata AddLineageRequest-style payload list
- `json`: custom typed JSON document
  - top-level: `destination_table`, `edges[]`
  - edge: `source_table`, `destination_table`, `columns[]`
  - column: `to_column`, `from_columns`, `function`, `confidence`
- `markdown`: human-readable lineage table report
- `openlineage`: planned follow-up

## Current Constraints

- Multiple joins in one parsed plan are rejected.
- Join mappings must include `left` and `right` source aliases.
- Ambiguous non-join overlapping columns are rejected with clear errors.
- For static type checking, dynamically added `LazyFrame.add_metadata(...)` may require stubs for full IDE/mypy method discovery.

## Development

```bash
uv run pytest
uv run ruff check .
uv run mypy
uv build
```
