# polars-lineage

Extract column-level lineage from Polars `LazyFrame` transformations.

This PoC uses a metadata-first workflow: attach metadata to each source `LazyFrame` with
`add_metadata(...)`, then run `.extract_lineage()` on the wrapped result.

## Install and Setup

```bash
pip install polars-lineage
```

For local development:

```bash
uv sync --dev
```

## Python API (Metadata First)

Importing `polars_lineage` registers `LazyFrame.add_metadata(...)`.

`add_metadata(...)` accepts:

- `name`: logical source name
- `uri`: source URI
- optional `destination_table`: explicit destination FQN

If `destination_table` is not provided, a deterministic destination FQN is derived.

Example:

```python
import polars as pl
import polars_lineage  # registers LazyFrame.add_metadata

df_orders = (
    pl.DataFrame({"id": [1, 2], "amount": [10, 20]})
    .lazy()
    .add_metadata(name="orders", uri="postgres://warehouse/svc.db.raw.orders")
)

df_accounts = (
    pl.DataFrame({"id": [1, 2], "segment": ["A", "B"]})
    .lazy()
    .add_metadata(name="accounts", uri="https://crm/accounts")
)

lineage = (
    df_orders.join(df_accounts, on="id", how="left")
    .with_columns(pl.col("amount").alias("amount_copy"))
    .extract_lineage()
)

print(lineage)
```

URI parsing notes:

- If the URI path ends with a table FQN (`service.database.schema.table`), that FQN is used.
- Otherwise lineage derives source FQN from URI parts:
  - service: URI scheme
  - database: URI hostname (or `external`)
  - schema: `public`
  - table: final URI path segment

## Wrapper Notes

- `LineageLazyFrame` preserves metadata through chained `LazyFrame` operations.
- Joining two wrapped frames merges source metadata as `left` and `right`.
- `.extract_lineage()` returns deterministic OpenMetadata-style payloads.

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

## Current Constraints

- Multiple joins in one parsed plan are rejected.
- Ambiguous non-join overlapping columns are rejected with clear errors.
- For static type checking, dynamically added `LazyFrame.add_metadata(...)` may require stubs.

## Development

```bash
uv run pytest
uv run ruff check .
uv run mypy
uv build
```
