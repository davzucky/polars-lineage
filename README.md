# polars-lineage

Extract column-level lineage from Polars `LazyFrame` transformations.

This PoC uses a metadata-first workflow through a `LazyFrame.lineage` namespace.
Attach source metadata with `lineage.add_source(...)`, then run `lineage.extract()`.

## Install and Setup

```bash
pip install polars-lineage
```

For local development:

```bash
uv sync --dev
```

## Python API (Metadata First)

Importing `polars_lineage` registers `LazyFrame.lineage`.

`lineage.add_source(...)` accepts:

- `name`: logical source name
- `uri`: source URI
- optional `destination_table`: explicit destination FQN

If `destination_table` is not provided, a deterministic destination FQN is derived.

Example:

```python
import polars as pl
import polars_lineage  # registers LazyFrame.lineage namespace

df_orders = (
    pl.DataFrame({"id": [1, 2], "amount": [10, 20]})
    .lazy()
    .lineage.add_source(name="orders", uri="postgres://warehouse/svc.db.raw.orders")
)

df_accounts = (
    pl.DataFrame({"id": [1, 2], "segment": ["A", "B"]})
    .lazy()
    .lineage.add_source(name="accounts", uri="https://crm/accounts")
)

lineage = (
    df_orders.join(df_accounts, on="id", how="left")
    .with_columns(pl.col("amount").alias("amount_copy"))
    .lineage.extract()
)

print(lineage)

markdown = (
    df_orders.join(df_accounts, on="id", how="left")
    .with_columns(pl.col("amount").alias("amount_copy"))
    .lineage.render(format="markdown")
)

print(markdown)
```

URI parsing notes:

- If the URI path ends with a table FQN (`service.database.schema.table`), that FQN is used.
- Otherwise lineage derives source FQN from URI parts:
  - service: URI scheme
  - database: URI hostname (or `external`)
  - schema: `public`
  - table: final URI path segment

## Namespace Notes

- `lineage.add_source(...)` returns the same `pl.LazyFrame` instance.
- Metadata is propagated through common lazy operations (including joins).
- `lineage.extract()` returns deterministic OpenMetadata-style payloads.

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
- For static type checking, dynamically registered `LazyFrame.lineage` may require stubs.

## Development

```bash
uv run pytest
uv run ruff check .
uv run mypy
uv build
```
