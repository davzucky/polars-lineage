# AGENTS

Guidance for coding agents working in this repository.

## Project Overview

- Project: `polars-lineage`
- Purpose: Extract column-level lineage from Polars `LazyFrame` plans and emit OpenMetadata-shaped JSON payloads.
- Package manager and task runner: `uv`
- Test style: TDD (`red -> green -> refactor`), small incremental changes.

## Core Rules

- Create a GitHub issue before starting a feature-sized change.
- Prefer small commits and push frequently.
- Run a review pass (sub-agent or equivalent) for non-trivial diffs.
- Keep outputs deterministic (sorted, stable JSON shape/order).

## Local Commands

- Install/sync dependencies: `uv sync --dev`
- Run tests: `uv run pytest`
- Lint: `uv run ruff check .`
- Type check: `uv run mypy`
- Build package: `uv build`

## Public API First

Prefer validating behavior through the public API when possible:

- `polars_lineage.extract_lazyframe_lineage(lazyframe, mapping)`

`mapping` can be:

- `MappingConfig`, or
- dict with at least:
  - `sources` (alias -> table FQN)
  - `destination_table` (table FQN)

## CLI Notes

CLI command:

- `polars-lineage extract --mapping mapping.yml --out lineage.json`

Current CLI expects `plan_path` in `mapping.yml` and reads plan text from disk.

## Join/Plan Constraints

- Join plans require explicit `left` and `right` keys in `mapping.sources`.
- Multiple joins in one parsed plan are currently rejected.
- Ambiguous non-join overlapping columns are rejected with clear errors.

## Directory Map

- `src/polars_lineage/` - runtime package
- `tests/unit/` - unit tests
- `tests/e2e/` - end-to-end tests
- `docs/` - design and implementation notes
