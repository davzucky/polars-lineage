from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl

from polars_lineage.config import MappingConfig
from polars_lineage.exporter import OutputFormat, RenderedLineage, export_lineage
from polars_lineage.exporter.models import LineageDocument
from polars_lineage.extractor.explain_tree import extract_plan_lineage
from polars_lineage.ir import ColumnLineage
from polars_lineage.resolver import resolve_transitive_lineage
from polars_lineage.validation import validate_lineage


def extract_lineage_ir_from_plan(plan: str, mapping: MappingConfig) -> list[ColumnLineage]:
    extracted = extract_plan_lineage(plan, mapping)
    resolved = resolve_transitive_lineage(extracted)
    validate_lineage(resolved)
    return resolved


def extract_lineage_ir_from_lazyframe(
    lazyframe: pl.LazyFrame, mapping: MappingConfig
) -> list[ColumnLineage]:
    plan = lazyframe.explain(format="tree", optimized=False)
    return extract_lineage_ir_from_plan(plan, mapping)


def extract_lineage_output_from_plan(
    plan: str, mapping: MappingConfig, output_format: OutputFormat = "openmetadata"
) -> RenderedLineage:
    resolved = extract_lineage_ir_from_plan(plan, mapping)
    return export_lineage(resolved, mapping, output_format)


def extract_lineage_output_from_lazyframe(
    lazyframe: pl.LazyFrame,
    mapping: MappingConfig,
    output_format: OutputFormat = "openmetadata",
) -> RenderedLineage:
    resolved = extract_lineage_ir_from_lazyframe(lazyframe, mapping)
    return export_lineage(resolved, mapping, output_format)


def extract_lineage_payloads_from_plan(plan: str, mapping: MappingConfig) -> list[dict[str, Any]]:
    output = extract_lineage_output_from_plan(plan, mapping, output_format="openmetadata")
    if not isinstance(output, list):  # pragma: no cover - defensive typing guard
        raise TypeError("openmetadata export must return a JSON payload list")
    return output


def extract_lineage_payloads_from_lazyframe(
    lazyframe: pl.LazyFrame, mapping: MappingConfig
) -> list[dict[str, Any]]:
    output = extract_lineage_output_from_lazyframe(lazyframe, mapping, output_format="openmetadata")
    if not isinstance(output, list):  # pragma: no cover - defensive typing guard
        raise TypeError("openmetadata export must return a JSON payload list")
    return output


def run_extraction_to_file(
    lazyframe: pl.LazyFrame,
    mapping: MappingConfig,
    output_path: Path,
    output_format: OutputFormat = "openmetadata",
) -> None:
    output = extract_lineage_output_from_lazyframe(lazyframe, mapping, output_format=output_format)
    if isinstance(output, str):
        output_path.write_text(output, encoding="utf-8")
        return
    payload: Any
    if isinstance(output, LineageDocument):
        payload = output.model_dump(mode="json")
    else:
        payload = output
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
