from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl

from polars_lineage.config import MappingConfig
from polars_lineage.exporter.openmetadata import export_openmetadata_requests
from polars_lineage.extractor.explain_tree import extract_plan_lineage
from polars_lineage.resolver import resolve_transitive_lineage
from polars_lineage.validation import validate_lineage


def extract_lineage_payloads_from_plan(plan: str, mapping: MappingConfig) -> list[dict[str, Any]]:
    extracted = extract_plan_lineage(plan, mapping)
    resolved = resolve_transitive_lineage(extracted)
    validate_lineage(resolved)
    return export_openmetadata_requests(resolved)


def extract_lineage_payloads_from_lazyframe(
    lazyframe: pl.LazyFrame, mapping: MappingConfig
) -> list[dict[str, Any]]:
    plan = lazyframe.explain(format="tree", optimized=False)
    return extract_lineage_payloads_from_plan(plan, mapping)


def run_extraction_to_file(
    lazyframe: pl.LazyFrame, mapping: MappingConfig, output_path: Path
) -> None:
    payloads = extract_lineage_payloads_from_lazyframe(lazyframe, mapping)
    output_path.write_text(json.dumps(payloads, indent=2, sort_keys=True), encoding="utf-8")
