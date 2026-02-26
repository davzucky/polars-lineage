from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, TypeAlias, assert_never

from polars_lineage.config import MappingConfig
from polars_lineage.exporter.json import export_lineage_document
from polars_lineage.exporter.markdown import export_lineage_markdown
from polars_lineage.exporter.models import LineageDocument
from polars_lineage.exporter.openmetadata import export_openmetadata_requests
from polars_lineage.ir import ColumnLineage

OutputFormat: TypeAlias = Literal["openmetadata", "json", "markdown"]
RenderedLineage: TypeAlias = list[dict[str, Any]] | LineageDocument | str


def export_lineage(
    lineage: list[ColumnLineage], mapping: MappingConfig, output_format: OutputFormat
) -> RenderedLineage:
    if output_format == "openmetadata":
        return export_openmetadata_requests(lineage)
    if output_format == "json":
        return export_lineage_document(lineage, destination_table=mapping.destination_table)
    if output_format == "markdown":
        document = export_lineage_document(lineage, destination_table=mapping.destination_table)
        return export_lineage_markdown(document, mapping)
    if TYPE_CHECKING:
        assert_never(output_format)
    raise ValueError(f"unsupported output format: {output_format}")
