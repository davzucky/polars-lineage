import json
from pathlib import Path

import typer

from polars_lineage.config import load_mapping_config
from polars_lineage.exporter import OutputFormat
from polars_lineage.exporter.models import LineageDocument
from polars_lineage.pipeline import extract_lineage_output_from_plan

app = typer.Typer(help="polars-lineage command line tools")


@app.callback()
def callback() -> None:
    """polars-lineage command group."""


@app.command("extract")
def extract(
    mapping: Path = typer.Option(..., "--mapping", exists=True, readable=True),
    out: Path = typer.Option(..., "--out"),
    output_format: OutputFormat = typer.Option("openmetadata", "--format"),
) -> None:
    """Extract lineage and write output in the requested format."""
    try:
        config = load_mapping_config(mapping)
        if config.plan_path is None:
            typer.echo("mapping must include plan_path", err=True)
            raise typer.Exit(code=1)

        plan_text = config.plan_path.read_text(encoding="utf-8")
        output = extract_lineage_output_from_plan(
            plan_text,
            config,
            output_format=output_format,
        )
        if isinstance(output, str):
            out.write_text(output, encoding="utf-8")
        elif isinstance(output, LineageDocument):
            out.write_text(
                json.dumps(output.model_dump(mode="json"), indent=2, sort_keys=True),
                encoding="utf-8",
            )
        else:
            out.write_text(json.dumps(output, indent=2, sort_keys=True), encoding="utf-8")
    except typer.Exit:
        raise
    except Exception as exc:  # pragma: no cover - defensive CLI boundary
        typer.echo(f"extract failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc


def main() -> None:
    app()
