import json
from pathlib import Path

import typer

from polars_lineage.config import load_mapping_config
from polars_lineage.pipeline import extract_lineage_payloads_from_plan

app = typer.Typer(help="polars-lineage command line tools")


@app.callback()
def callback() -> None:
    """polars-lineage command group."""


@app.command("extract")
def extract(
    mapping: Path = typer.Option(..., "--mapping", exists=True, readable=True),
    out: Path = typer.Option(..., "--out"),
) -> None:
    """Extract lineage and write OpenMetadata-style payload JSON."""
    try:
        config = load_mapping_config(mapping)
        if config.plan_path is None:
            typer.echo("mapping must include plan_path", err=True)
            raise typer.Exit(code=1)

        plan_text = config.plan_path.read_text(encoding="utf-8")
        payloads = extract_lineage_payloads_from_plan(plan_text, config)
        out.write_text(json.dumps(payloads, indent=2, sort_keys=True), encoding="utf-8")
    except typer.Exit:
        raise
    except Exception as exc:  # pragma: no cover - defensive CLI boundary
        typer.echo(f"extract failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc


def main() -> None:
    app()
