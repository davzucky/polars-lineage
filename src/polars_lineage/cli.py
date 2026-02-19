import typer

app = typer.Typer(help="polars-lineage command line tools")


@app.callback()
def callback() -> None:
    """polars-lineage command group."""


@app.command("extract")
def extract() -> None:
    """Placeholder command for extraction pipeline."""
    typer.echo("extract is not implemented yet")
    raise typer.Exit(code=1)


def main() -> None:
    app()
