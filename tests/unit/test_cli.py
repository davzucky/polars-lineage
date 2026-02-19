from typer.testing import CliRunner

from polars_lineage.cli import app

runner = CliRunner()


def test_cli_help_resolves() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "Usage:" in result.stdout


def test_extract_reports_not_implemented() -> None:
    result = runner.invoke(app, ["extract"])

    assert result.exit_code == 1
    assert "not implemented" in result.stdout.lower()
