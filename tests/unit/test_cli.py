import json
from pathlib import Path

from typer.testing import CliRunner

from polars_lineage.cli import app

runner = CliRunner()


def test_cli_help_resolves() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "Usage:" in result.stdout


def test_extract_reports_not_implemented() -> None:
    result = runner.invoke(app, ["extract"])

    assert result.exit_code != 0


def test_extract_command_writes_output_from_mapping_plan(tmp_path: Path) -> None:
    mapping_path = tmp_path / "mapping.yml"
    plan_path = tmp_path / "plan.txt"
    out_path = tmp_path / "lineage.json"

    plan_path.write_text(
        """
              0                        1                             2                          3
   ┌────────────────────────────────────────────────────────────────────────────────────────────────────────
   │
   │      ╭────────╮
 0 │      │ SELECT │
   │      ╰───┬┬───╯
   │          ││
   │          │╰───────────────────────┬─────────────────────────────┬──────────────────────────╮
   │          │                        │                             │                          │
   │  ╭───────┴───────╮  ╭─────────────┴─────────────╮  ╭────────────┴────────────╮  ╭──────────┴──────────╮
   │  │ expression:   │  │ expression:               │  │ FROM:                   │
 1 │  │ col("a")      │  │ [(col("a")) + (col("b"))] │  │ DF ["a", "b"]         │
   │  │   .alias("x") │  │   .alias("sum")           │  │ PROJECT */2 COLUMNS     │
   │  ╰───────────────╯  ╰───────────────────────────╯  ╰──────────────────────────╯
        """.strip(),
        encoding="utf-8",
    )
    mapping_path.write_text(
        """
sources:
  orders: svc.db.raw.orders
destination_table: svc.db.curated.metrics
plan_path: PLAN_PATH
        """.replace("PLAN_PATH", str(plan_path)),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "extract",
            "--mapping",
            str(mapping_path),
            "--out",
            str(out_path),
        ],
    )

    assert result.exit_code == 0
    payloads = json.loads(out_path.read_text(encoding="utf-8"))
    assert payloads[0]["edge"]["toEntity"]["fullyQualifiedName"] == "svc.db.curated.metrics"


def test_extract_command_resolves_relative_plan_path(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    mapping_path = config_dir / "mapping.yml"
    plan_path = config_dir / "plan.txt"
    out_path = tmp_path / "lineage.json"

    plan_path.write_text(
        """
0 │ │ SELECT │
1 │ │ expression: col("a").alias("x") │ │ FROM: DF ["a"]
        """.strip(),
        encoding="utf-8",
    )
    mapping_path.write_text(
        """
sources:
  orders: svc.db.raw.orders
destination_table: svc.db.curated.metrics
plan_path: plan.txt
        """.strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "extract",
            "--mapping",
            str(mapping_path),
            "--out",
            str(out_path),
        ],
    )

    assert result.exit_code == 0
    assert out_path.exists()


def test_extract_command_reports_missing_plan_path(tmp_path: Path) -> None:
    mapping_path = tmp_path / "mapping.yml"
    out_path = tmp_path / "lineage.json"
    mapping_path.write_text(
        """
sources:
  orders: svc.db.raw.orders
destination_table: svc.db.curated.metrics
        """.strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["extract", "--mapping", str(mapping_path), "--out", str(out_path)],
    )

    assert result.exit_code == 1
    assert "plan_path" in (result.stdout + result.stderr)
