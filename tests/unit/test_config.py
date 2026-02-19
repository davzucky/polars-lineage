from pathlib import Path

import pytest
from pydantic import ValidationError

from polars_lineage.config import MappingConfig, load_mapping_config


def test_mapping_config_accepts_valid_fqns() -> None:
    config = MappingConfig(
        sources={"orders_csv": "svc.db.raw.orders"},
        destination_table="svc.db.curated.order_metrics",
    )

    assert config.sources["orders_csv"] == "svc.db.raw.orders"


def test_mapping_config_rejects_invalid_source_fqn() -> None:
    with pytest.raises(ValidationError):
        MappingConfig(
            sources={"orders_csv": "svc.db.orders"},
            destination_table="svc.db.curated.order_metrics",
        )


def test_mapping_config_rejects_invalid_destination_fqn() -> None:
    with pytest.raises(ValidationError):
        MappingConfig(
            sources={"orders_csv": "svc.db.raw.orders"},
            destination_table="svc.db.order_metrics",
        )


def test_mapping_config_rejects_empty_sources() -> None:
    with pytest.raises(ValidationError):
        MappingConfig(
            sources={},
            destination_table="svc.db.curated.order_metrics",
        )


def test_mapping_config_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        MappingConfig.model_validate(
            {
                "sources": {"orders_csv": "svc.db.raw.orders"},
                "destination_table": "svc.db.curated.order_metrics",
                "unexpected": "value",
            }
        )


def test_mapping_config_rejects_empty_source_alias() -> None:
    with pytest.raises(ValidationError):
        MappingConfig(
            sources={"   ": "svc.db.raw.orders"},
            destination_table="svc.db.curated.order_metrics",
        )


def test_mapping_config_rejects_missing_destination_table() -> None:
    with pytest.raises(ValidationError):
        MappingConfig.model_validate(
            {
                "sources": {"orders_csv": "svc.db.raw.orders"},
            }
        )


def test_mapping_config_rejects_duplicate_normalized_aliases() -> None:
    with pytest.raises(ValidationError):
        MappingConfig(
            sources={
                "orders": "svc.db.raw.orders",
                " orders ": "svc.db.raw.orders_v2",
            },
            destination_table="svc.db.curated.order_metrics",
        )


def test_load_mapping_config_reads_yaml(tmp_path: Path) -> None:
    mapping_path = tmp_path / "mapping.yml"
    mapping_path.write_text(
        """
sources:
  orders: svc.db.raw.orders
destination_table: svc.db.curated.order_metrics
plan_path: /tmp/plan.txt
        """.strip(),
        encoding="utf-8",
    )

    config = load_mapping_config(mapping_path)

    assert config.plan_path == Path("/tmp/plan.txt")


def test_load_mapping_config_rejects_non_object_yaml(tmp_path: Path) -> None:
    mapping_path = tmp_path / "mapping.yml"
    mapping_path.write_text("- not\n- a\n- dict\n", encoding="utf-8")

    with pytest.raises(ValueError):
        load_mapping_config(mapping_path)
