import pytest
from pydantic import ValidationError

from polars_lineage.config import MappingConfig


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
