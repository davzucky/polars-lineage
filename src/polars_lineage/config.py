from __future__ import annotations

from pydantic import BaseModel, ConfigDict, field_validator


def _validate_fqn(value: str) -> str:
    normalized = value.strip()
    parts = [part.strip() for part in normalized.split(".")]
    if len(parts) != 4 or any(not part for part in parts):
        raise ValueError("table FQN must use service.database.schema.table")
    return ".".join(parts)


def _validate_source_alias(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError("source alias cannot be empty")
    return normalized


class MappingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sources: dict[str, str]
    destination_table: str

    @field_validator("destination_table")
    @classmethod
    def validate_destination_table(cls, value: str) -> str:
        return _validate_fqn(value)

    @field_validator("sources")
    @classmethod
    def validate_sources(cls, value: dict[str, str]) -> dict[str, str]:
        if not value:
            raise ValueError("at least one source mapping is required")
        normalized_sources: dict[str, str] = {}
        for alias, fqn in value.items():
            normalized_alias = _validate_source_alias(alias)
            if normalized_alias in normalized_sources:
                raise ValueError(f"duplicate source alias after normalization: {normalized_alias}")
            normalized_sources[normalized_alias] = _validate_fqn(fqn)
        return normalized_sources
