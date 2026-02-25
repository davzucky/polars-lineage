from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, StringConstraints, field_validator

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class LineageColumn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    to_column: NonEmptyStr
    from_columns: list[NonEmptyStr]
    function: NonEmptyStr
    confidence: Literal["exact", "inferred", "unknown"]

    @field_validator("from_columns", mode="after")
    @classmethod
    def sort_from_columns(cls, value: list[str]) -> list[str]:
        return sorted(set(value))


class LineageEdge(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_table: NonEmptyStr
    destination_table: NonEmptyStr
    columns: list[LineageColumn]

    @field_validator("columns", mode="after")
    @classmethod
    def sort_columns(cls, value: list[LineageColumn]) -> list[LineageColumn]:
        return sorted(
            value,
            key=lambda item: (
                item.to_column,
                tuple(item.from_columns),
                item.function,
                item.confidence,
            ),
        )


class LineageDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    destination_table: NonEmptyStr
    edges: list[LineageEdge]

    @field_validator("edges", mode="after")
    @classmethod
    def sort_edges(cls, value: list[LineageEdge]) -> list[LineageEdge]:
        return sorted(value, key=lambda item: (item.source_table, item.destination_table))
