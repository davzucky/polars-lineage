from __future__ import annotations

from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class DatasetRef(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="forbid")

    service: NonEmptyStr
    database: NonEmptyStr
    schema_name: NonEmptyStr = Field(alias="schema")
    table: NonEmptyStr

    @model_validator(mode="before")
    @classmethod
    def reject_duplicate_schema_keys(cls, data: object) -> object:
        if isinstance(data, dict) and "schema" in data and "schema_name" in data:
            raise ValueError("provide either 'schema' or 'schema_name', not both")
        return data

    @property
    def fqn(self) -> str:
        return ".".join([self.service, self.database, self.schema_name, self.table])

    @classmethod
    def from_fqn(cls, value: str) -> "DatasetRef":
        parts = value.split(".")
        if len(parts) != 4:
            raise ValueError("dataset FQN must use service.database.schema.table")
        return cls(
            service=parts[0],
            database=parts[1],
            schema=parts[2],
            table=parts[3],
        )


class ColumnRef(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    dataset: DatasetRef
    column: NonEmptyStr


class ColumnLineage(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    from_columns: tuple[ColumnRef, ...]
    to_column: ColumnRef
    function: NonEmptyStr
    confidence: Literal["exact", "inferred", "unknown"]

    @field_validator("from_columns", mode="after")
    @classmethod
    def sort_from_columns(cls, value: tuple[ColumnRef, ...]) -> tuple[ColumnRef, ...]:
        return tuple(sorted(value, key=lambda item: (item.dataset.fqn, item.column)))
