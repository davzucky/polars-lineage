from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import polars as pl

from polars_lineage.config import MappingConfig
from polars_lineage.pipeline import extract_lineage_payloads_from_lazyframe


def _normalize_mapping(mapping: MappingConfig | dict[str, Any]) -> MappingConfig:
    if isinstance(mapping, MappingConfig):
        return mapping
    return MappingConfig.model_validate(mapping)


def _sanitize_token(value: str) -> str:
    cleaned = "".join(char if char.isalnum() else "_" for char in value.strip().lower())
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    cleaned = cleaned.strip("_")
    return cleaned or "unknown"


def _default_destination_fqn(sources: dict[str, str]) -> str:
    suffix = "__".join(sorted(_sanitize_token(alias) for alias in sources))
    return f"derived.lineage.public.{suffix or 'result'}"


def _source_fqn_from_metadata(name: str, uri: str) -> str:
    parsed = urlparse(uri)
    path = parsed.path.strip("/")
    maybe_fqn = path.split("/")[-1]
    fqn_parts = maybe_fqn.split(".")
    if len(fqn_parts) == 4 and all(fqn_parts):
        return ".".join(_sanitize_token(part) for part in fqn_parts)

    service = _sanitize_token(parsed.scheme or "external")
    database = _sanitize_token(parsed.hostname or "external")
    table = _sanitize_token((path.split("/")[-1] if path else name) or name)
    return f"{service}.{database}.public.{table}"


def _unwrap_lineage_value(value: Any) -> Any:
    if isinstance(value, LineageLazyFrame):
        return value.lazyframe
    if isinstance(value, list):
        return [_unwrap_lineage_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_unwrap_lineage_value(item) for item in value)
    if isinstance(value, dict):
        return {key: _unwrap_lineage_value(item) for key, item in value.items()}
    return value


def _first_source_fqn(mapping: MappingConfig) -> str:
    return next(iter(mapping.sources.values()))


def _merge_mapping_for_method(
    method_name: str, base: MappingConfig, others: list[MappingConfig]
) -> MappingConfig:
    if not others:
        return base

    if method_name == "join":
        right_mapping = others[0]
        return MappingConfig(
            sources={
                "left": _first_source_fqn(base),
                "right": _first_source_fqn(right_mapping),
            },
            destination_table=_default_destination_fqn(
                {
                    "left": _first_source_fqn(base),
                    "right": _first_source_fqn(right_mapping),
                }
            ),
        )

    merged_sources = dict(base.sources)
    for other in others:
        for alias, fqn in other.sources.items():
            if alias not in merged_sources:
                merged_sources[alias] = fqn
    return MappingConfig(
        sources=merged_sources,
        destination_table=base.destination_table,
    )


@dataclass(frozen=True)
class LineageLazyFrame:
    lazyframe: pl.LazyFrame
    mapping: MappingConfig

    def extract_lineage(self) -> list[dict[str, Any]]:
        return extract_lineage_payloads_from_lazyframe(self.lazyframe, self.mapping)

    def unwrap(self) -> pl.LazyFrame:
        return self.lazyframe

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self.lazyframe, name)
        if not callable(attribute):
            return attribute

        def wrapped_method(*args: Any, **kwargs: Any) -> Any:
            other_mappings = [
                item.mapping
                for item in [*args, *kwargs.values()]
                if isinstance(item, LineageLazyFrame)
            ]
            result = attribute(
                *[_unwrap_lineage_value(item) for item in args],
                **{key: _unwrap_lineage_value(value) for key, value in kwargs.items()},
            )
            mapping = _merge_mapping_for_method(name, self.mapping, other_mappings)
            return _wrap_result_with_mapping(result, mapping)

        return wrapped_method


def _wrap_result_with_mapping(result: Any, mapping: MappingConfig) -> Any:
    if isinstance(result, pl.LazyFrame):
        return LineageLazyFrame(result, mapping)
    if isinstance(result, list):
        return [_wrap_result_with_mapping(item, mapping) for item in result]
    if isinstance(result, tuple):
        return tuple(_wrap_result_with_mapping(item, mapping) for item in result)
    return result


def _with_lineage(
    lazyframe: pl.LazyFrame, mapping: MappingConfig | dict[str, Any]
) -> LineageLazyFrame:
    return LineageLazyFrame(lazyframe=lazyframe, mapping=_normalize_mapping(mapping))


def add_lazyframe_metadata(
    lazyframe: pl.LazyFrame,
    *,
    name: str,
    uri: str,
    destination_table: str | None = None,
) -> LineageLazyFrame:
    if not name.strip():
        raise ValueError("name must be non-empty")
    if not uri.strip():
        raise ValueError("uri must be non-empty")

    normalized_sources = {name: _source_fqn_from_metadata(name, uri)}

    return _with_lineage(
        lazyframe,
        {
            "sources": normalized_sources,
            "destination_table": destination_table or _default_destination_fqn(normalized_sources),
        },
    )


def register_lazyframe_metadata_method() -> None:
    if hasattr(pl.LazyFrame, "add_metadata"):
        return

    def _add_metadata_method(
        self: pl.LazyFrame,
        *,
        name: str,
        uri: str,
        destination_table: str | None = None,
    ) -> LineageLazyFrame:
        return add_lazyframe_metadata(
            self,
            name=name,
            uri=uri,
            destination_table=destination_table,
        )

    setattr(pl.LazyFrame, "add_metadata", _add_metadata_method)


register_lazyframe_metadata_method()
