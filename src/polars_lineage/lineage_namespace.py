from __future__ import annotations

from collections.abc import Iterable
from typing import Any, cast
from urllib.parse import urlparse

import polars as pl

from polars_lineage.config import MappingConfig
from polars_lineage.metadata_store import get_mapping, require_mapping, set_mapping
from polars_lineage.pipeline import extract_lineage_payloads_from_lazyframe

_PATCHED = False
_GROUP_BY_PATCHED = False


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


def _first_source_fqn(mapping: MappingConfig) -> str:
    return next(iter(mapping.sources.values()))


def _extract_mappings(values: Iterable[Any]) -> list[MappingConfig]:
    found: list[MappingConfig] = []
    for value in values:
        if isinstance(value, pl.LazyFrame):
            mapping = get_mapping(value)
            if mapping is not None:
                found.append(mapping)
        elif isinstance(value, (list, tuple)):
            found.extend(_extract_mappings(value))
        elif isinstance(value, dict):
            found.extend(_extract_mappings(value.values()))
    return found


def _merge_mapping_for_method(
    method_name: str, base: MappingConfig | None, others: list[MappingConfig]
) -> MappingConfig | None:
    if base is None and not others:
        return None

    if method_name == "join":
        if base is None or not others:
            raise ValueError(
                "join lineage requires metadata on both frames; call "
                "`lazyframe.lineage.add_source(name=..., uri=...)` on left and right"
            )
        if len(others) != 1:
            raise ValueError("multiple joins in one parsed plan are currently rejected")
        if "left" in base.sources and "right" in base.sources:
            raise ValueError("multiple joins in one parsed plan are currently rejected")

        if "left" in base.sources and "right" in others[0].sources:
            left_source = base.sources["left"]
            right_source = others[0].sources["right"]
        else:
            if len(base.sources) != 1 or len(others[0].sources) != 1:
                raise ValueError("multiple joins in one parsed plan are currently rejected")
            left_source = _first_source_fqn(base)
            right_source = _first_source_fqn(others[0])

        return MappingConfig(
            sources={
                "left": left_source,
                "right": right_source,
            },
            destination_table=_default_destination_fqn(
                {
                    "left": left_source,
                    "right": right_source,
                }
            ),
        )

    seed = base or others[0]
    merged_sources = dict(seed.sources)
    for other in [] if base is None else others:
        for alias, fqn in other.sources.items():
            if alias not in merged_sources:
                merged_sources[alias] = fqn
    if base is None and len(others) > 1:
        for other in others[1:]:
            for alias, fqn in other.sources.items():
                if alias not in merged_sources:
                    merged_sources[alias] = fqn

    return MappingConfig(
        sources=merged_sources,
        destination_table=seed.destination_table,
    )


def _patch_lazyframe_method(method_name: str) -> None:
    original = getattr(pl.LazyFrame, method_name, None)
    if original is None or getattr(original, "__lineage_patched__", False):
        return

    def patched(self: pl.LazyFrame, *args: Any, **kwargs: Any) -> Any:
        result = original(self, *args, **kwargs)
        base_mapping = get_mapping(self)
        other_mappings = _extract_mappings([*args, *kwargs.values()])
        merged = _merge_mapping_for_method(method_name, base_mapping, other_mappings)
        if merged is None:
            return result
        if isinstance(result, pl.LazyFrame):
            set_mapping(result, merged)
            return result
        setattr(result, "_lineage_mapping", merged)
        return result

    setattr(patched, "__lineage_patched__", True)
    setattr(pl.LazyFrame, method_name, patched)


def _patch_group_by_agg() -> None:
    global _GROUP_BY_PATCHED
    if _GROUP_BY_PATCHED:
        return
    try:
        from polars.lazyframe.group_by import LazyGroupBy
    except ImportError:  # pragma: no cover
        return

    original = getattr(LazyGroupBy, "agg", None)
    if original is None or getattr(original, "__lineage_patched__", False):
        _GROUP_BY_PATCHED = True
        return

    def patched(self: Any, *args: Any, **kwargs: Any) -> pl.LazyFrame:
        result = cast(pl.LazyFrame, original(self, *args, **kwargs))
        mapping = getattr(self, "_lineage_mapping", None)
        if isinstance(mapping, MappingConfig):
            set_mapping(result, mapping)
        return result

    setattr(patched, "__lineage_patched__", True)
    setattr(LazyGroupBy, "agg", patched)
    _GROUP_BY_PATCHED = True


def _enable_lineage_propagation() -> None:
    global _PATCHED
    if _PATCHED:
        return

    for method_name in (
        "select",
        "with_columns",
        "filter",
        "sort",
        "rename",
        "drop",
        "join",
        "group_by",
    ):
        _patch_lazyframe_method(method_name)

    _patch_group_by_agg()
    _PATCHED = True


@pl.api.register_lazyframe_namespace("lineage")
class LazyFrameLineageNamespace:
    def __init__(self, lazyframe: pl.LazyFrame) -> None:
        self._lazyframe = lazyframe
        _enable_lineage_propagation()

    def add_source(
        self,
        *,
        name: str,
        uri: str,
        destination_table: str | None = None,
    ) -> pl.LazyFrame:
        if not name.strip():
            raise ValueError("name must be non-empty")
        if not uri.strip():
            raise ValueError("uri must be non-empty")

        normalized_sources = {name: _source_fqn_from_metadata(name, uri)}
        mapping = MappingConfig(
            sources=normalized_sources,
            destination_table=destination_table or _default_destination_fqn(normalized_sources),
        )
        set_mapping(self._lazyframe, mapping)
        return self._lazyframe

    def extract(self) -> list[dict[str, Any]]:
        mapping = require_mapping(self._lazyframe)
        return extract_lineage_payloads_from_lazyframe(self._lazyframe, mapping)


def register_lineage_namespace() -> None:
    _enable_lineage_propagation()
