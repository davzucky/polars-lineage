from __future__ import annotations

import weakref

import polars as pl

from polars_lineage.config import MappingConfig

_MAPPINGS_BY_ID: dict[int, MappingConfig] = {}
_REFS_BY_ID: dict[int, weakref.ref[pl.LazyFrame]] = {}


def _cleanup(reference: weakref.ref[pl.LazyFrame]) -> None:
    dead_key: int | None = None
    for key, value in _REFS_BY_ID.items():
        if value is reference:
            dead_key = key
            break
    if dead_key is None:
        return
    _REFS_BY_ID.pop(dead_key, None)
    _MAPPINGS_BY_ID.pop(dead_key, None)


def set_mapping(lazyframe: pl.LazyFrame, mapping: MappingConfig) -> None:
    key = id(lazyframe)
    _MAPPINGS_BY_ID[key] = mapping
    if key not in _REFS_BY_ID:
        _REFS_BY_ID[key] = weakref.ref(lazyframe, _cleanup)


def get_mapping(lazyframe: pl.LazyFrame) -> MappingConfig | None:
    return _MAPPINGS_BY_ID.get(id(lazyframe))


def require_mapping(lazyframe: pl.LazyFrame) -> MappingConfig:
    mapping = get_mapping(lazyframe)
    if mapping is None:
        raise ValueError(
            "lineage metadata not found; call "
            "`lazyframe.lineage.add_source(name=..., uri=...)` first"
        )
    return mapping
