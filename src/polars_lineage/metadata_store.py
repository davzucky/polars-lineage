from __future__ import annotations

import weakref
from functools import partial

import polars as pl

from polars_lineage.config import MappingConfig

_MAPPINGS_BY_ID: dict[int, MappingConfig] = {}
_REFS_BY_ID: dict[int, weakref.ref[pl.LazyFrame]] = {}


def _cleanup_by_key(key: int) -> None:
    _REFS_BY_ID.pop(key, None)
    _MAPPINGS_BY_ID.pop(key, None)


def _cleanup_with_ref(_key: int, _reference: weakref.ReferenceType[pl.LazyFrame]) -> None:
    _cleanup_by_key(_key)


def set_mapping(lazyframe: pl.LazyFrame, mapping: MappingConfig) -> None:
    key = id(lazyframe)
    _MAPPINGS_BY_ID[key] = mapping
    if key not in _REFS_BY_ID:
        _REFS_BY_ID[key] = weakref.ref(lazyframe, partial(_cleanup_with_ref, key))


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
