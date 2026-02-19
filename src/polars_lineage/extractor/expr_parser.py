from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

_COLUMN_PATTERN = re.compile(r'col\("([^"]+)"\)')
_CALL_PATTERN = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\(")


@dataclass(frozen=True)
class ParsedExpression:
    columns: tuple[str, ...]
    function: str
    confidence: Literal["exact", "inferred", "unknown"]


def parse_expression(expression: str) -> ParsedExpression:
    normalized = " ".join(expression.split())
    columns = tuple(sorted(set(_COLUMN_PATTERN.findall(normalized))))

    if normalized.startswith("dyn ") or normalized in {"len()", "null"}:
        return ParsedExpression(columns=(), function=normalized, confidence="exact")

    known_tokens = (
        "col(",
        ".alias(",
        ".sum()",
        ".mean()",
        ".avg()",
        ".min()",
        ".max()",
        ".count()",
        ".fill_null(",
        "coalesce",
        "when",
        "then",
        "otherwise",
        "+",
        "-",
        "*",
        "/",
        "==",
        "!=",
        ">=",
        "<=",
        ">",
        "<",
    )

    if any(token in normalized for token in known_tokens):
        known_calls = {
            "col",
            "coalesce",
            "when",
            "then",
            "otherwise",
            "len",
            "fill_null",
            "sum",
            "mean",
            "avg",
            "min",
            "max",
            "count",
            "alias",
        }
        calls = {match.group(1) for match in _CALL_PATTERN.finditer(normalized)}
        if calls and not calls.issubset(known_calls):
            return ParsedExpression(columns=columns, function=normalized, confidence="inferred")
        return ParsedExpression(columns=columns, function=normalized, confidence="exact")

    if columns:
        return ParsedExpression(columns=columns, function=normalized, confidence="inferred")

    return ParsedExpression(columns=(), function=normalized, confidence="unknown")
