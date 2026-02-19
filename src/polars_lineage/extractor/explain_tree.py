from __future__ import annotations

import re

from polars_lineage.config import MappingConfig
from polars_lineage.extractor.expr_parser import parse_expression
from polars_lineage.ir import ColumnLineage, ColumnRef, DatasetRef

_DF_COLUMNS_PATTERN = re.compile(r"DF \[(?P<columns>[^\]]+)\]")
_BOX_CHARS = re.compile(r"[┌┐└┘├┤┬┴─╭╮╯╰│]+")
_ALIAS_PATTERN = re.compile(r'\.alias\("([^"]+)"\)')
_COLUMN_PATTERN = re.compile(r'col\("([^"]+)"\)')
_EXPR_BLOCK_PATTERN = re.compile(
    r"expression:\s*(?P<body>.*?)"
    r"(?=(?:expression:|aggregate by:|FROM:|left on:|right on:|LEFT PLAN:|RIGHT PLAN:|$))"
)
_AGG_BY_PATTERN = re.compile(
    r"aggregate by:\s*(?P<body>.*?)"
    r"(?=(?:expression:|FROM:|left on:|right on:|LEFT PLAN:|RIGHT PLAN:|$))"
)
_LEFT_JOIN_SIDE_PATTERN = re.compile(
    r"left on:\s*(?P<body>.*?)"
    r"(?=(?:right on:|LEFT PLAN:|RIGHT PLAN:|expression:|aggregate by:|FROM:|$))"
)
_RIGHT_JOIN_SIDE_PATTERN = re.compile(
    r"right on:\s*(?P<body>.*?)"
    r"(?=(?:LEFT PLAN:|RIGHT PLAN:|expression:|aggregate by:|FROM:|$))"
)


def _normalize_block(value: str) -> str:
    text = _BOX_CHARS.sub(" ", value)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _column_texts_from_tree(plan: str) -> dict[int, str]:
    column_tokens: dict[int, list[str]] = {}
    for raw_line in plan.splitlines():
        if "│" not in raw_line:
            continue
        parts = raw_line.split("│")
        if len(parts) < 3:
            continue
        for column_index, part in enumerate(parts[2:], start=0):
            cleaned = _normalize_block(part)
            if not cleaned or cleaned.isdigit():
                continue
            column_tokens.setdefault(column_index, []).append(cleaned)

    return {column_index: " ".join(tokens) for column_index, tokens in column_tokens.items()}


def _parse_join_keys(column_texts: dict[int, str]) -> tuple[set[str], set[str]]:
    left_join_keys: set[str] = set()
    right_join_keys: set[str] = set()
    for text in column_texts.values():
        for side_match in _LEFT_JOIN_SIDE_PATTERN.finditer(text):
            left_join_keys.update(_COLUMN_PATTERN.findall(side_match.group("body")))
        for side_match in _RIGHT_JOIN_SIDE_PATTERN.finditer(text):
            right_join_keys.update(_COLUMN_PATTERN.findall(side_match.group("body")))
    return left_join_keys, right_join_keys


def _parse_datasets(
    column_texts: dict[int, str], mapping: MappingConfig
) -> tuple[
    DatasetRef,
    dict[str, tuple[DatasetRef, ...]],
    set[str],
    set[str],
    DatasetRef | None,
    DatasetRef | None,
]:
    source_by_alias = {alias: DatasetRef.from_fqn(fqn) for alias, fqn in mapping.sources.items()}
    source_datasets = list(source_by_alias.values())
    if not source_datasets:
        raise ValueError("at least one source dataset is required")

    combined_text = " ".join(column_texts.values())
    join_count = len(re.findall(r"\bJOIN\b", combined_text))
    if join_count > 1:
        raise ValueError("multiple joins are not supported yet")
    if join_count == 1 and not {"left", "right"}.issubset(source_by_alias):
        raise ValueError("join plans require left/right source aliases in mapping.sources")

    left_dataset = source_by_alias.get("left")
    right_dataset = source_by_alias.get("right")
    if left_dataset is None and source_datasets:
        left_dataset = source_datasets[0]
    if right_dataset is None and len(source_datasets) > 1:
        right_dataset = source_datasets[1]

    destination_dataset = DatasetRef.from_fqn(mapping.destination_table)
    left_join_keys, right_join_keys = _parse_join_keys(column_texts)

    df_matches: list[tuple[int, re.Match[str]]] = []
    for column_index, text in column_texts.items():
        for match in _DF_COLUMNS_PATTERN.finditer(text):
            df_matches.append((column_index, match))

    if not df_matches:
        return destination_dataset, {}, left_join_keys, right_join_keys, left_dataset, right_dataset

    namespace_map: dict[str, set[DatasetRef]] = {}
    fallback_index = 0
    for _, match in sorted(df_matches, key=lambda item: item[0]):
        text = match.string

        left_marker = text.rfind("LEFT PLAN:", 0, match.start())
        right_marker = text.rfind("RIGHT PLAN:", 0, match.start())

        if left_marker > right_marker and left_dataset is not None:
            dataset = left_dataset
        elif right_marker > left_marker and right_dataset is not None:
            dataset = right_dataset
        elif len(source_datasets) == 1:
            dataset = source_datasets[0]
        else:
            dataset = source_datasets[min(fallback_index, len(source_datasets) - 1)]
            fallback_index += 1

        column_values = [item.strip().strip('"') for item in match.group("columns").split(",")]
        for column in column_values:
            namespace_map.setdefault(column, set()).add(dataset)

    normalized_map = {
        column: tuple(sorted(candidates, key=lambda item: item.fqn))
        for column, candidates in namespace_map.items()
    }

    return (
        destination_dataset,
        normalized_map,
        left_join_keys,
        right_join_keys,
        left_dataset,
        right_dataset,
    )


def extract_plan_lineage(plan: str, mapping: MappingConfig) -> list[ColumnLineage]:
    column_texts = _column_texts_from_tree(plan)
    (
        destination_dataset,
        namespace_map,
        left_join_keys,
        right_join_keys,
        left_dataset,
        right_dataset,
    ) = _parse_datasets(column_texts, mapping)

    parsed_blocks: list[tuple[str, str]] = []
    for column_text in column_texts.values():
        for expression_match in _EXPR_BLOCK_PATTERN.finditer(column_text):
            expression_text = expression_match.group("body").strip()
            if not expression_text:
                continue
            alias_match = _ALIAS_PATTERN.search(expression_text)
            if alias_match:
                destination_column = alias_match.group(1)
                expression = expression_text[: alias_match.start()].strip()
            else:
                destination_match = _COLUMN_PATTERN.search(expression_text)
                if destination_match is None:
                    continue
                destination_column = destination_match.group(1)
                expression = expression_text
            parsed_blocks.append((destination_column, expression))

        for aggregate_match in _AGG_BY_PATTERN.finditer(column_text):
            aggregate_text = aggregate_match.group("body")
            for aggregate_column in _COLUMN_PATTERN.findall(aggregate_text):
                parsed_blocks.append((aggregate_column, f'col("{aggregate_column}")'))

    parsed_blocks = list(dict.fromkeys(parsed_blocks))

    derived_columns = {destination_column for destination_column, _ in parsed_blocks}

    lineage: list[ColumnLineage] = []
    for destination_column, expression in parsed_blocks:
        parsed = parse_expression(expression)

        source_columns: list[ColumnRef] = []
        for source_column_name in parsed.columns:
            source_candidates = namespace_map.get(source_column_name)
            source_dataset: DatasetRef
            if source_candidates is None:
                if source_column_name in derived_columns:
                    source_dataset = destination_dataset
                else:
                    raise ValueError(f"unresolved source column: {source_column_name}")
            elif len(source_candidates) == 1:
                source_dataset = source_candidates[0]
            elif (
                source_column_name in left_join_keys
                and source_column_name in right_join_keys
                and left_dataset in source_candidates
            ):
                source_dataset = left_dataset
            elif source_column_name in left_join_keys and left_dataset in source_candidates:
                source_dataset = left_dataset
            elif source_column_name in right_join_keys and right_dataset in source_candidates:
                source_dataset = right_dataset
            elif source_column_name in left_join_keys | right_join_keys:
                source_dataset = source_candidates[0]
            else:
                candidate_tables = ", ".join(item.fqn for item in source_candidates)
                raise ValueError(
                    f"ambiguous source column: {source_column_name} candidates={candidate_tables}"
                )
            source_columns.append(ColumnRef(dataset=source_dataset, column=source_column_name))

        lineage.append(
            ColumnLineage(
                from_columns=tuple(source_columns),
                to_column=ColumnRef(dataset=destination_dataset, column=destination_column),
                function=parsed.function,
                confidence=parsed.confidence,
            )
        )

    return sorted(lineage, key=lambda item: item.to_column.column)
