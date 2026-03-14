"""Helpers for exporting a generated operator catalog from the binder matrix."""

from __future__ import annotations

import json
from collections import OrderedDict
from collections import defaultdict
from pathlib import Path
from typing import Any

from orso.types import OrsoTypes

from opteryx.expression.formatter import BINARY_OPERATOR_TOKENS
from opteryx.expression.formatter import COMPARISON_OPERATOR_TOKENS
from opteryx.expression.formatter import EXTRACTION_OPERATOR_TOKENS
from opteryx.expression.operator_catalog import OPERATOR_DEFINITIONS
from opteryx.expression.operator_catalog import OperatorSignatureDefinition
from opteryx.planner.binder.operator_map import OPERATOR_MAP


def _type_id(type_name: OrsoTypes | None) -> str | None:
    if type_name is None or type_name == OrsoTypes._MISSING_TYPE:
        return None
    return type_name.name.lower()


def _operator_token(operator: str) -> str | None:
    metadata = OPERATOR_DEFINITIONS.get(operator)
    if metadata and metadata.token:
        return metadata.token
    if operator == "MapAccess":
        return "[]"
    if operator in BINARY_OPERATOR_TOKENS:
        return BINARY_OPERATOR_TOKENS[operator]
    if operator in EXTRACTION_OPERATOR_TOKENS:
        return EXTRACTION_OPERATOR_TOKENS[operator]
    if operator in COMPARISON_OPERATOR_TOKENS:
        return COMPARISON_OPERATOR_TOKENS[operator]
    return None


def _operator_category(operator: str, result_types: set[str | None]) -> str:
    metadata = OPERATOR_DEFINITIONS.get(operator)
    if metadata and metadata.category:
        return metadata.category
    if operator in {"And", "Or"}:
        return "logical"
    if operator == "MapAccess" or operator in EXTRACTION_OPERATOR_TOKENS:
        return "extraction"
    if operator in BINARY_OPERATOR_TOKENS:
        return "binary"
    non_dynamic_results = {result_type for result_type in result_types if result_type is not None}
    if non_dynamic_results == {"boolean"} or not non_dynamic_results:
        return "comparison"
    return "binary"


def _manual_signature_export(signature: OperatorSignatureDefinition) -> dict[str, Any]:
    return {
        "left_type": _type_id(signature.left_type),
        "right_type": _type_id(signature.right_type),
        "result_type": _type_id(signature.result_type),
        "result_type_is_dynamic": signature.result_type is None,
        "cost_estimate": signature.cost_estimate,
    }


def export_operator_catalog() -> OrderedDict[str, dict[str, Any]]:
    grouped: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for (left_type, right_type, operator), metadata in OPERATOR_MAP.items():
        grouped[operator].append(
            {
                "left_type": _type_id(left_type),
                "right_type": _type_id(right_type),
                "result_type": _type_id(metadata.result_type),
                "result_type_is_dynamic": metadata.result_type == OrsoTypes._MISSING_TYPE,
                "cost_estimate": metadata.cost_estimate,
            }
        )

    for operator, definition in OPERATOR_DEFINITIONS.items():
        for signature in definition.signatures:
            grouped[operator].append(_manual_signature_export(signature))

    exported: dict[str, dict[str, Any]] = {}
    for operator in sorted(set(grouped) | set(OPERATOR_DEFINITIONS)):
        signatures = grouped.get(operator, [])
        ordered_signatures = sorted(
            signatures,
            key=lambda item: (
                item["left_type"] or "",
                item["right_type"] or "",
                item["result_type"] or "",
            ),
        )
        left_types = sorted({item["left_type"] for item in ordered_signatures if item["left_type"]})
        right_types = sorted(
            {item["right_type"] for item in ordered_signatures if item["right_type"]}
        )
        result_types = sorted(
            {item["result_type"] for item in ordered_signatures if item["result_type"]}
        )
        raw_result_types = {item["result_type"] for item in ordered_signatures}
        metadata = OPERATOR_DEFINITIONS.get(operator)

        entry = {
            "internal_name": operator,
            "display_name": _operator_token(operator) or operator,
            "token": _operator_token(operator),
            "category": _operator_category(operator, raw_result_types),
            "summary": metadata.summary if metadata else operator,
            "documentation": metadata.documentation if metadata else operator,
            "signature_count": len(ordered_signatures),
            "has_dynamic_result": any(
                item["result_type_is_dynamic"] for item in ordered_signatures
            ),
            "left_types": left_types,
            "right_types": right_types,
            "result_types": result_types,
            "signatures": ordered_signatures,
        }
        if metadata and metadata.notes:
            entry["notes"] = metadata.notes
        exported[operator] = entry

    ordered = OrderedDict()
    for name in sorted(exported):
        ordered[name] = exported[name]
    return ordered


def write_operator_catalog(path: str | Path) -> None:
    output_path = Path(path)
    output_path.write_text(
        json.dumps(export_operator_catalog(), indent=4) + "\n",
        encoding="utf8",
    )
