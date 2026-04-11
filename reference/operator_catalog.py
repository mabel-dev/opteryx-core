"""Helpers for exporting a generated operator catalog from the binder matrix."""

from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from typing import Any

from opteryx.expression.operator_catalog import OPERATOR_DEFINITIONS
from opteryx.expression.operator_catalog import OperatorSignatureDefinition
from opteryx.expression.operator_catalog import default_operator_friendly_name
from opteryx.expression.operator_catalog import get_operator_definition
from opteryx.expression.operator_catalog import get_operator_signatures
from opteryx.expression.operator_catalog import get_operator_sql_symbol
from orso.types import OrsoTypes


def _type_id(type_name: OrsoTypes | None) -> str | None:
    if type_name is None or type_name == OrsoTypes._MISSING_TYPE:
        return None
    return type_name.name.lower()


def _operator_category(operator: str, result_types: set[str | None]) -> str:
    metadata = get_operator_definition(operator)
    if metadata and metadata.category:
        return metadata.category
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
    exported: dict[str, dict[str, Any]] = {}
    for operator in sorted(OPERATOR_DEFINITIONS):
        signatures = [
            _manual_signature_export(signature) for signature in get_operator_signatures(operator)
        ]
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
        metadata = get_operator_definition(operator)

        entry = {
            "ast_symbol": operator,
            "friendly_name": (
                metadata.friendly_name
                if metadata and metadata.friendly_name
                else default_operator_friendly_name(operator)
            ),
            "sql_symbol": get_operator_sql_symbol(operator),
            "node_kind": metadata.node_kind if metadata else None,
            "category": _operator_category(operator, raw_result_types),
            "description": metadata.summary if metadata else operator,
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
