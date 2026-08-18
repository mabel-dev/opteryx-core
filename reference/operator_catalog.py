"""Helpers for exporting a generated operator catalog from the binder matrix."""

from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from typing import Any

from opteryx.expression.operator_catalog import OPERATOR_DEFINITIONS
from opteryx.expression.operator_catalog import default_operator_friendly_name
from opteryx.expression.operator_catalog import get_operator_definition
from opteryx.expression.operator_catalog import get_operator_sql_symbol
from opteryx.planner.binder.operator_map import OPERATOR_MAP

# The published operator reference is two halves, and neither is hand-written twice.
#
# * WHAT IT MEANS - the prose, the operand names, the syntax forms, the examples -
#   comes from `OPERATOR_DEFINITIONS`. None of it is derivable from code.
# * WHICH TYPES IT ACCEPTS comes from `OPERATOR_MAP`, the binder's 317-row type
#   matrix. That is the table the binder actually consults, so a docs page built
#   from it cannot claim a type combination the engine rejects, and cannot miss
#   one it accepts. Restating those types by hand next to the prose is the drift
#   this split exists to prevent.
#
# The one exception is the logical operators. `determine_type()` answers AND/OR/XOR
# from the NODE TYPE and returns BOOLEAN without ever reaching the map, so the map's
# `(BOOLEAN, BOOLEAN, 'And')` and `('Or')` rows are never consulted and there is no
# `'Xor'` row at all. Deriving from the map would therefore publish two logical
# operators with a signature and a third with none, which describes the table rather
# than the engine. `_LOGICAL_SIGNATURE` states the node-type rule instead.
_LOGICAL_SIGNATURE = {
    "left_type": "boolean",
    "right_type": "boolean",
    "result_type": "boolean",
    "result_type_is_dynamic": False,
    "cost_estimate": 100.0,
}


def _category_name(category) -> str:
    """`LogicalCategory.VARCHAR` -> `"varchar"` - the spelling `types.json` is keyed by."""
    return category.name.lower()


def _operator_signatures(operator: str) -> list[dict[str, Any]]:
    """Every (left, right) -> result the binder accepts for *operator*."""
    definition = get_operator_definition(operator)
    if definition is not None and definition.node_kind == "logical":
        return [dict(_LOGICAL_SIGNATURE)]

    signatures = []
    for (left, right, name), entry in OPERATOR_MAP.items():
        if name != operator:
            continue
        result = entry.result_type
        signatures.append(
            {
                "left_type": _category_name(left),
                "right_type": _category_name(right),
                # A None result type is the binder saying the answer depends on the
                # value, not that it does not know - `ARRAY<T>[i]` is T, resolved per
                # expression. That is what `result_type_is_dynamic` tells a reader.
                "result_type": None if result is None else _category_name(result.category),
                "result_type_is_dynamic": result is None,
                "cost_estimate": entry.cost_estimate,
            }
        )
    signatures.sort(key=lambda signature: (signature["left_type"], signature["right_type"]))
    return signatures


def _unique(values) -> list[str]:
    return sorted({value for value in values if value is not None})


def export_operator_catalog() -> OrderedDict[str, dict[str, Any]]:
    exported: dict[str, dict[str, Any]] = {}
    for operator in sorted(OPERATOR_DEFINITIONS):
        metadata = get_operator_definition(operator)
        signatures = _operator_signatures(operator)

        left_types = _unique(signature["left_type"] for signature in signatures)
        right_types = _unique(signature["right_type"] for signature in signatures)

        entry: dict[str, Any] = {
            "ast_symbol": operator,
            "friendly_name": (
                metadata.friendly_name
                if metadata and metadata.friendly_name
                else default_operator_friendly_name(operator)
            ),
            "sql_symbol": get_operator_sql_symbol(operator),
            "node_kind": metadata.node_kind if metadata else None,
            "category": metadata.category if metadata else None,
            "description": metadata.summary if metadata else operator,
            "documentation": metadata.documentation if metadata else operator,
            "implemented": metadata.implemented if metadata else True,
            "syntax_forms": list(metadata.syntax_forms) if metadata else [],
            # The operand list is the join of the two halves: the name and the prose
            # from the catalog, the accepted types from the binder matrix.
            "operands": [
                {
                    "name": operand.name,
                    "documentation": operand.documentation,
                    "constant_only": operand.constant_only,
                    "types": left_types if position == 0 else right_types,
                }
                for position, operand in enumerate(metadata.operands if metadata else ())
            ],
            # Each example carries the answer the engine gave when the catalog was
            # written; tests/unit/test_documentation.py re-runs them and fails if it
            # no longer gives it, so the page cannot show a stale result.
            "examples": [
                {"sql": example.sql, "result": list(example.result)}
                for example in (metadata.examples if metadata else ())
            ],
            "see_also": list(metadata.see_also) if metadata else [],
            "left_types": left_types,
            "right_types": right_types,
            "result_types": _unique(signature["result_type"] for signature in signatures),
            "has_dynamic_result": any(
                signature["result_type_is_dynamic"] for signature in signatures
            ),
            "signature_count": len(signatures),
            "signatures": signatures,
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
