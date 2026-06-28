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


def export_operator_catalog() -> OrderedDict[str, dict[str, Any]]:
    exported: dict[str, dict[str, Any]] = {}
    for operator in sorted(OPERATOR_DEFINITIONS):
        metadata = get_operator_definition(operator)

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
