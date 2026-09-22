"""Helpers for exporting unary predicate capabilities for documentation."""

from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from typing import Any

from reference.precedence_catalog import UNARY_OPS
from reference.precedence_catalog import check_precedence_coverage
from reference.precedence_catalog import precedence_for

UNARY_OPERATION_DEFINITIONS = {
    "IsJsonValue": {
        "ast_symbol": "IsJsonValue",
        "category": "json_predicate",
        "documentation": "Returns true when the input is JSON text parsing to any well-formed JSON document.",
        "implementation": "draken_is_json_value",
        "node_type": "UNARY_OPERATOR",
        "notes": "Total predicate — never returns NULL. Validates JSON TEXT without building a document. WITH/WITHOUT UNIQUE KEYS is parsed but refused.",
        "operand_type": "json_text",
        "status": "active",
        "summary": "JSON value well-formedness test.",
        "syntax_forms": ['expr IS JSON', 'expr IS JSON VALUE'],
    },
    "IsNotJsonValue": {
        "ast_symbol": "IsNotJsonValue",
        "category": "json_predicate",
        "documentation": "Returns true when the input is NOT JSON text parsing to any well-formed JSON document. A malformed document, and a NULL, are both true.",
        "implementation": "draken_is_not_json_value",
        "node_type": "UNARY_OPERATOR",
        "notes": "Total predicate — never returns NULL. Validates JSON TEXT without building a document. WITH/WITHOUT UNIQUE KEYS is parsed but refused.",
        "operand_type": "json_text",
        "status": "active",
        "summary": "Negated JSON value well-formedness test.",
        "syntax_forms": ['expr IS NOT JSON', 'expr IS NOT JSON VALUE'],
    },
    "IsJsonScalar": {
        "ast_symbol": "IsJsonScalar",
        "category": "json_predicate",
        "documentation": "Returns true when the input is JSON text parsing to a JSON scalar — a string, number, boolean or null.",
        "implementation": "draken_is_json_scalar",
        "node_type": "UNARY_OPERATOR",
        "notes": "Total predicate — never returns NULL. Validates JSON TEXT without building a document. WITH/WITHOUT UNIQUE KEYS is parsed but refused.",
        "operand_type": "json_text",
        "status": "active",
        "summary": "JSON scalar well-formedness test.",
        "syntax_forms": ['expr IS JSON SCALAR'],
    },
    "IsNotJsonScalar": {
        "ast_symbol": "IsNotJsonScalar",
        "category": "json_predicate",
        "documentation": "Returns true when the input is NOT JSON text parsing to a JSON scalar — a string, number, boolean or null. A malformed document, and a NULL, are both true.",
        "implementation": "draken_is_not_json_scalar",
        "node_type": "UNARY_OPERATOR",
        "notes": "Total predicate — never returns NULL. Validates JSON TEXT without building a document. WITH/WITHOUT UNIQUE KEYS is parsed but refused.",
        "operand_type": "json_text",
        "status": "active",
        "summary": "Negated JSON scalar well-formedness test.",
        "syntax_forms": ['expr IS NOT JSON SCALAR'],
    },
    "IsJsonArray": {
        "ast_symbol": "IsJsonArray",
        "category": "json_predicate",
        "documentation": "Returns true when the input is JSON text parsing to a JSON array.",
        "implementation": "draken_is_json_array",
        "node_type": "UNARY_OPERATOR",
        "notes": "Total predicate — never returns NULL. Validates JSON TEXT without building a document. WITH/WITHOUT UNIQUE KEYS is parsed but refused.",
        "operand_type": "json_text",
        "status": "active",
        "summary": "JSON array well-formedness test.",
        "syntax_forms": ['expr IS JSON ARRAY'],
    },
    "IsNotJsonArray": {
        "ast_symbol": "IsNotJsonArray",
        "category": "json_predicate",
        "documentation": "Returns true when the input is NOT JSON text parsing to a JSON array. A malformed document, and a NULL, are both true.",
        "implementation": "draken_is_not_json_array",
        "node_type": "UNARY_OPERATOR",
        "notes": "Total predicate — never returns NULL. Validates JSON TEXT without building a document. WITH/WITHOUT UNIQUE KEYS is parsed but refused.",
        "operand_type": "json_text",
        "status": "active",
        "summary": "Negated JSON array well-formedness test.",
        "syntax_forms": ['expr IS NOT JSON ARRAY'],
    },
    "IsJsonObject": {
        "ast_symbol": "IsJsonObject",
        "category": "json_predicate",
        "documentation": "Returns true when the input is JSON text parsing to a JSON object.",
        "implementation": "draken_is_json_object",
        "node_type": "UNARY_OPERATOR",
        "notes": "Total predicate — never returns NULL. Validates JSON TEXT without building a document. WITH/WITHOUT UNIQUE KEYS is parsed but refused.",
        "operand_type": "json_text",
        "status": "active",
        "summary": "JSON object well-formedness test.",
        "syntax_forms": ['expr IS JSON OBJECT'],
    },
    "IsNotJsonObject": {
        "ast_symbol": "IsNotJsonObject",
        "category": "json_predicate",
        "documentation": "Returns true when the input is NOT JSON text parsing to a JSON object. A malformed document, and a NULL, are both true.",
        "implementation": "draken_is_not_json_object",
        "node_type": "UNARY_OPERATOR",
        "notes": "Total predicate — never returns NULL. Validates JSON TEXT without building a document. WITH/WITHOUT UNIQUE KEYS is parsed but refused.",
        "operand_type": "json_text",
        "status": "active",
        "summary": "Negated JSON object well-formedness test.",
        "syntax_forms": ['expr IS NOT JSON OBJECT'],
    },
    "IsFalse": {
        "ast_symbol": "IsFalse",
        "category": "boolean_predicate",
        "documentation": "Returns true when the input evaluates to false.",
        "implementation": "opteryx.expression.unary_operations._is_false",
        "node_type": "UNARY_OPERATOR",
        "notes": "Requires a boolean operand.",
        "operand_type": "boolean",
        "status": "active",
        "summary": "Boolean false test.",
        "syntax_forms": ["expr IS FALSE"],
    },
    "IsNotFalse": {
        "ast_symbol": "IsNotFalse",
        "category": "boolean_predicate",
        "documentation": "Returns true when the input is not false.",
        "implementation": "opteryx.expression.unary_operations._is_not_false",
        "node_type": "UNARY_OPERATOR",
        "notes": "Requires a boolean operand.",
        "operand_type": "boolean",
        "status": "active",
        "summary": "Boolean is-not-false test.",
        "syntax_forms": ["expr IS NOT FALSE"],
    },
    "IsNotNull": {
        "ast_symbol": "IsNotNull",
        "category": "null_predicate",
        "documentation": "Returns true when the input is not null.",
        "implementation": "opteryx.expression.unary_operations._is_not_null",
        "node_type": "UNARY_OPERATOR",
        "notes": "Vectorized null check.",
        "operand_type": "any",
        "status": "active",
        "summary": "Null negation test.",
        "syntax_forms": ["expr IS NOT NULL"],
    },
    "IsNotTrue": {
        "ast_symbol": "IsNotTrue",
        "category": "boolean_predicate",
        "documentation": "Returns true when the input is not true.",
        "implementation": "opteryx.expression.unary_operations._is_not_true",
        "node_type": "UNARY_OPERATOR",
        "notes": "Requires a boolean operand.",
        "operand_type": "boolean",
        "status": "active",
        "summary": "Boolean is-not-true test.",
        "syntax_forms": ["expr IS NOT TRUE"],
    },
    "IsNull": {
        "ast_symbol": "IsNull",
        "category": "null_predicate",
        "documentation": "Returns true when the input is null.",
        "implementation": "opteryx.expression.unary_operations._is_null",
        "node_type": "UNARY_OPERATOR",
        "notes": "Vectorized null check.",
        "operand_type": "any",
        "status": "active",
        "summary": "Null test.",
        "syntax_forms": ["expr IS NULL"],
    },
    "IsTrue": {
        "ast_symbol": "IsTrue",
        "category": "boolean_predicate",
        "documentation": "Returns true when the input evaluates to true.",
        "implementation": "opteryx.expression.unary_operations._is_true",
        "node_type": "UNARY_OPERATOR",
        "notes": "Requires a boolean operand.",
        "operand_type": "boolean",
        "status": "active",
        "summary": "Boolean true test.",
        "syntax_forms": ["expr IS TRUE"],
    },
    "Not": {
        "ast_symbol": "Not",
        "category": "logical",
        "documentation": "Logical negation of a boolean expression.",
        "implementation": "opteryx.expression.evaluator.draken.evaluate_draken",
        "node_type": "NOT",
        "notes": "Built by the planner as NodeType.NOT, not a standard function call.",
        "operand_type": "boolean",
        "status": "active",
        "summary": "Logical negation.",
        "syntax_forms": ["NOT expr"],
    },
    # The sign operators. Keyed apart from the binary `Minus` / `Plus` in
    # operators.json, which share their spelling but not their binding power.
    "UnaryMinus": {
        "ast_symbol": "Minus",
        "category": "arithmetic",
        "documentation": "Negates a numeric expression.",
        "implementation": "opteryx.planner.logical_planner.logical_planner_builders.unary_op",
        "node_type": "BINARY_OPERATOR",
        "notes": (
            "A numeric literal is negated at plan time (`-5` is the literal -5); any "
            "other operand is lowered to `0 - expr`."
        ),
        "operand_type": "numeric",
        "status": "active",
        "summary": "Numeric negation.",
        "syntax_forms": ["-expr"],
    },
    "UnaryPlus": {
        "ast_symbol": "Plus",
        "category": "arithmetic",
        "documentation": "Returns a numeric expression unchanged.",
        "implementation": "opteryx.planner.logical_planner.logical_planner_builders.unary_op",
        "node_type": "UNARY_OPERATOR",
        "notes": "Removed by the planner; the operand is used as written.",
        "operand_type": "numeric",
        "status": "active",
        "summary": "Numeric identity.",
        "syntax_forms": ["+expr"],
    },
}


def export_unary_ops_catalog() -> OrderedDict[str, dict[str, Any]]:
    check_precedence_coverage(UNARY_OPS, UNARY_OPERATION_DEFINITIONS, required=True)
    exported: dict[str, dict[str, Any]] = {}
    for name in sorted(UNARY_OPERATION_DEFINITIONS):
        exported[name] = dict(
            UNARY_OPERATION_DEFINITIONS[name], precedence=precedence_for(UNARY_OPS, name)
        )

    ordered = OrderedDict()
    for name in sorted(exported):
        ordered[name] = exported[name]
    return ordered


def write_unary_ops_catalog(path: str | Path) -> None:
    output_path = Path(path)
    output_path.write_text(
        json.dumps(export_unary_ops_catalog(), indent=4) + "\n",
        encoding="utf8",
    )
