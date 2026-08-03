# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The AST Rewriter sits between the Parser and the Logical Planner. It transforms the raw
AST produced by sqloxide before any logical plan nodes are created.

Input:  list of AST statement dicts produced by sqloxide
Output: the same list with rewrites applied in-place

Responsibilities:
- Parameter substitution — replaces positional (?) and named (:name) placeholder nodes
  with literal AST nodes built from the caller-supplied parameter values. Done post-parse
  to prevent injection: parameter values never influence how the SQL is parsed.
- JSON accessor rewriting — fixes a sqlparser-rs representation bug where
  `document->(element = value)` is produced instead of `(document->element) = value`.
  Corrected here so the Logical Planner sees well-formed accessor expressions.

The AST Rewriter does NOT resolve column names, look up schemas, or build plan nodes.
"""

import datetime
import decimal
from typing import Any, Dict, List, Union

from opteryx.exceptions import ParameterError

LiteralNode = Dict[str, Any]


def _build_literal_node(value: Any) -> LiteralNode:
    """
    Construct the AST node for a given literal value.

    Parameters:
        value: The literal value to be converted into an AST node.

    Returns:
        A dictionary representing the AST node for the given literal.
    """
    if value is None:
        return {"Value": "Null"}
    elif isinstance(value, bool):
        return {"Value": {"Boolean": value}}
    elif isinstance(value, str):
        return {"Value": {"SingleQuotedString": value}}
    elif isinstance(value, (int, float, decimal.Decimal)):
        return {"Value": {"Number": [value, False]}}
    elif isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
        return {"Value": {"SingleQuotedString": value.isoformat()}}
    else:
        raise ValueError(f"Unsupported literal type: {type(value)}")


def parameter_list_binder(node: Union[Dict, List], parameter_set: List[Any]) -> Union[Dict, List]:
    """
    Recursively walk the AST replacing 'Placeholder' nodes with parameters.

    Parameters:
        node: The AST node or list of nodes.
        parameter_set: The list of parameters to bind.

    Returns:
        The AST with parameters bound.

    Raises:
        ParameterError: If the number of placeholders and parameters do not match.
    """
    if isinstance(node, list):
        return [parameter_list_binder(child, parameter_set) for child in node]

    if isinstance(node, dict):
        if "Value" in node and "Placeholder" in node["Value"]["value"]:
            if node["Value"]["value"]["Placeholder"] != "?":
                raise ParameterError("Parameter lists are only used with qmark (?) parameters.")
            if not parameter_set:
                raise ParameterError(
                    "Incorrect number of bindings supplied. More placeholders than parameters."
                )
            placeholder_value = parameter_set.pop(0)
            if "value" in dir(placeholder_value):
                placeholder_value = placeholder_value.value
            return _build_literal_node(placeholder_value)
        return {k: parameter_list_binder(v, parameter_set) for k, v in node.items()}

    return node  # Leaf node


def parameter_dict_binder(node: Union[Dict, List], parameter_set: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(node, list):
        return [parameter_dict_binder(child, parameter_set) for child in node]

    if isinstance(node, dict):
        if "Placeholder" in node:
            placeholder_name = node["Placeholder"]
            if "value" in dir(placeholder_name):
                placeholder_name = placeholder_name.value
            placeholder_name = placeholder_name[1:]
            if placeholder_name not in parameter_set:
                raise ParameterError(f"Parameter not defined - {placeholder_name}")
            placeholder_value = parameter_set[placeholder_name]
            return _build_literal_node(placeholder_value)
        return {k: parameter_dict_binder(v, parameter_set) for k, v in node.items()}
    return node


def do_ast_rewriter(asts: List[dict], parameters: Union[list, dict]):
    # bind the user provided parameters, we this that here because we want it after the
    # AST has been created (to avoid injection flaws) but also because the order
    # matters
    if isinstance(parameters, list) and len(parameters) > 0:
        with_parameters_exchanged = parameter_list_binder(asts, parameter_set=parameters)
        if len(parameters) != 0:
            raise ParameterError(
                "More parameters were provided than placeholders found in the query."
            )
    elif isinstance(parameters, dict) and len(parameters) > 0:
        with_parameters_exchanged = parameter_dict_binder(asts, parameter_set=parameters or {})
    else:
        with_parameters_exchanged = asts

    return with_parameters_exchanged
