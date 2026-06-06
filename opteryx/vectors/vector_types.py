# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Helpers for the engine's vector-compatible type semantics."""

from __future__ import annotations

from typing import Optional

from opteryx.types.logical_type import LogicalCategory

NUMERIC_VECTOR_ELEMENT_TYPES = frozenset(
    {
        LogicalCategory.INTEGER,
        LogicalCategory.DOUBLE,
        LogicalCategory.DECIMAL,
    }
)


def resolve_node_type(node) -> tuple[Optional[LogicalCategory], Optional[LogicalCategory]]:
    """Return the logical type and element type carried by a node."""
    from opteryx.types.logical_type import ColumnType

    schema_column = getattr(node, "schema_column", None)

    if schema_column is not None and getattr(schema_column, "category", None) is not None:
        node_type = getattr(schema_column, "category", None)
    else:
        _raw_type = getattr(node, "type", None)
        # Phase 2: node.type is ColumnType; extract category for LogicalCategory callers.
        node_type = _raw_type.category if isinstance(_raw_type, ColumnType) else _raw_type

    if schema_column is not None and getattr(schema_column, "element_type", None) is not None:
        element_type = getattr(schema_column, "element_type", None)
    else:
        element_type = getattr(node, "element_type", None)

    return node_type, element_type


def is_numeric_vector_type(
    value_type: Optional[LogicalCategory], element_type: Optional[LogicalCategory]
) -> bool:
    """True when the type pair represents a VECTOR."""
    del element_type
    return value_type == LogicalCategory.VECTOR


def node_is_numeric_vector(node) -> bool:
    """True when the node is typed as a numeric vector."""
    return is_numeric_vector_type(*resolve_node_type(node))


def node_is_literal_numeric_vector(node) -> bool:
    """True when the node is a numeric vector literal, even if element_type is not populated."""
    from opteryx.expression import NodeType

    if node is None or node.node_type != NodeType.LITERAL:
        return False
    from opteryx.types.logical_type import ColumnType as _ColumnType
    _ntype = getattr(node, "type", None)
    _ncat = _ntype.category if isinstance(_ntype, _ColumnType) else _ntype
    if _ncat == LogicalCategory.VECTOR:
        return True
    value = getattr(node, "value", None)

    # Check if value is a sequence type (list, tuple, or array-like)
    # Handle numpy arrays without importing numpy by checking class name
    if value is None:
        return False

    value_type_name = type(value).__name__

    # Accept lists, tuples, and numpy arrays (detected by class name)
    if not isinstance(value, (list, tuple)) and value_type_name != "ndarray":
        return False

    # Check if it's a non-empty sequence of numeric values
    try:
        if len(value) == 0:
            return False

        # Try to convert all elements to float to verify they're numeric
        # This is a lightweight check without numpy dependency
        for elem in value:
            float(elem)  # Will raise if not numeric
        return True
    except (TypeError, ValueError):
        return False


def node_is_constant_embed_call(node) -> bool:
    """True when the node is EMBED(<string-literal>)."""
    from opteryx.expression import NodeType

    if node is None or node.node_type != NodeType.FUNCTION or node.value != "EMBED":
        return False
    parameters = getattr(node, "parameters", ())
    if len(parameters) != 1:
        return False
    argument = parameters[0]
    if argument.node_type != NodeType.LITERAL:
        return False
    arg_type, _ = resolve_node_type(argument)
    return arg_type in (LogicalCategory.VARCHAR, LogicalCategory.BLOB) or isinstance(
        getattr(argument, "value", None), (str, bytes, bytearray)
    )


def node_is_vector_query_expression(node) -> bool:
    """True when the node can supply a query vector to vector search operators."""
    return (
        node_is_numeric_vector(node)
        or node_is_literal_numeric_vector(node)
        or node_is_constant_embed_call(node)
    )


def get_vector_source_identifier(node):
    """Return the identifier behind a vector source expression, if any."""
    from opteryx.expression import NodeType

    if node is None:
        return None
    if node.node_type == NodeType.IDENTIFIER and node_is_numeric_vector(node):
        return node
    if (
        node.node_type == NodeType.CAST
        and getattr(node, "value", None) in {"VECTOR", "TRY_VECTOR"}
        and getattr(node, "left", None) is not None
        and node.left.node_type == NodeType.IDENTIFIER
    ):
        return node.left
    return None
