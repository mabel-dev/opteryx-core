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
        LogicalCategory.FLOAT,
        LogicalCategory.DECIMAL,
    }
)


def resolve_node_type(node) -> tuple[Optional[LogicalCategory], Optional[LogicalCategory]]:
    """Return the logical type and element type carried by a node."""
    from opteryx.compiled.structures.expressions import expressions_with
    from opteryx.types.logical_type import ColumnType

    schema_column = node.schema_column

    if schema_column is not None and schema_column.category is not None:
        node_type = schema_column.category
    else:
        _raw_type = node.type if type(node) in expressions_with("type") else None
        # Phase 2: node.type is ColumnType; extract category for LogicalCategory callers.
        node_type = _raw_type.category if isinstance(_raw_type, ColumnType) else _raw_type

    return node_type, None


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
    _ntype = node.type
    _ncat = _ntype.category if isinstance(_ntype, _ColumnType) else _ntype
    if _ncat == LogicalCategory.VECTOR:
        return True
    value = node.value

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
    parameters = node.parameters
    if len(parameters) != 1:
        return False
    argument = parameters[0]
    if argument.node_type != NodeType.LITERAL:
        return False
    arg_type, _ = resolve_node_type(argument)
    return arg_type in (LogicalCategory.VARCHAR, LogicalCategory.VARBINARY) or isinstance(
        argument.value, (str, bytes, bytearray)
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
        and node.value in {"VECTOR", "TRY_VECTOR"}
        and node.left is not None
        and node.left.node_type == NodeType.IDENTIFIER
    ):
        return node.left
    return None
