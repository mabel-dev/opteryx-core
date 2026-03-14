# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Helpers for the engine's vector-compatible type semantics."""

from __future__ import annotations

from typing import Optional

import numpy
from orso.types import OrsoTypes

NUMERIC_VECTOR_ELEMENT_TYPES = frozenset(
    {
        OrsoTypes.INTEGER,
        OrsoTypes.DOUBLE,
        OrsoTypes.DECIMAL,
    }
)


def resolve_node_type(node) -> tuple[Optional[OrsoTypes], Optional[OrsoTypes]]:
    """Return the logical type and element type carried by a node."""
    schema_column = getattr(node, "schema_column", None)

    if schema_column is not None and getattr(schema_column, "type", None) not in (
        None,
        OrsoTypes._MISSING_TYPE,
    ):
        node_type = getattr(schema_column, "type", None)
    else:
        node_type = getattr(node, "type", None)

    if schema_column is not None and getattr(schema_column, "element_type", None) not in (
        None,
        OrsoTypes._MISSING_TYPE,
    ):
        element_type = getattr(schema_column, "element_type", None)
    else:
        element_type = getattr(node, "element_type", None)

    return node_type, element_type


def is_numeric_vector_type(
    value_type: Optional[OrsoTypes], element_type: Optional[OrsoTypes]
) -> bool:
    """True when the type pair represents a VECTOR."""
    del element_type
    return value_type == OrsoTypes.VECTOR


def node_is_numeric_vector(node) -> bool:
    """True when the node is typed as a numeric vector."""
    return is_numeric_vector_type(*resolve_node_type(node))


def node_is_literal_numeric_vector(node) -> bool:
    """True when the node is a numeric vector literal, even if element_type is not populated."""
    from opteryx.expression import NodeType

    if node is None or node.node_type != NodeType.LITERAL:
        return False
    if getattr(node, "type", None) == OrsoTypes.VECTOR:
        return True
    value = getattr(node, "value", None)
    if not isinstance(value, (list, tuple, numpy.ndarray)):
        return False
    try:
        vector = numpy.asarray(value, dtype=numpy.float32)
    except (TypeError, ValueError):
        return False
    return vector.ndim == 1 and vector.size > 0


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
    return arg_type in (OrsoTypes.VARCHAR, OrsoTypes.BLOB) or isinstance(
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
