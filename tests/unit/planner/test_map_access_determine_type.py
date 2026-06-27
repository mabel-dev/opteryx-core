import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.exceptions import IncorrectTypeError
from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner.binder.operator_map import determine_type
from opteryx.types.logical_type import ARRAY, INT64, VARCHAR
from opteryx.types.schema import ConstantColumn, SchemaColumn


def _literal(value_type, value):
    return Node(
        NodeType.LITERAL,
        type=value_type,
        value=value,
        schema_column=ConstantColumn(name="literal", column_type=value_type, value=value),
    )


def _identifier(value_type):
    column = SchemaColumn(name="col", column_type=value_type, identity="col")
    return Node(NodeType.IDENTIFIER, schema_column=column)


def test_determine_type_map_access_array_returns_element_type():
    left = _identifier(ARRAY(INT64))
    right = _literal(INT64, 0)
    node = Node(NodeType.BINARY_OPERATOR, value="MapAccess", left=left, right=right)

    assert determine_type(node) == INT64


def test_determine_type_map_access_varchar_returns_varchar():
    left = _identifier(VARCHAR)
    right = _literal(INT64, 1)
    node = Node(NodeType.BINARY_OPERATOR, value="MapAccess", left=left, right=right)

    assert determine_type(node) == VARCHAR


def test_determine_type_map_access_varchar_subscript_by_string_raises():
    left = _identifier(VARCHAR)
    right = _literal(VARCHAR, "1")
    node = Node(NodeType.BINARY_OPERATOR, value="MapAccess", left=left, right=right)

    with pytest.raises(IncorrectTypeError):
        determine_type(node)


def test_determine_type_map_access_invalid_types_raise():
    left = _identifier(INT64)
    right = _literal(INT64, 1)
    node = Node(NodeType.BINARY_OPERATOR, value="MapAccess", left=left, right=right)

    with pytest.raises(IncorrectTypeError):
        determine_type(node)
