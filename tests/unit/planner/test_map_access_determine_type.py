import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from orso.schema import ConstantColumn
from orso.schema import FlatColumn
from orso.types import OrsoTypes

from opteryx.exceptions import IncorrectTypeError
from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.planner.binder.operator_map import determine_type


def _literal(value_type, value):
    return Node(
        NodeType.LITERAL,
        type=value_type,
        value=value,
        schema_column=ConstantColumn(name="literal", type=value_type, value=value),
    )


def _identifier(value_type, *, element_type=None):
    column = FlatColumn(name="col", type=value_type, element_type=element_type)
    column.identity = "col"
    return Node(NodeType.IDENTIFIER, schema_column=column)


def test_determine_type_map_access_array_returns_element_type():
    left = _identifier(OrsoTypes.ARRAY, element_type=OrsoTypes.INTEGER)
    right = _literal(OrsoTypes.INTEGER, 0)
    node = Node(NodeType.BINARY_OPERATOR, value="MapAccess", left=left, right=right)

    assert determine_type(node) == OrsoTypes.INTEGER


def test_determine_type_map_access_varchar_returns_varchar():
    left = _identifier(OrsoTypes.VARCHAR)
    right = _literal(OrsoTypes.INTEGER, 1)
    node = Node(NodeType.BINARY_OPERATOR, value="MapAccess", left=left, right=right)

    assert determine_type(node) == OrsoTypes.VARCHAR


def test_determine_type_map_access_varchar_subscript_by_string_raises():
    left = _identifier(OrsoTypes.VARCHAR)
    right = _literal(OrsoTypes.VARCHAR, "1")
    node = Node(NodeType.BINARY_OPERATOR, value="MapAccess", left=left, right=right)

    with pytest.raises(IncorrectTypeError):
        determine_type(node)


def test_determine_type_map_access_invalid_types_raise():
    left = _identifier(OrsoTypes.INTEGER)
    right = _literal(OrsoTypes.INTEGER, 1)
    node = Node(NodeType.BINARY_OPERATOR, value="MapAccess", left=left, right=right)

    with pytest.raises(IncorrectTypeError):
        determine_type(node)
