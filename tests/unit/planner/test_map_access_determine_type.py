import os
import sys

import pytest
from opteryx.compiled.structures.expressions import BinaryOperator
from opteryx.compiled.structures.expressions import Literal

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.exceptions import IncorrectTypeError
from opteryx.expression import NodeType
from opteryx.planner.binder.operator_map import determine_type
from opteryx.types.logical_type import ARRAY, INT64, VARCHAR
from opteryx.planner.plan_context import PlanContext
from opteryx.compiled.structures.expressions import LogicalColumn
from opteryx.compiled.structures.expressions import ExprArena

# One expression arena for the expressions this module builds outside any query.
_TEST_ARENA = ExprArena()


def _literal(value_type, value):
    return Literal(
        type=value_type,
        value=value,
        schema_column=PlanContext().columns.constant("literal", column_type=value_type, value=value),
        arena=_TEST_ARENA,
    )


def _identifier(value_type):
    column = PlanContext().columns.relation_column("t", "col", column_type=value_type)
    return LogicalColumn(node_type=NodeType.IDENTIFIER, source_column=None, schema_column=column, arena=_TEST_ARENA)


def test_determine_type_map_access_array_returns_element_type():
    left = _identifier(ARRAY(INT64))
    right = _literal(INT64, 0)
    node = BinaryOperator(value="MapAccess", left=left, right=right, arena=_TEST_ARENA)

    assert determine_type(node) == INT64


def test_determine_type_map_access_varchar_returns_varchar():
    left = _identifier(VARCHAR)
    right = _literal(INT64, 1)
    node = BinaryOperator(value="MapAccess", left=left, right=right, arena=_TEST_ARENA)

    assert determine_type(node) == VARCHAR


def test_determine_type_map_access_varchar_subscript_by_string_raises():
    left = _identifier(VARCHAR)
    right = _literal(VARCHAR, b"1")
    node = BinaryOperator(value="MapAccess", left=left, right=right, arena=_TEST_ARENA)

    with pytest.raises(IncorrectTypeError):
        determine_type(node)


def test_determine_type_map_access_invalid_types_raise():
    left = _identifier(INT64)
    right = _literal(INT64, 1)
    node = BinaryOperator(value="MapAccess", left=left, right=right, arena=_TEST_ARENA)

    with pytest.raises(IncorrectTypeError):
        determine_type(node)
