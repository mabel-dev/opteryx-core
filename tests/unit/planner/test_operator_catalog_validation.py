import os
import sys

import pytest
from opteryx.compiled.structures.expressions import BinaryOperator
from opteryx.compiled.structures.expressions import Literal

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.planner.logical_planner.logical_planner_builders import binary_op
from opteryx.planner.binder.operator_map import determine_type
from opteryx.types.logical_type import INT64
from opteryx.planner.plan_context import PlanContext


def test_binder_rejects_operators_not_in_catalog():
    node = BinaryOperator(
        value="TotallyUnsupported",
        left=Literal(type=INT64, value=1),
        right=Literal(type=INT64, value=2),
    )

    with pytest.raises(UnsupportedSyntaxError, match="Unsupported operator 'TotallyUnsupported'"):
        determine_type(node)


def test_planner_rejects_binary_operators_not_in_catalog():
    plan_context = PlanContext()
    branch = {
        "left": {"Value": {"value": {"Number": ("1", False)}}},
        "op": {"Custom": "TotallyUnsupported"},
        "right": {"Value": {"value": {"Number": ("2", False)}}},
    }

    with pytest.raises(UnsupportedSyntaxError, match="Unsupported operator 'TotallyUnsupported'"):
        binary_op(branch, plan_context=plan_context)
