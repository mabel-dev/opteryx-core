import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from orso.types import OrsoTypes

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner.logical_planner.logical_planner_builders import binary_op
from opteryx.planner.binder.operator_map import determine_type


def test_binder_rejects_operators_not_in_catalog():
    node = Node(
        NodeType.BINARY_OPERATOR,
        value="TotallyUnsupported",
        left=Node(NodeType.LITERAL, type=OrsoTypes.INTEGER, value=1),
        right=Node(NodeType.LITERAL, type=OrsoTypes.INTEGER, value=2),
    )

    with pytest.raises(UnsupportedSyntaxError, match="Unsupported operator 'TotallyUnsupported'"):
        determine_type(node)


def test_planner_rejects_binary_operators_not_in_catalog():
    branch = {
        "left": {"Value": {"value": {"Number": ("1", False)}}},
        "op": {"Custom": "TotallyUnsupported"},
        "right": {"Value": {"value": {"Number": ("2", False)}}},
    }

    with pytest.raises(UnsupportedSyntaxError, match="Unsupported operator 'TotallyUnsupported'"):
        binary_op(branch)
