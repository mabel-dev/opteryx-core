import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from orso.schema import ConstantColumn
from orso.schema import FlatColumn
from orso.types import OrsoTypes

from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.models import QueryTelemetry
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.optimizer.strategies.constant_folding import fold_constants
from opteryx.planner.optimizer.strategies.predicate_ordering import order_predicates


def _literal(value_type, value, *, element_type=None):
    return Node(
        NodeType.LITERAL,
        type=value_type,
        value=value,
        element_type=element_type,
        schema_column=ConstantColumn(name="literal", type=value_type, value=value, element_type=element_type),
    )


def _identifier(name, value_type):
    column = FlatColumn(name=name, type=value_type)
    column.identity = name
    return Node(NodeType.IDENTIFIER, schema_column=column)


def _filter(condition):
    node = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
    node.condition = condition
    return node


def test_constant_folding_folds_constant_map_access_expression():
    telemetry = QueryTelemetry("test_map_access_constant_folding")
    expr = Node(
        NodeType.BINARY_OPERATOR,
        value="MapAccess",
        left=_literal(OrsoTypes.ARRAY, [10, 20, 30], element_type=OrsoTypes.INTEGER),
        right=_literal(OrsoTypes.INTEGER, 1),
        schema_column=ConstantColumn(name="result", type=OrsoTypes.INTEGER),
    )

    folded = fold_constants(expr, telemetry)

    assert folded.node_type == NodeType.LITERAL
    assert folded.type == OrsoTypes.INTEGER
    assert folded.value == 20


def test_predicate_ordering_treats_nested_function_map_access_as_complex():
    telemetry = QueryTelemetry("test_map_access_predicate_ordering")

    cheap_condition = Node(
        NodeType.COMPARISON_OPERATOR,
        value="Eq",
        left=_identifier("id", OrsoTypes.INTEGER),
        right=_literal(OrsoTypes.INTEGER, 1),
    )

    split_fn = Node(
        NodeType.FUNCTION,
        value="SPLIT",
        parameters=[_identifier("name", OrsoTypes.VARCHAR), _literal(OrsoTypes.VARCHAR, " ")],
    )
    map_access = Node(
        NodeType.BINARY_OPERATOR,
        value="MapAccess",
        left=split_fn,
        right=_literal(OrsoTypes.INTEGER, 0),
        schema_column=ConstantColumn(name="first_part", type=OrsoTypes.VARCHAR),
    )
    complex_condition = Node(
        NodeType.COMPARISON_OPERATOR,
        value="Eq",
        left=map_access,
        right=_literal(OrsoTypes.VARCHAR, "Neil"),
    )

    ordered = order_predicates(
        [_filter(complex_condition), _filter(cheap_condition)],
        telemetry,
    )

    assert ordered[0].condition is cheap_condition
    assert ordered[1].condition is complex_condition
