import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.expression import NodeType
from opteryx.models import Node, QueryTelemetry
from opteryx.planner.logical_planner import LogicalPlanNode, LogicalPlanStepType
from opteryx.planner.optimizer.strategies.constant_folding import fold_constants
from opteryx.planner.optimizer.strategies.predicate_ordering import order_predicates
from opteryx.types.logical_type import ARRAY, INT64, VARCHAR
from opteryx.types.schema import ConstantColumn, SchemaColumn


def _literal(value_type, value):
    return Node(
        NodeType.LITERAL,
        type=value_type,
        value=value,
        schema_column=ConstantColumn(name="literal", column_type=value_type, value=value),
    )


def _identifier(name, value_type):
    column = SchemaColumn(name=name, column_type=value_type, identity=name)
    return Node(NodeType.IDENTIFIER, schema_column=column)


def _filter(condition):
    node = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
    node.condition = condition
    return node


# MapAccess is an EXTRACTION_OPERATOR, not a BINARY_OPERATOR - see
# expression.binary_operators.EXTRACTION_OPERATORS and the node the logical planner
# builds in logical_planner_builders.json_access. Building it as a BINARY_OPERATOR
# tests a shape nothing produces, and fails in the bytecode builder.
def test_constant_folding_folds_constant_map_access_expression():
    telemetry = QueryTelemetry("test_map_access_constant_folding")
    expr = Node(
        NodeType.EXTRACTION_OPERATOR,
        value="MapAccess",
        left=_literal(ARRAY(INT64), [10, 20, 30]),
        right=_literal(INT64, 1),
        schema_column=ConstantColumn(name="result", column_type=INT64),
    )

    folded = fold_constants(expr, telemetry)

    assert folded.node_type == NodeType.LITERAL
    assert folded.type == INT64
    assert folded.value == 20


def test_predicate_ordering_treats_nested_function_map_access_as_complex():
    telemetry = QueryTelemetry("test_map_access_predicate_ordering")

    cheap_condition = Node(
        NodeType.COMPARISON_OPERATOR,
        value="Eq",
        left=_identifier("id", INT64),
        right=_literal(INT64, 1),
    )

    split_fn = Node(
        NodeType.FUNCTION,
        value="SPLIT",
        parameters=[_identifier("name", VARCHAR), _literal(VARCHAR, " ")],
    )
    map_access = Node(
        NodeType.EXTRACTION_OPERATOR,
        value="MapAccess",
        left=split_fn,
        right=_literal(INT64, 0),
        schema_column=ConstantColumn(name="first_part", column_type=VARCHAR),
    )
    complex_condition = Node(
        NodeType.COMPARISON_OPERATOR,
        value="Eq",
        left=map_access,
        right=_literal(VARCHAR, "Neil"),
    )

    ordered = order_predicates(
        [_filter(complex_condition), _filter(cheap_condition)],
        telemetry,
    )

    assert ordered[0].condition is cheap_condition
    assert ordered[1].condition is complex_condition
