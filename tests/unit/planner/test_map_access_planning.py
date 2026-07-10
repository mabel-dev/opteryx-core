import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.exceptions import FunctionNotFoundError
from opteryx.expression import NodeType
from opteryx.planner.ast_rewriter import do_ast_rewriter
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner import do_logical_planning_phase
from opteryx.third_party import sqloxide


def _first_projection_expression(sql: str):
    parsed = sqloxide.parse_sql(sql, _dialect="opteryx")
    ast = do_ast_rewriter(parsed, parameters=[])[0]
    plan, _, _ = do_logical_planning_phase(ast)

    for _, node in plan.nodes(True):
        if node.node_type == LogicalPlanStepType.Project:
            assert node.columns, "Project node has no expressions."
            return node.columns[0]

    raise AssertionError("No project node found in logical plan.")


def _plan(sql: str):
    parsed = sqloxide.parse_sql(sql, _dialect="opteryx")
    ast = do_ast_rewriter(parsed, parameters=[])[0]
    return do_logical_planning_phase(ast)


def test_bracket_access_on_function_expression_uses_map_access():
    expr = _first_projection_expression("SELECT SPLIT(name, ' ')[0] AS v FROM $planets")

    assert expr.node_type == NodeType.EXTRACTION_OPERATOR
    assert expr.value == "MapAccess"


def test_bracket_access_on_identifier_uses_map_access():
    expr = _first_projection_expression("SELECT missions[0] AS v FROM $astronauts")

    assert expr.node_type == NodeType.EXTRACTION_OPERATOR
    assert expr.value == "MapAccess"


def test_string_key_arrow_access_uses_arrow_operator():
    expr = _first_projection_expression("SELECT birth_place->'town' AS v FROM $planets")

    assert expr.node_type == NodeType.EXTRACTION_OPERATOR
    assert expr.value == "Arrow"


def test_get_integer_key_is_rejected_as_unknown_function():
    with pytest.raises(FunctionNotFoundError, match="Unknown function 'GET'"):
        _plan("SELECT GET(SPLIT(name, ' '), 0) AS v FROM $planets")


def test_get_string_key_is_rejected_as_unknown_function():
    with pytest.raises(FunctionNotFoundError, match="Unknown function 'GET'"):
        _plan("SELECT GET(birth_place, 'town') AS v FROM $planets")
