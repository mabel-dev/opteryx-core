"""A CTE whose body is a VALUES relation must plan, bind and run.

`WITH b AS (SELECT * FROM (VALUES ('x')) AS v(c)) SELECT * FROM b` died in the
Binder with a bare `AttributeError: 'str' object has no attribute 'node_type'`.

The cause is that `columns` does not mean the same thing on every logical plan
node. On a Project/Exit/Subquery/Union it is a PROJECTION — a list of expression
nodes. On a FunctionDataset (VALUES, UNNEST, GENERATE_SERIES) it is a tuple of
output NAMES, plain strings. `SELECT *` over VALUES leaves no Project at all, so
the CTE body's head IS the FunctionDataset, and the Relation Resolver's
`_output_columns` walk — which stopped at the first node with a truthy `columns`
— stamped `('c',)` onto the Subquery boundary it splices in. The Binder then read
`column.node_type` off a `str`.

That combination is why every part worked alone: VALUES standalone has no
boundary node spliced over it, and a CTE over anything that projects (`SELECT 'x'
AS c`) has a real Project at its head.

The three FAILING and three WORKING statements below are the bisected boundary of
the defect and are pinned verbatim — the working ones as much as the broken ones,
because the fix narrows which nodes the walk will read a projection from and
could just as easily have lost the body's column NAMES for the cases that worked.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import InvalidInternalStateError
from opteryx.expression import NodeType
from opteryx.models import LogicalColumn
from opteryx.models import Node


def results(sql):
    session = opteryx.session()
    out: dict = {}
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        for key, values in morsel.to_arrow().to_pydict().items():
            out.setdefault(key, []).extend(values)
    return out


# --- the three statements that crashed -----------------------------------------------


def test_cte_over_values_alone():
    assert results("WITH b AS (SELECT * FROM (VALUES ('x')) AS v(c)) SELECT * FROM b") == {
        "c": ["x"]
    }


def test_cte_over_values_cross_joined():
    rows = results(
        "WITH b AS (SELECT * FROM (VALUES ('x')) AS v(c)) "
        "SELECT * FROM b CROSS JOIN (SELECT 1 AS y) t"
    )
    assert rows == {"c": ["x"], "y": [1]}, rows


def test_cte_over_values_inner_joined_on_its_column():
    # The join reads `b.c` by name — if the boundary node lost the body's names
    # this binds against nothing rather than crashing, so assert the VALUE too.
    rows = results(
        "WITH b AS (SELECT * FROM (VALUES ('x')) AS v(c)) "
        "SELECT * FROM b INNER JOIN (SELECT 'x' AS y) t ON b.c = t.y"
    )
    assert rows == {"c": ["x"], "y": ["x"]}, rows


# --- the three statements that already worked, pinned against regression -------------


def test_values_standalone():
    assert results("SELECT * FROM (VALUES ('x')) AS v(c)") == {"c": ["x"]}


def test_values_inline_in_a_join():
    rows = results("SELECT * FROM (VALUES ('x')) AS v(c) CROSS JOIN (SELECT 1 AS y) t")
    assert rows == {"c": ["x"], "y": [1]}, rows


def test_ctes_without_values():
    rows = results(
        "WITH b AS (SELECT 'x' AS c), s AS (SELECT 1 AS y) SELECT * FROM s CROSS JOIN b"
    )
    assert rows == {"y": [1], "c": ["x"]}, rows


# --- the shape the defect was hit on, and the multiply-referenced body ---------------


def test_multi_row_multi_column_values_in_a_cte():
    rows = results(
        "WITH b AS (SELECT * FROM (VALUES ('x', 1), ('y', 2), ('z', 3)) AS v(c, n)) "
        "SELECT * FROM b ORDER BY n"
    )
    assert rows == {"c": ["x", "y", "z"], "n": [1, 2, 3]}, rows


def test_cte_over_values_referenced_twice():
    # Two references take the OTHER resolver path: the body is materialized once and
    # each reference mints its own identities, over the same Subquery boundary node.
    rows = results(
        "WITH b AS (SELECT * FROM (VALUES ('x'), ('y')) AS v(c)) "
        "SELECT l.c AS lc, r.c AS rc FROM b l CROSS JOIN b r ORDER BY lc, rc"
    )
    assert rows == {"lc": ["x", "x", "y", "y"], "rc": ["x", "y", "x", "y"]}, rows


def test_cte_over_values_is_the_typosquat_idiom():
    # The query this was found on, reduced: a small inline lookup table in a CTE,
    # cross-joined against a real relation with a string function over both sides.
    rows = results(
        "WITH brands AS (SELECT * FROM (VALUES ('Mars'), ('Venus')) AS v(brand)) "
        "SELECT brand, name FROM brands CROSS JOIN $planets "
        "WHERE LEVENSHTEIN(brand, name) = 0 ORDER BY brand"
    )
    assert rows == {"brand": ["Mars", "Venus"], "name": ["Mars", "Venus"]}, rows


# --- the body head that is NOT a projection, but is not VALUES either ----------------


def test_cte_over_values_with_order_and_limit_at_the_body_head():
    # Order/Limit carry no columns of their own; the walk must still descend past
    # them. Here it descends onto the FunctionDataset and must answer WILDCARD.
    rows = results(
        "WITH b AS (SELECT * FROM (VALUES ('x'), ('y')) AS v(c) ORDER BY c LIMIT 1) "
        "SELECT * FROM b"
    )
    assert rows == {"c": ["x"]}, rows


def test_cte_over_values_that_does_project():
    rows = results("WITH b AS (SELECT c AS d FROM (VALUES ('x')) AS v(c)) SELECT * FROM b")
    assert rows == {"d": ["x"]}, rows


# --- the guard: no bare AttributeError out of the resolver ---------------------------


def test_a_non_expression_projection_is_a_typed_error_naming_the_relation():
    """The boundary contract is checked where the relation is still named.

    Without this the same class of defect reaches `binder/project.py` and surfaces
    as `AttributeError: 'str' object has no attribute 'node_type'` — no error type
    the caller can catch, and no mention of which relation produced it.
    """
    from opteryx.planner.relation_resolver import _boundary_columns

    class _FakePlan:
        def __init__(self, node):
            self._node = node

        def __getitem__(self, _nid):
            return self._node

        def ingoing_edges(self, _nid):
            return []

    from opteryx.planner.logical_planner import LogicalPlanNode
    from opteryx.planner.logical_planner import LogicalPlanStepType

    node = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
    node.columns = ("c",)

    with pytest.raises(InvalidInternalStateError) as err:
        _boundary_columns(_FakePlan(node), "nid", "brands")
    assert "brands" in str(err.value)


def test_a_well_formed_projection_passes_the_guard():
    from opteryx.planner.logical_planner import LogicalPlanNode
    from opteryx.planner.logical_planner import LogicalPlanStepType
    from opteryx.planner.relation_resolver import _boundary_columns

    class _FakePlan:
        def __init__(self, node):
            self._node = node

        def __getitem__(self, _nid):
            return self._node

        def ingoing_edges(self, _nid):
            return []

    node = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
    node.columns = [LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="c")]
    assert _boundary_columns(_FakePlan(node), "nid", "brands") == node.columns

    # a leaf with no projection at all is the wildcard, not an error
    values = LogicalPlanNode(node_type=LogicalPlanStepType.FunctionDataset, function="VALUES")
    values.columns = ("c",)
    wildcard = _boundary_columns(_FakePlan(values), "nid", "brands")
    assert len(wildcard) == 1 and isinstance(wildcard[0], Node)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
