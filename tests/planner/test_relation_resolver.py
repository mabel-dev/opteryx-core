"""
The Relation Resolver: CTEs, views, and termination.

Resolution turns a NAME in a FROM clause into a PLAN. It runs BEFORE the Plan Rewriter,
so the rewriter sees ONE fully-expanded plan. These tests pin the four properties that
matter, each of which was a live bug:

  1. A subquery inside a view body is eliminated. When expansion happened in the Binder —
     i.e. AFTER the rewriter — an IN-subquery in a view body survived to the expression
     compiler and the query died with "unsupported node type 39" in production.
  2. A view containing a CTE can be queried at all (the view's CTEs used to be discarded,
     leaving a dangling scan).
  3. Cycles fail loud. A self-referencing view, a view cycle, a self-referencing CTE and
     WITH RECURSIVE each used to spin the planner forever — an unkillable worker.
  4. A view body does NOT see the CTEs of the query that called it.
"""

import os
import sys
import tempfile

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities.eidetic import Eidetic
from opteryx.connectors.capabilities.eidetic import ViewDefinition
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner import do_logical_planning_phase
from opteryx.planner.plan_rewriter import do_plan_rewrite
from opteryx.planner.relation_resolver import do_resolve_relations
from opteryx.models import QueryTelemetry
from opteryx.third_party import sqloxide


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _planned(sql):
    """Resolve then rewrite — the plan exactly as the Binder receives it."""
    ast = sqloxide.parse_sql(sql, _dialect="opteryx")[0]
    plan, _, ctes = do_logical_planning_phase(ast)
    plan = do_resolve_relations(plan, ctes, QueryTelemetry())
    return do_plan_rewrite(plan, QueryTelemetry())


def _surviving_subquery_nodes(plan):
    """Every NodeType.SUBQUERY expression node left anywhere in the plan."""
    found = []
    for _nid, node in plan.nodes(True):
        for column in node.columns or []:
            found.extend(get_all_nodes_of_type(column, (NodeType.SUBQUERY,)))
        if node.condition is not None:
            found.extend(get_all_nodes_of_type(node.condition, (NodeType.SUBQUERY,)))
    return found


def _step_types(plan):
    return {node.node_type for _nid, node in plan.nodes(True)}


def _rows(sql):
    rows = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        rows.extend(morsel[i] for i in range(morsel.num_rows))
    return sorted(rows)


@pytest.fixture
def views():
    """A workspace with views defined in it."""
    register_workspace("ws", LocalStoreConnector, store_root=tempfile.mkdtemp())
    session = opteryx.session()
    for statement in (
        "CREATE VIEW ws.in_sq AS SELECT p.id, p.name FROM $planets AS p "
        "  WHERE p.id IN (SELECT q.id FROM $planets AS q WHERE q.id < 5)",
        "CREATE VIEW ws.with_cte AS WITH small AS (SELECT id, name FROM $planets WHERE id < 5) "
        "  SELECT * FROM small",
        "CREATE VIEW ws.on_a_view AS SELECT id, name FROM ws.in_sq",
        "CREATE VIEW ws.self_ref AS SELECT id FROM ws.self_ref",
        "CREATE VIEW ws.cycle_a AS SELECT id FROM ws.cycle_b",
        "CREATE VIEW ws.cycle_b AS SELECT id FROM ws.cycle_a",
        "CREATE VIEW ws.wants_outer_cte AS SELECT id FROM some_outer_cte",
        "CREATE VIEW ws.group_by_all AS SELECT UPPER(name) AS n, COUNT(*) AS total "
        "  FROM $planets GROUP BY ALL",
        "CREATE VIEW ws.using_join AS SELECT id, a.name FROM $planets AS a "
        "  INNER JOIN $planets AS b USING (id)",
    ):
        list(session.execute_to_morsels(statement))


# ---------------------------------------------------------------------------
# 1. subqueries inside CTE and view bodies are eliminated
# ---------------------------------------------------------------------------

def test_in_subquery_in_a_cte_body_is_rewritten():
    plan = _planned(
        "WITH c AS (SELECT p.id FROM $planets AS p "
        "  WHERE p.id IN (SELECT q.id FROM $planets AS q WHERE q.id < 5)) SELECT * FROM c"
    )
    assert _surviving_subquery_nodes(plan) == [], "IN-subquery survived inside a CTE body"
    assert LogicalPlanStepType.Join in _step_types(plan)


def test_view_with_in_subquery_matches_inline_sql(views):
    """The production bug: this raised NotImplementedError (node type 39)."""
    inline = "SELECT p.id, p.name FROM $planets AS p WHERE p.id IN (SELECT q.id FROM $planets AS q WHERE q.id < 5)"
    assert _rows("SELECT * FROM ws.in_sq") == _rows(inline)


def test_view_on_a_view_resolves(views):
    assert len(_rows("SELECT * FROM ws.on_a_view")) == 4


def test_same_view_twice_in_one_query(views):
    """Each expansion gets fresh node ids — otherwise the second overwrites the first."""
    assert len(_rows("SELECT a.id FROM ws.in_sq a JOIN ws.in_sq b ON a.id = b.id")) == 4


# ---------------------------------------------------------------------------
# 1b. GROUP BY ALL keeps its function binding through view expansion
#
# GROUP BY ALL reuses the SAME expression-node object from the SELECT list as the
# aggregate's group-by key (see logical_planner.py) — the binder relies on that
# sharing to resolve a FUNCTION node's catalog entry once and have it apply
# wherever the node appears. Copying a plan (LogicalPlan.copy(), used by every
# view/CTE expansion) used to copy each graph node's property tree independently
# with no memo, silently splitting that one shared node into two copies. Only one
# got bound, so `SELECT * FROM a_view_using_group_by_all` died at physical planning
# with "FUNCTION 'UPPER' has no function_ref — not bound" while the view's own SQL,
# run directly, worked fine.
# ---------------------------------------------------------------------------


def test_view_with_group_by_all_computed_key_resolves(views):
    # SELECT * column order isn't pinned to the view body's — select explicitly so
    # this only checks the values, not wildcard-expansion ordering.
    assert _rows("SELECT n, total FROM ws.group_by_all") == _rows(
        "SELECT UPPER(name) AS n, COUNT(*) AS total FROM $planets GROUP BY ALL"
    )


# ---------------------------------------------------------------------------
# 2. a view containing a CTE
# ---------------------------------------------------------------------------

def test_view_containing_a_cte_is_queryable(views):
    assert len(_rows("SELECT * FROM ws.with_cte")) == 4


# ---------------------------------------------------------------------------
# 3. termination — these all used to hang the planner forever
# ---------------------------------------------------------------------------

def test_self_referencing_view_fails_loud(views):
    with pytest.raises(UnsupportedSyntaxError, match="defined in terms of itself"):
        _rows("SELECT * FROM ws.self_ref")


def test_view_cycle_fails_loud_and_names_the_path(views):
    with pytest.raises(UnsupportedSyntaxError, match=r"ws.cycle_a -> ws.cycle_b -> ws.cycle_a"):
        _rows("SELECT * FROM ws.cycle_a")


def test_self_referencing_cte_fails_loud():
    with pytest.raises(UnsupportedSyntaxError, match="defined in terms of itself"):
        _rows("WITH t AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM t) SELECT * FROM t")


def test_with_recursive_fails_loud():
    """Not supported — but it must SAY so, not hang. Needs a native fixpoint operator."""
    with pytest.raises(UnsupportedSyntaxError, match=r"\*\*WITH RECURSIVE\*\* is not supported"):
        _rows(
            "WITH RECURSIVE t(n) AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM t WHERE n < 5) "
            "SELECT * FROM t"
        )


# ---------------------------------------------------------------------------
# 4. scoping — a view is a closed unit
# ---------------------------------------------------------------------------

def test_view_body_cannot_see_the_callers_ctes(views):
    """The caller defines `some_outer_cte`; the view body references that name and must
    NOT resolve to it — a view sees only its own CTEs."""
    with pytest.raises(DatasetNotFoundError, match="some_outer_cte"):
        _rows(
            "WITH some_outer_cte AS (SELECT id FROM $planets) SELECT * FROM ws.wants_outer_cte"
        )


def test_cte_shadows_a_catalog_relation():
    """A CTE name wins over a catalog relation of the same name."""
    rows = _rows("WITH $planets AS (SELECT 1 AS id) SELECT * FROM $planets")
    assert rows == [(1,)], rows


# ---------------------------------------------------------------------------
# 5. column alias lists — `WITH t(a, b) AS (...)` used to be parsed and discarded
# ---------------------------------------------------------------------------

def _column_names(sql):
    morsels = list(opteryx.session().execute_to_morsels(sql))
    return [name.decode() if isinstance(name, bytes) else name for name in morsels[0].column_names]


def test_cte_column_aliases_rename_the_output():
    assert _column_names("WITH x(a, b) AS (SELECT id, name FROM $planets) SELECT a, b FROM x") == [
        "a",
        "b",
    ]


def test_cte_column_aliases_replace_the_body_names():
    """The rename is a rename: the body's own name is gone, not merely shadowed."""
    from opteryx.exceptions import ColumnNotFoundError

    with pytest.raises(ColumnNotFoundError):
        _rows("WITH x(a) AS (SELECT id FROM $planets) SELECT id FROM x")


def test_cte_column_alias_count_mismatch_fails_loud():
    with pytest.raises(UnsupportedSyntaxError, match="column alias"):
        _rows("WITH x(a, b) AS (SELECT id FROM $planets) SELECT a FROM x")


def test_cte_column_aliases_over_wildcard_fail_loud():
    """We cannot line names up against a projection that binding hasn't resolved yet —
    say so, rather than silently ignoring the names."""
    with pytest.raises(UnsupportedSyntaxError, match="wildcard projection"):
        _rows("WITH x(a) AS (SELECT * FROM $planets) SELECT a FROM x")


# ---------------------------------------------------------------------------
# 6. statements that WRAP a query carry the WITH clause on the inner query — the CTEs
#    used to be dropped, and the query failed with "dataset '<cte>' could not be found"
# ---------------------------------------------------------------------------

def test_explain_over_a_cte_resolves():
    assert len(_rows("EXPLAIN WITH c AS (SELECT id FROM $planets) SELECT * FROM c")) > 0


def test_explain_over_chained_ctes_resolves():
    assert (
        len(
            _rows(
                "EXPLAIN WITH a AS (SELECT id FROM $planets), "
                "b AS (SELECT id FROM a WHERE id < 4) SELECT * FROM b"
            )
        )
        > 0
    )


def test_explain_still_rejects_recursive():
    """The recursion guard must fire through the wrapper too, not just on a bare query."""
    with pytest.raises(UnsupportedSyntaxError, match=r"\*\*WITH RECURSIVE\*\* is not supported"):
        _rows(
            "EXPLAIN WITH RECURSIVE t(n) AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM t) "
            "SELECT * FROM t"
        )


# ---------------------------------------------------------------------------
# 7. the OTHER branch of resolve_relation
#
# A connector exposing get_relation resolves view-or-dataset in one catalog round trip;
# one without it falls back to a view-only probe. Production (OpteryxConnector) takes the
# get_relation branch, LocalStoreConnector the fallback — so the tests above exercise the
# fallback only. Cover the production branch with a connector shaped like it.
# ---------------------------------------------------------------------------

_CATALOG_VIEWS = {
    "cat.in_sq": "SELECT p.id, p.name FROM $planets AS p "
    "WHERE p.id IN (SELECT q.id FROM $planets AS q WHERE q.id < 5)",
    "cat.with_cte": "WITH small AS (SELECT id, name FROM $planets WHERE id < 5) SELECT * FROM small",
    "cat.self_ref": "SELECT id FROM cat.self_ref",
}


class _CatalogConnector(Eidetic, BaseConnector):
    """Shaped like OpteryxConnector: one round trip resolves view-or-dataset."""

    def __init__(self, **kwargs):
        pass

    def get_relation(self, name):
        if name in _CATALOG_VIEWS:
            return "view", ViewDefinition(
                name=name, statement=_CATALOG_VIEWS[name], owner="test"
            )
        return None, None


@pytest.fixture
def catalog():
    register_workspace("cat", _CatalogConnector)


def test_get_relation_branch_expands_a_view_with_a_subquery(catalog):
    assert len(_rows("SELECT * FROM cat.in_sq")) == 4


def test_get_relation_branch_expands_a_view_with_a_cte(catalog):
    assert len(_rows("SELECT * FROM cat.with_cte")) == 4


def test_get_relation_branch_detects_a_cycle(catalog):
    with pytest.raises(UnsupportedSyntaxError, match="defined in terms of itself"):
        _rows("SELECT * FROM cat.self_ref")


# ---------------------------------------------------------------------------
# 5. splicing does not duplicate a join's leg relation names
# ---------------------------------------------------------------------------


def _join_legs(sql):
    """(left_relation_names, right_relation_names) for every Join in the resolved plan."""
    ast = sqloxide.parse_sql(sql, _dialect="opteryx")[0]
    plan, _, ctes = do_logical_planning_phase(ast)
    plan = do_resolve_relations(plan, ctes, QueryTelemetry())
    return [
        (list(node.left_relation_names or []), list(node.right_relation_names or []))
        for _, node in plan.nodes(True)
        if node.node_type == LogicalPlanStepType.Join
    ]


def test_splicing_does_not_duplicate_leg_relation_names():
    """`_splice` re-runs join_leg_preprocess over the WHOLE plan on every expansion,
    so a Join whose legs the logical planner already computed gets walked again. It
    used to append unconditionally, giving `['b', 'b']`."""
    legs = _join_legs(
        "WITH c AS (SELECT id FROM $planets AS a INNER JOIN $planets AS b USING (id)) "
        "SELECT * FROM c"
    )
    assert legs, "no Join survived resolution"
    for left, right in legs:
        assert len(left) == len(set(left)), f"duplicate left leg names: {left}"
        assert len(right) == len(set(right)), f"duplicate right leg names: {right}"


def test_join_using_inside_a_cte_body_binds():
    """The duplicate leg name reached the binder's USING handler, which pops the
    named column out of each listed relation in turn. The second pop of an
    already-popped column returned None and setting `.origin` on it died with
    `AttributeError: 'NoneType' object has no attribute 'origin'`."""
    assert _rows(
        "WITH c AS (SELECT id FROM $planets AS a INNER JOIN $planets AS b USING (id)) "
        "SELECT * FROM c"
    ) == [(i,) for i in range(1, 10)]


def test_join_using_inside_a_view_body_binds(views):
    """Same defect, reached the way it was actually reported — through a view."""
    assert len(_rows("SELECT * FROM ws.using_join")) == 9


def test_join_using_still_binds_without_a_splice():
    """The control: the same join with nothing to expand never had the duplicate."""
    assert _rows(
        "SELECT id FROM $planets AS a INNER JOIN $planets AS b USING (id)"
    ) == [(i,) for i in range(1, 10)]


def test_join_using_on_a_column_neither_leg_has_fails_clean():
    """No relation on the leg holds the column — a real error with a real message,
    not an AttributeError on None."""
    with pytest.raises(Exception) as excinfo:
        _rows("SELECT * FROM $planets AS a INNER JOIN $planets AS b USING (no_such_column)")
    assert "no_such_column" in str(excinfo.value)


def test_insert_from_a_cte_resolves(tmp_path):
    register_workspace("wsi", LocalStoreConnector, store_root=str(tmp_path))
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE wsi.target (id BIGINT)"))
    list(
        session.execute_to_morsels(
            "INSERT INTO wsi.target WITH c AS (SELECT id FROM $planets WHERE id < 4) "
            "SELECT * FROM c"
        )
    )
    assert _rows("SELECT * FROM wsi.target") == [(1,), (2,), (3,)]
