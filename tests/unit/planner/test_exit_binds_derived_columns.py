"""An EXIT node must be able to bind a column that lives in the derived schema.

`visit_exit` used to clear `$derived` *before* binding its columns. That is the very
schema such a column needs: an aggregate registers itself in `$derived`, and binding an
unbound expression appends to it. Clearing it first meant an EXIT carrying a raw aggregate
could neither resolve against it nor add to it, and the query died with `KeyError:
'$derived'`.

SQL never reached that path — the SQL planner's EXIT columns are already-bound identifiers,
which short-circuit. But a plan built directly against the logical planner can put a raw
aggregate node on the EXIT, and the OData service's COUNT/$apply plans do exactly that. Over
a plain dataset it happened to work; over a *view* it did not, and every `$count=true` and
`$apply` against a view 500'd.

The plans here are hand-built to mirror `ODataLogicalPlanBuilder` rather than going through
SQL, because going through SQL is precisely what does not reproduce it.
"""

import pytest

from opteryx.connectors.capabilities.eidetic import ViewDefinition
from opteryx.expression import NodeType
from opteryx.managers import views as views_module
from opteryx.models import Node
from opteryx.planner import execute_logical_plan
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.utils import random_string

# A view whose body carries an IN-subquery — the shape the plan rewriter lowers to a
# LEFT SEMI join, and the shape a real semi-join view (e.g. exploited_vulnerabilities) has.
VIEW_SQL = (
    "SELECT id, name FROM $planets WHERE id IN (SELECT p2.id FROM $planets AS p2 WHERE p2.gravity > 9)"
)
VIEW = ViewDefinition(name="heavy_planets", statement=VIEW_SQL, last_row_count=3)

HEAVY_PLANETS = 3  # Earth, Jupiter, Neptune
ALL_PLANETS = 9


@pytest.fixture
def view_in_catalog(monkeypatch):
    """Serve `heavy_planets` as a view without needing a real catalog."""
    real_resolve = views_module.resolve_relation

    def resolve(relation, telemetry):
        if relation == "heavy_planets":
            return "view", views_module._view_plan_from_definition(VIEW)
        return real_resolve(relation, telemetry)

    monkeypatch.setattr(views_module, "resolve_relation", resolve)


def odata_style_count_plan(relation):
    """Scan -> Aggregate(COUNT(*)) -> Exit, with the aggregate node itself on the EXIT.

    This mirrors ODataLogicalPlanBuilder's `count_only` plan: there is no Project between
    the aggregate and the exit, so the EXIT column is the raw aggregate node.
    """
    plan = LogicalPlan()

    scan = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    scan.relation = relation
    scan.alias = relation.split(".")[-1]
    scan.hints = []
    scan_id = random_string()
    plan.add_node(scan_id, scan)

    count = Node(
        node_type=NodeType.AGGREGATOR, value="COUNT", parameters=[Node(node_type=NodeType.WILDCARD)]
    )
    count.alias = "count"
    aggregate = LogicalPlanNode(node_type=LogicalPlanStepType.Aggregate)
    aggregate.groups = []
    aggregate.aggregates = [count]
    aggregate_id = random_string()
    plan.add_node(aggregate_id, aggregate)
    plan.add_edge(scan_id, aggregate_id)

    exit_node = LogicalPlanNode(node_type=LogicalPlanStepType.Exit)
    exit_node.columns = [count]
    exit_id = random_string()
    plan.add_node(exit_id, exit_node)
    plan.add_edge(aggregate_id, exit_id)

    return plan


def _count(plan):
    morsels, _ = execute_logical_plan(plan)
    values = []
    for morsel in morsels:
        morsel.materialize()
        values += morsel.column("count").to_pylist()
    return values


def test_count_over_a_dataset(view_in_catalog):
    assert _count(odata_style_count_plan("$planets")) == [ALL_PLANETS]


def test_count_over_a_view_containing_a_subquery(view_in_catalog):
    # The regression: this raised KeyError('$derived') during binding, surfacing as a 500
    # on every $count=true / $apply against a view.
    assert _count(odata_style_count_plan("heavy_planets")) == [HEAVY_PLANETS]


def test_view_rows_still_read(view_in_catalog):
    """The non-aggregate path over the same view was always fine — keep it that way."""
    plan = LogicalPlan()
    scan = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    scan.relation = "heavy_planets"
    scan.alias = "heavy_planets"
    scan.hints = []
    scan_id = random_string()
    plan.add_node(scan_id, scan)

    exit_node = LogicalPlanNode(node_type=LogicalPlanStepType.Exit)
    exit_node.columns = [Node(node_type=NodeType.WILDCARD)]
    exit_id = random_string()
    plan.add_node(exit_id, exit_node)
    plan.add_edge(scan_id, exit_id)

    morsels, _ = execute_logical_plan(plan)
    assert sum(morsel.num_rows for morsel in morsels) == HEAVY_PLANETS


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
