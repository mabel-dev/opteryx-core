"""A filter must reduce NDV — but only the LIVE count, never the key domain.

A filter drops ROWS. A distinct value disappears only when every row carrying
it is dropped, so NDV falls far more slowly than the row count
(`surviving_distinct_count`), and it can never exceed the row count that
remains (`cap_ndvs`, src/cpp/planner/statistics_refresh.hpp). Before this, neither happened: `l_orderkey` came out of
a scan reporting 60,000 distinct values against 20,058 rows.

The second half of this file is the trap. Scaling `distinct_count` in place
silently fed a POST-filter number to the join-key divisor, which is documented
in `_build_equiv_tdoms` as required to be PRE-filter -- "a filter removes ROWS,
not the values the key column could hold". The measured effect on TPC-DS Q54's
shape was that a filtered dimension stopped predicting any reduction and DPccp
put the UNFILTERED 100,000-row customer table first: the leading join went from
8,206 rows to 2,150,243. `base_distinct_count` is what keeps both numbers
available, and these tests pin each to its own consumer.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

# Importing opteryx.planner.optimizer (the package) first resolves the
# pre-existing import cycle a compiled planner module hits when imported first.
import opteryx.planner.optimizer  # noqa: F401
from opteryx.compiled.planner.plan_graph import EdgeRole
from opteryx.compiled.planner.statistics import StatisticsInput
from opteryx.compiled.structures.expressions import Comparison
from opteryx.compiled.structures.expressions import Literal
from opteryx.compiled.structures.expressions import LogicalColumn
from opteryx.compiled.structures.plan_steps import FilterStep
from opteryx.compiled.structures.plan_steps import JoinStep
from opteryx.compiled.structures.plan_steps import ScanStep
from opteryx.expression import NodeType
from opteryx.planner.cost_estimation import surviving_distinct_count
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.plan_context import PlanContext
from opteryx.types.logical_type import INT64


def _relation(plan_context, rows, ndv, key, base_ndv=None, base_rows=None, extra=None):
    column_stats = {key: {"distinct_count": ndv, "base_distinct_count": base_ndv}}
    column_stats.update(extra or {})
    return StatisticsInput(
        plan_context.columns,
        row_count_estimate=rows,
        column_stats=column_stats,
        base_row_count=base_rows,
    )


def _join_divisor(plan_context, left, right, left_key, right_key):
    """The divisor the inner-join estimate applied: |L| x |R| / estimate.

    The key classes' KeyStats are internal to the native join propagator; the
    number they produce is the join's row count, |L| x |R| / max(ndv_l, ndv_r)
    for one null-free key class, so the divisor is recovered from it (exactly:
    these fixtures divide evenly)."""
    join = JoinStep()
    join.type = "inner"
    join.left_columns = [left_key]
    join.right_columns = [right_key]
    plan = LogicalPlan(plan_context)
    left_nid = plan.add_node(ScanStep())
    right_nid = plan.add_node(ScanStep())
    join_nid = plan.add_node(join)
    plan.add_edge(left_nid, join_nid, EdgeRole.LEFT)
    plan.add_edge(right_nid, join_nid, EdgeRole.RIGHT)
    store = plan_context.statistics
    store.seed(left_nid, left)
    store.seed(right_nid, right)
    store.compute(plan, join_nid)
    return store.row_count(left_nid) * store.row_count(right_nid) / store.row_count(join_nid)


def test_a_value_survives_while_any_of_its_rows_does():
    """NDV must not be scaled by the row ratio. 100 distinct values spread over
    600,000 rows keep essentially all of themselves through a 50% filter."""
    assert surviving_distinct_count(100, 600_000, 0.5) == 100
    # A near-unique column has nothing to hide behind and falls with the rows.
    assert surviving_distinct_count(1000, 1000, 0.1) == 99


def test_scaling_never_grows_or_invents():
    assert surviving_distinct_count(None, 5000, 0.5) is None      # unknown stays unknown
    assert surviving_distinct_count(10, 1_000_000, 0.999) == 10   # cannot grow
    assert surviving_distinct_count(1000, 5000, 1.0) == 1000      # nothing removed
    assert surviving_distinct_count(1000, 5000, 0.0) == 1         # floored, never 0


def test_scaling_reduces_the_live_count_and_keeps_the_domain():
    """A Filter of selectivity 0.63 on ANOTHER column (`o < 63` over o in
    [0, 100]) scales the key's NDV -- the Filter propagator's NDV scaling, with
    no range narrowing or equality capping on the key itself."""
    plan_context = PlanContext()
    key = plan_context.columns.relation_column("t", "k", column_type=INT64)
    other = plan_context.columns.relation_column("t", "o", column_type=INT64)
    base = _relation(
        plan_context, 600_000, ndv=150_000, key=key.identity, extra={other.identity: {"value_range": (0, 100)}}
    )

    arena = plan_context.expressions
    identifier = LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="o", arena=arena)
    identifier.schema_column = other
    condition = Comparison(value="Lt", left=identifier, right=Literal(value=63, type=INT64, arena=arena), arena=arena)
    step = FilterStep()
    step.condition = condition

    plan = LogicalPlan(plan_context)
    child_nid = plan.add_node(ScanStep())
    filter_nid = plan.add_node(step)
    plan.add_edge(child_nid, filter_nid)
    store = plan_context.statistics
    store.seed(child_nid, base)
    assert store.estimate_selectivity(child_nid, condition) == 0.63
    store.compute(plan, filter_nid)

    assert store.distinct_count(filter_nid, key.identity) < 150_000, "the live count must respond to the filter"
    assert store.domain_distinct_count(filter_nid, key.identity) == 150_000, "the domain is not a filterable thing"


def test_a_filter_does_not_move_the_join_divisor():
    """THE REGRESSION. The divisor is a property of the key DOMAIN, so filtering
    one side must not move it -- otherwise the filter's selectivity is charged a
    second time inside the divisor and a filtered dimension predicts no
    reduction at all."""
    plan_context = PlanContext()
    key = plan_context.columns.relation_column("l", "k").identity
    other_key = plan_context.columns.relation_column("r", "k").identity
    right = _relation(plan_context, 100_000, ndv=100_000, key=other_key)

    unfiltered = _relation(plan_context, 200_000, ndv=200_000, key=key)
    # Same relation after a filter: rows and the live NDV both fell, the domain
    # did not -- exactly what the Filter's NDV scaling produces.
    filtered = _relation(plan_context, 20_000, ndv=20_000, key=key, base_ndv=200_000, base_rows=200_000)

    before = _join_divisor(plan_context, unfiltered, right, key, other_key)
    after = _join_divisor(plan_context, filtered, right, key, other_key)

    assert before == after, (
        f"filtering moved the divisor {before} -> {after}; the post-filter NDV reached the divisor"
    )
    assert after == 200_000


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
