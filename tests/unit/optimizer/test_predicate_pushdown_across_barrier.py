# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A sibling leg's predicate must survive a barrier at the head of another leg.

The optimizer walks a plan depth-first with ONE shared `collected_predicates`
list, so a predicate destined for one leg of a join rides into the other leg's
subtree. The barrier arm (Limit / Union / Window / FramedWindow / Aggregate) used
to dispose of every predicate it held: the ones the barrier emits are parked
above it (correct -- a filter must not cross a Union), and the REST were restored
to their original position, which is above the whole join stack. A sibling leg's
predicate was therefore evicted before the traversal ever reached its own scan.

The shape below is the `my_customers` CTE of TPC-DS Q54 reduced to two variants
that differ only in whether one leg is a UNION ALL. Without the union, all four
single-relation predicates reach their scans. With it, they used to strand: the
date_dim scan stayed at its full row count and the first join went from 5,502
rows to 22.6M, filtered back down to a handful only after every join had run.

The assertion is on the SCAN estimates, not on the counters: the counters say a
predicate moved, the scan estimate says it moved to the right place.
"""

import os
import sys
import uuid

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

DATASET = "testdata.tpcds_1_skene"

_QUERY = """
SELECT DISTINCT c_customer_sk, c_current_addr_sk
FROM ({sales}) cs_or_ws_sales,
     {ds}.item, {ds}.date_dim, {ds}.customer
WHERE sold_date_sk = d_date_sk
  AND item_sk = i_item_sk
  AND i_category = 'Women'
  AND i_class = 'maternity'
  AND c_customer_sk = cs_or_ws_sales.customer_sk
  AND d_moy = 12
  AND d_year = 1998
"""

_CATALOG_SALES = """
SELECT cs_sold_date_sk sold_date_sk, cs_bill_customer_sk customer_sk, cs_item_sk item_sk
FROM {ds}.catalog_sales
"""

_WEB_SALES = """
SELECT ws_sold_date_sk sold_date_sk, ws_bill_customer_sk customer_sk, ws_item_sk item_sk
FROM {ds}.web_sales
"""

NO_BARRIER = _QUERY.format(sales=_CATALOG_SALES.format(ds=DATASET), ds=DATASET)
UNION_BARRIER = _QUERY.format(
    sales=_CATALOG_SALES.format(ds=DATASET) + " UNION ALL " + _WEB_SALES.format(ds=DATASET),
    ds=DATASET,
)


def _scan_estimates(sql):
    """{relation: estimated row count} for every Scan in the optimized plan."""
    from opteryx.models import ExecutionContext, QueryTelemetry
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.binder import do_bind_phase
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.optimizer import do_optimizer
    from opteryx.planner.optimizer.statistics_refresh import refresh_statistics
    from opteryx.planner.plan_rewriter import do_plan_rewrite
    from opteryx.planner.relation_resolver import do_resolve_relations
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide

    # QueryTelemetry is a SINGLETON keyed by query_id, and the no-arg form keys on
    # "" — two calls in one process would share counters and this test would read the
    # previous plan's readings. Key it per call.
    query_id = str(uuid.uuid4())
    telemetry = QueryTelemetry(query_id)
    ctx = ExecutionContext(access_policies=[{"pattern": "testdata.*", "role": "reader"}])

    clean = do_sql_rewrite(sql)
    parsed = sqloxide.parse_sql(clean, _dialect="opteryx")
    ast = do_ast_rewriter(parsed, parameters=[])[0]
    plan, _, ctes = do_logical_planning_phase(ast)
    plan = do_resolve_relations(plan, ctes, telemetry)
    plan = do_plan_rewrite(plan, telemetry)
    bound = do_bind_phase(plan, execution_context=ctx, query_id=query_id, telemetry=telemetry)
    optimized = do_optimizer(bound, telemetry)
    refresh_statistics(optimized, telemetry=telemetry)

    estimates = {
        row["relation"]: row["row_count"]
        for row in telemetry._reading.get("estimated_row_counts", [])
        if row.get("node_type") == "Scan" and row.get("relation")
    }
    return estimates, telemetry


needs_data = pytest.mark.skipif(
    not os.path.isdir("testdata/tpcds_1_skene"),
    reason="testdata/tpcds_1_skene not populated",
)


@needs_data
def test_predicates_reach_their_scans_without_a_barrier():
    """The control arm: with no Union in the plan, pushdown already worked."""
    estimates, _ = _scan_estimates(NO_BARRIER)

    assert estimates[f"{DATASET}.date_dim"] < 100, estimates
    assert estimates[f"{DATASET}.item"] < 1000, estimates


@needs_data
def test_a_union_leg_does_not_strand_the_other_legs_predicates():
    """The regression: the same predicates must still reach the same scans when a
    sibling leg is headed by a Union barrier."""
    baseline, _ = _scan_estimates(NO_BARRIER)
    estimates, telemetry = _scan_estimates(UNION_BARRIER)

    # The union adds a second fact table; it must not change what the DIMENSION
    # scans are estimated to read.
    assert estimates[f"{DATASET}.date_dim"] == baseline[f"{DATASET}.date_dim"], (
        f"date_dim scan estimate changed when a sibling leg became a UNION: "
        f"{baseline[f'{DATASET}.date_dim']} -> {estimates[f'{DATASET}.date_dim']} "
        f"-- its predicates were stranded above the join stack"
    )
    assert estimates[f"{DATASET}.item"] == baseline[f"{DATASET}.item"], estimates

    # And they got there by being retained ACROSS the barrier, not by luck.
    assert telemetry._reading["optimization_predicate_pushdown_barrier_retained"] > 0
    assert telemetry._reading["optimization_predicate_pushdown_unplaced"] == 0, (
        "a predicate was still restored above the join stack"
    )
