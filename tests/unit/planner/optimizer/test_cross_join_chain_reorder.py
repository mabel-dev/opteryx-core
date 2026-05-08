# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression tests for CrossJoinChainReorderStrategy.

The strategy reshapes a chain of implicit cross joins (`FROM A, B, C, D`) so
that adjacent operands share an equality edge in the join graph. Without it,
patterns like `FROM A, B, C WHERE A.x = C.x AND B.y = C.y` leave the bottom
`A x B` as a Cartesian — A and B have no direct predicate.

The original repro that motivated this strategy was TPC-H Q02 at SF=1 where
the bottom `part x supplier` Cartesian produced 40M rows of strings and blew
Draken's int32 string-offset limit. That repro lives in the TPC-H battery —
these tests exercise the strategy in isolation against the in-built `$planets`
dataset (with self-joins) so they are fast and self-contained.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "..", "..", "..", ".."))

import opteryx
from opteryx.planner.logical_planner import LogicalPlanStepType


def _count_cross_joins(plan) -> int:
    return sum(
        1
        for nid in plan.nodes()
        if plan[nid].node_type == LogicalPlanStepType.Join
        and plan[nid].type == "cross join"
        and not getattr(plan[nid], "on", None)
        and not getattr(plan[nid], "using", None)
    )


def _physical_plan_for(sql: str):
    session = opteryx.session()
    list(session.execute_to_morsels(sql))
    return session._plan


def test_three_table_chain_with_transitive_predicate_eliminates_cross_join():
    """
    `FROM A, B, C` where A and B connect only via C. Default left-deep order
    is `((A x B) x C)` which leaves the bottom as a Cartesian. After reorder
    the chain should become `((A x C) x B)` or similar — every cross join
    should be convertible to an inner join, so zero remain in the plan.

    Built using a triple self-join on $planets: a, c, b all on the same
    table; predicates connect a-c and b-c but not a-b.
    """
    sql = """
    SELECT a.name AS na, b.name AS nb, c.name AS nc
    FROM $planets a, $planets b, $planets c
    WHERE a.id = c.id
      AND b.id = c.id
    """
    plan = _physical_plan_for(sql)
    assert _count_cross_joins(plan) == 0, "all cross joins should be converted"


def test_chain_with_split_filters_collects_all_predicates():
    """
    SplitConjunctivePredicatesStrategy runs before us and explodes one WHERE
    into many Filter nodes. The reorder strategy must collect predicates
    across every Filter above the chain, not just one of them, otherwise it
    underspecifies connectivity and may pick a worse order than the original.
    """
    sql = """
    SELECT a.name AS na, b.name AS nb, c.name AS nc
    FROM $planets a, $planets b, $planets c
    WHERE a.id = c.id
      AND b.id = a.id
      AND c.name IS NOT NULL
    """
    plan = _physical_plan_for(sql)
    assert _count_cross_joins(plan) == 0


def test_explicit_cartesian_runs():
    """
    An explicit CROSS JOIN with no predicates is a real Cartesian. The
    strategy must not break it — the query should still execute and return
    the cross-product row count.
    """
    sql = "SELECT a.id AS a_id, b.id AS b_id FROM $planets a CROSS JOIN $planets b"
    rows = sum(
        m.num_rows for m in opteryx.session().execute_to_morsels(sql)
    )
    # 9 planets x 9 planets = 81 rows.
    assert rows == 81


def test_already_well_ordered_chain_works():
    """
    When the natural FROM order already gives a connected join graph at every
    step, the strategy must be a no-op and the query must still produce
    correct results.
    """
    sql = """
    SELECT a.name AS na
    FROM $planets a, $planets b
    WHERE a.id = b.id
    """
    rows = sum(
        m.num_rows for m in opteryx.session().execute_to_morsels(sql)
    )
    assert rows == 9


if __name__ == "__main__":
    test_three_table_chain_with_transitive_predicate_eliminates_cross_join()
    test_chain_with_split_filters_collects_all_predicates()
    test_explicit_cartesian_runs()
    test_already_well_ordered_chain_works()
    print("OK")
