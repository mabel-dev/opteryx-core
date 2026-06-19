# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Guards for TimestampCastSinkStrategy.

The strategy retypes an int64 scan column to TIMESTAMP64 when the column is
referenced *only* as a pure-retag ``::TIMESTAMP[unit]`` cast, so the reader
retags it and the cast resolves to identity. These tests pin the fail-safe
eligibility decisions (fire / skip) and that results are unchanged.
"""

import os
import sys
import uuid

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from draken.draken_native import DrakenType
from opteryx.models import ExecutionContext, QueryTelemetry
from opteryx.planner.ast_rewriter import do_ast_rewriter
from opteryx.planner.binder import do_bind_phase
from opteryx.planner.logical_planner import LogicalPlanStepType, do_logical_planning_phase
from opteryx.planner.optimizer import do_optimizer
from opteryx.planner.plan_rewriter import do_plan_rewrite
from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.third_party import sqloxide

TABLE = "testdata.tpch_001.lineitem"


def _scan_physical(sql: str, column: str):
    telemetry = QueryTelemetry()
    ctx = ExecutionContext(access_policies=[{"pattern": "testdata.*", "role": "reader"}])
    plan, _, ctes = do_logical_planning_phase(
        do_ast_rewriter(sqloxide.parse_sql(do_sql_rewrite(sql), _dialect="opteryx"), parameters=[])[0]
    )
    plan = do_plan_rewrite(plan, ctes, telemetry)
    bound = do_bind_phase(
        plan, execution_context=ctx, query_id=str(uuid.uuid4()),
        common_table_expressions=ctes, telemetry=telemetry,
    )
    opt = do_optimizer(bound, telemetry)
    out = []
    for _, node in opt.nodes(True):
        if node.node_type == LogicalPlanStepType.Scan:
            for col in node.schema.columns:
                if col.name == column:
                    out.append(col.column_type.physical)
    return out


def test_cast_only_column_is_retyped():
    # the only use is a pure-retag cast -> scan column becomes TIMESTAMP64.
    assert _scan_physical(f"SELECT l_suppkey::TIMESTAMP[s] AS m FROM {TABLE}", "l_suppkey") == [
        DrakenType.TIMESTAMP64
    ]


def test_mixed_raw_and_cast_use_is_skipped():
    # used raw AND cast -> cannot retype, stays INT64.
    assert _scan_physical(
        f"SELECT l_suppkey, l_suppkey::TIMESTAMP[s] AS m FROM {TABLE}", "l_suppkey"
    ) == [DrakenType.INT64]


def test_predicated_column_is_skipped():
    # a pushed-down predicate on the column disqualifies retyping.
    assert _scan_physical(
        f"SELECT l_suppkey::TIMESTAMP[s] AS m FROM {TABLE} WHERE l_suppkey > 5", "l_suppkey"
    ) == [DrakenType.INT64]


def test_conflicting_units_are_skipped():
    # two casts with different units -> ambiguous, stays INT64.
    assert _scan_physical(
        f"SELECT l_suppkey::TIMESTAMP[s] AS a, l_suppkey::TIMESTAMP[us] AS b FROM {TABLE}",
        "l_suppkey",
    ) == [DrakenType.INT64]


def test_results_unchanged_by_sink():
    # The retag must be value-identical to the cast: int 93 -> 93 seconds.
    rows = []
    for m in opteryx.session().execute_to_morsels(
        f"SELECT l_suppkey, l_suppkey::TIMESTAMP[s] AS ts FROM {TABLE} ORDER BY l_suppkey LIMIT 1"
    ):
        for i in range(len(m)):
            rows.append(m[i])
    suppkey, ts = rows[0]
    assert ts.hour * 3600 + ts.minute * 60 + ts.second == suppkey
    assert ts.microsecond == 0


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
