# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""SemiJoinReducerStrategy — fires where a build/group disappears, declines elsewhere.

The DECLINE cases matter as much as the wins here. This strategy copies a subplan, so
a reducer that does not remove work is a straight loss, and the two ways of getting
that wrong are both measured, both regressions we have already had, and both cheap to
reintroduce:

  * TPC-H Q19 — predicates already pushed, so a reducer only trims join PROBES.
    Measured 0.80x, 20% slower. Probe misses are already cheap.
  * TPC-H Q21 — the source leg carries its own 60M-row `lineitem` scan, so copying it
    to reduce a 60M-row build costs more than it saves. Measured slower with the
    reducer than without.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.connectors import DiskConnector

opteryx.register_workspace("testdata", DiskConnector)

DATASET = "testdata.tpch_001"


def _run(sql):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is not None:
            for index in range(morsel.num_rows):
                rows.append(morsel[index])
    telemetry = dict(session.telemetry)
    session.close()
    return rows, telemetry.get("optimization_semi_join_reducer", 0)


def _query(number):
    path = os.path.join(
        os.path.dirname(__file__),
        "../../../tests/performance/tpch/opteryx/queries",
        f"query{number}.sql",
    )
    return open(path).read().replace("testdata.tpch.", DATASET + ".")


def test_reducer_does_not_change_answers():
    """Every query it fires on must return exactly what it returned without it."""
    for number in ("04", "17", "19", "20", "21"):
        sql = _query(number)
        with_reducer, _ = _run(sql)
        os.environ["FEATURE_DISABLE_SEMI_JOIN_REDUCER"] = "true"
        try:
            import importlib

            import opteryx.config

            importlib.reload(opteryx.config)
            without_reducer, _ = _run(sql)
        finally:
            del os.environ["FEATURE_DISABLE_SEMI_JOIN_REDUCER"]
            importlib.reload(opteryx.config)
        assert sorted(map(str, with_reducer)) == sorted(map(str, without_reducer)), (
            f"Q{number}: the reducer changed the answer"
        )


def test_declines_when_the_source_leg_is_not_cheaper_than_the_build():
    """Q21's source leg contains its own full `lineitem` scan — copying it loses."""
    _rows, fired = _run(_query("21"))
    assert fired == 0, "Q21 must not be reduced: its source leg costs more than it saves"


def test_declines_a_plain_inner_join_probe():
    """Q19 has no grouped aggregate and no semi/anti build to remove — 0.80x if reduced."""
    _rows, fired = _run(_query("19"))
    assert fired == 0, "Q19 must not be reduced: a reducer there only trims probes"


def test_no_reducer_without_a_candidate_join():
    """`should_i_run` must be false, or every trivial query pays a statistics refresh."""
    from opteryx.models import QueryTelemetry
    from opteryx.planner.logical_planner import LogicalPlan
    from opteryx.planner.optimizer.strategies.semi_join_reducer import SemiJoinReducerStrategy

    strategy = SemiJoinReducerStrategy(QueryTelemetry())
    assert strategy.should_i_run(LogicalPlan()) is False


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
