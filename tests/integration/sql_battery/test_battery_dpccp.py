# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Correctness battery for ``JoinPlanningStrategy`` (DPccp).

The battery runs a fixed set of multi-relation queries with the DPccp flag
on and asserts each result matches the same query run with the flag off.
This is correctness only — the bake-off versus other planners (TPC-H / JOB
at scale) is the next ticket.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx
from opteryx.config import features

# fmt:off
STATEMENTS = [
    # 2-relation: trivial. Flag-on must be a no-op.
    "SELECT a.name FROM $planets a, $planets b WHERE a.id = b.id",
    # 3-relation chain with manifests (testdata/satellites)
    "SELECT a.id AS ai, b.id AS bi, c.id AS ci "
    "FROM testdata.satellites a, testdata.satellites b, testdata.satellites c "
    "WHERE a.id = b.id AND b.id = c.id LIMIT 5",
    # 3-relation with transitive predicate (a-c, b-c, no a-b direct edge)
    "SELECT a.id AS ai, b.id AS bi, c.id AS ci "
    "FROM testdata.satellites a, testdata.satellites b, testdata.satellites c "
    "WHERE a.id = c.id AND b.id = c.id LIMIT 5",
    # 4-relation star with manifests
    "SELECT a.id AS ai, b.id AS bi, c.id AS ci, d.id AS di "
    "FROM testdata.satellites a, testdata.satellites b, testdata.satellites c, testdata.satellites d "
    "WHERE a.id = b.id AND a.id = c.id AND a.id = d.id LIMIT 5",
    # Outer join: must NOT be re-planned
    "SELECT a.name AS na, b.name AS nb FROM $planets a LEFT OUTER JOIN $planets b ON a.id = b.id",
    # Mixed inner + cross
    "SELECT a.name AS na, b.name AS nb, c.name AS nc "
    "FROM $planets a, $planets b, $planets c "
    "WHERE a.id = b.id AND b.id = c.id LIMIT 5",
]
# fmt:on


def _rows(sql: str) -> int:
    return sum(len(m) for m in opteryx.session().execute_to_morsels(sql))


@pytest.mark.parametrize("statement", STATEMENTS)
def test_battery_dpccp_correctness(statement):
    prev = features.enable_dpccp_join_planning
    try:
        features.enable_dpccp_join_planning = False
        baseline = _rows(statement)
        features.enable_dpccp_join_planning = True
        candidate = _rows(statement)
    finally:
        features.enable_dpccp_join_planning = prev
    assert candidate == baseline, f"DPccp changed result count for: {statement}"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING DPccp BATTERY OF {len(STATEMENTS)} TESTS")
    for i, sql in enumerate(STATEMENTS):
        test_battery_dpccp_correctness(sql)
        print(f"  {i + 1:02d} OK")
    print("--- done")
