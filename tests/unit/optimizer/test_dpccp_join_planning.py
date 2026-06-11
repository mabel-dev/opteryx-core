# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""WP-11: DPccp join planning is enabled by default and reorders safely.

DPccp cost-based join enumeration is on by default
(``FEATURE_ENABLE_DPCCP_JOIN_PLANNING`` defaults to True). These tests guard
three things:
  * the flag stays on (a silent disable would quietly drop join reordering);
  * DPccp actually reorders a multi-join whose FROM order is suboptimal
    (i.e. it is not a no-op);
  * the reorder is semantically transparent — the result with DPccp on equals
    the result with it off.

DPccp's cost model uses per-relation row counts (always available); join-key
NDV is typically absent on this data, so reordering is row-count driven. That is
exactly why these correctness guards matter.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx
from opteryx.config import features

# A multi-join whose natural FROM order lists the largest table (lineitem) first;
# DPccp's row-count cost prefers to start from the smaller relations.
_REORDERED_QUERY = (
    "SELECT COUNT(*) AS c "
    "FROM testdata.tpch_001.lineitem l, testdata.tpch_001.orders o, "
    "testdata.tpch_001.customer c "
    "WHERE l.l_orderkey = o.o_orderkey AND o.o_custkey = c.c_custkey"
)


def _fetch_column(sql, column=b"c"):
    """Run sql and return the named column's values (the shared helper's
    to_arrow path is unavailable for these Morsels)."""
    values = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        if morsel.num_rows:
            values.extend(morsel.column(column).to_pylist())
    return values


def _apply_count(sql):
    """Run sql and return how many times DPccp applied a reorder."""
    import opteryx.planner.optimizer.strategies.join_planning as jp

    original = jp._apply_join_tree
    counter = {"n": 0}

    def _counting(*args, **kwargs):
        counter["n"] += 1
        return original(*args, **kwargs)

    jp._apply_join_tree = _counting
    try:
        _fetch_column(sql)
    finally:
        jp._apply_join_tree = original
    return counter["n"]


def test_dpccp_enabled_by_default():
    assert features.enable_dpccp_join_planning is True


def test_dpccp_reorders_a_suboptimal_join_order():
    # Proves DPccp is doing real work, not silently no-op'ing.
    assert _apply_count(_REORDERED_QUERY) >= 1


def test_dpccp_reorder_is_result_preserving():
    # The reorder must not change the answer: DPccp on == DPccp off.
    original = features.enable_dpccp_join_planning
    try:
        features.enable_dpccp_join_planning = True
        on_result = _fetch_column(_REORDERED_QUERY)
        features.enable_dpccp_join_planning = False
        off_result = _fetch_column(_REORDERED_QUERY)
    finally:
        features.enable_dpccp_join_planning = original
    assert on_result == off_result
    assert on_result[0] > 0


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
