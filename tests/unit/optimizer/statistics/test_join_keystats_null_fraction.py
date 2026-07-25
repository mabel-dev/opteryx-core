# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""WP-6: join cardinality must consume join-key null fractions.

``_join_stats`` builds ``KeyStats`` from each join key's ``ColumnStatistics``.
The null fraction was previously hard-coded to ``None`` even when the column
carried one, so the estimator's null discount (``_effective_rows``) never fired.
These tests pin the wiring: a null-heavy join key now reduces the estimated
output cardinality.
"""

import os
import sys
from types import SimpleNamespace

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

from opteryx.planner.optimizer.statistics import ColumnStatistics
from opteryx.planner.optimizer.statistics import RelationStatistics
from opteryx.planner.optimizer.statistics_refresh import _join_stats


# Join keys reach _join_stats as raw column *identities* (opaque bytes), which
# is also how RelationStatistics.columns is keyed. Anything name-shaped here
# would not resolve — and would silently re-create the dead-lookup bug where
# every join-key NDV read returned None and the estimator fell back to tdom.
_LK = b"tes_lk_00000001"
_RK = b"tes_rk_00000002"


def _join_node():
    return SimpleNamespace(
        type="inner",
        left_columns=[_LK],
        right_columns=[_RK],
    )


def _estimate(left_null_fraction):
    """Estimate inner-join row_count with the left key carrying the given null
    fraction; both sides 1000 rows, NDV 100 (so per-key selectivity 1/100)."""
    left = RelationStatistics(
        row_count=1000,
        columns={
            _LK: ColumnStatistics(
                column_name="lk", data_type="INTEGER", distinct_count=100, null_fraction=left_null_fraction
            )
        },
    )
    right = RelationStatistics(
        row_count=1000,
        columns={
            _RK: ColumnStatistics(column_name="rk", data_type="INTEGER", distinct_count=100, null_fraction=0.0)
        },
    )
    child_stats = [(left, "left"), (right, "right")]
    return _join_stats(_join_node(), child_stats).row_count


def test_null_fraction_halves_effective_rows():
    # 1000 * 1000 * (1/100) = 10000 with no nulls; the 0.5 null key discounts the
    # left side's effective rows by half -> ~5000.
    baseline = _estimate(0.0)
    half_null = _estimate(0.5)
    assert baseline == pytest.approx(10000, rel=0.05), baseline
    assert half_null < baseline
    assert half_null == pytest.approx(baseline * 0.5, rel=0.05), (half_null, baseline)


def test_none_null_fraction_behaves_like_zero():
    assert _estimate(None) == _estimate(0.0)


def test_all_null_key_column_collapses_cardinality():
    # A wholly-null join key matches nothing; estimate must shrink, not crash.
    all_null = _estimate(1.0)
    assert all_null < _estimate(0.0)
    assert all_null >= 0


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
