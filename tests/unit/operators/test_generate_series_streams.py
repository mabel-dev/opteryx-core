"""
GENERATE_SERIES over integers is a native Source that streams the series in 64k-row
morsels; the series is never built. These tests would exhaust memory (or be unusably slow)
against a materialising implementation, and pin the values every slice must carry.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from tests.helpers import execute_and_fetch_all


def test_every_slice_carries_the_right_values():
    # 3+ slices of 65_536: a wrong slice start would break the sum, min or max.
    n = 200_000
    (row,) = execute_and_fetch_all(
        f"SELECT COUNT(*) AS c, MIN(g) AS lo, MAX(g) AS hi, SUM(g) AS s FROM GENERATE_SERIES(1, {n}) AS g"
    )
    assert row == {"c": n, "lo": 1, "hi": n, "s": n * (n + 1) // 2}


def test_morsels_are_at_most_64k_rows():
    session = opteryx.session()
    morsels = list(session.execute_to_morsels("SELECT * FROM GENERATE_SERIES(1, 200000) AS g"))
    assert sum(m.num_rows for m in morsels) == 200_000
    assert max(m.num_rows for m in morsels) <= 65_536
    session.close()


def test_a_billion_row_series_is_not_materialised():
    # 50M rows was ~1.8 GB of Python ints before; streamed, it is one morsel per worker.
    n = 50_000_000
    (row,) = execute_and_fetch_all(
        f"SELECT COUNT(*) AS c, SUM(g) AS s FROM GENERATE_SERIES(1, {n}) AS g"
    )
    assert row == {"c": n, "s": n * (n + 1) // 2}


def test_step_and_direction():
    def series(sql):
        return sorted(r["g"] for r in execute_and_fetch_all(f"SELECT g FROM {sql} AS g"))

    assert series("GENERATE_SERIES(5)") == [1, 2, 3, 4, 5]
    assert series("GENERATE_SERIES(1, 10, 3)") == [1, 4, 7, 10]
    assert series("GENERATE_SERIES(10, 1, -3)") == [1, 4, 7, 10]
    assert series("GENERATE_SERIES(-3, 3, 2)") == [-3, -1, 1, 3]


def test_an_empty_series_has_no_rows():
    (row,) = execute_and_fetch_all("SELECT COUNT(*) AS c FROM GENERATE_SERIES(5, 1) AS g")
    assert row == {"c": 0}


def test_ordering_across_slices():
    rows = execute_and_fetch_all("SELECT g FROM GENERATE_SERIES(1, 300000) AS g ORDER BY g DESC LIMIT 3")
    assert [r["g"] for r in rows] == [300_000, 299_999, 299_998]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
