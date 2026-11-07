"""The radix join probe must drop NULL probe keys, and only tests per-row
liveness when the probe key can be NULL.

A build of 200,000 rows is over kRadixMinBuildRows (131,072), so these joins take
the radix table. `generate_series` has no validity (the non-nullable branch); the
CASE makes every third probe key NULL (the nullable branch).
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

import opteryx  # noqa: E402

NULLABLE_PROBE = (
    "(SELECT CASE WHEN p % 3 = 0 THEN NULL ELSE p END AS k "
    "FROM generate_series(1, 300000) AS p) AS q"
)


def _row(sql):
    morsels = list(opteryx.session().execute_to_morsels(sql))
    assert len(morsels) == 1
    m = morsels[0]
    return tuple(m.column(c).to_pylist()[0] for c in m.column_names)


def test_radix_inner_join_non_nullable_probe():
    sql = (
        "SELECT COUNT(*) FROM generate_series(1, 200000) AS b "
        "INNER JOIN generate_series(1, 300000) AS p ON b = p"
    )
    assert _row(sql) == (200000,)


def test_radix_inner_join_nullable_probe_drops_nulls():
    sql = f"SELECT COUNT(*) FROM generate_series(1, 200000) AS b INNER JOIN {NULLABLE_PROBE} ON b = q.k"
    assert _row(sql) == (200000 - 200000 // 3,)


def test_radix_left_join_nullable_probe_keeps_null_rows():
    sql = f"SELECT COUNT(*), COUNT(b) FROM {NULLABLE_PROBE} LEFT JOIN generate_series(1, 200000) AS b ON b = q.k"
    assert _row(sql) == (300000, 200000 - 200000 // 3)


if __name__ == "__main__":  # pragma: no cover
    import pytest

    sys.exit(pytest.main([__file__, "-q"]))
