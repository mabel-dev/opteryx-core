"""
Regression test: CAST(<narrow int> AS VARCHAR).

`$planets.id` is INT8. `SELECT CAST(id AS VARCHAR)` previously raised
``No native CAST INT8 → VARCHAR`` — the VARCHAR cast resolver handled INT64,
FLOAT, BOOL, TIMESTAMP and DATE sources but not the narrow ints (INT8/INT16/
INT32). Fixed by widening narrow→int64 first, then int64→string (the same
two-step the INTEGER/DOUBLE/TIMESTAMP targets already use for narrow sources).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def _col(sql, name="x"):
    rows = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        rows.extend(morsel.column(name).to_pylist())
    return rows


def test_cast_int8_column_to_varchar():
    # $planets.id is INT8 (values 1..9).
    out = _col("SELECT CAST(id AS VARCHAR) AS x FROM $planets ORDER BY id")
    assert out == [str(i) for i in range(1, 10)], out
    assert all(isinstance(v, str) for v in out), out


def test_cast_narrow_int_to_varchar_in_concat():
    out = _col("SELECT name || '#' || CAST(id AS VARCHAR) AS x FROM $planets ORDER BY id LIMIT 2")
    assert out == ["Mercury#1", "Venus#2"], out


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
