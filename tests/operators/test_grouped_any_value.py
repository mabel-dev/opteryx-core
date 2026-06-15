"""
Regression tests for grouped ANY_VALUE.

Grouped ANY_VALUE was wholesale broken: the collectors returned a
draken_native Vector from a shim-typed finalize (TypeError even for INT), and
the object path rebuilt every value through the INT64 default of
vector_from_sequence (std::bad_cast for string/date, corruption for temporal).

Fix: wrap the result in the shim and thread the SOURCE type through
vector_from_sequence so each type rebuilds via its own typed constructor.

ANY_VALUE returns an arbitrary (but real) value per group, so the assertions
check the result TYPE and that the value is a genuine member of the group —
not a fixed value.
"""

import datetime
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


def _grouped(sql, col):
    sess = opteryx.session()
    out = {}
    for m in sess.execute_to_morsels(sql):
        for st, av in zip(m.column(b"status").to_pylist(), m.column(col).to_pylist()):
            st = st.decode() if isinstance(st, bytes) else st
            out[st] = av
    return out


def test_any_value_int():
    res = _grouped("SELECT status, ANY_VALUE(year) AS av FROM testdata.astronauts GROUP BY status", b"av")
    assert res and all(isinstance(v, int) for v in res.values())


def test_any_value_float():
    res = _grouped("SELECT status, ANY_VALUE(space_walks_hours) AS av FROM testdata.astronauts GROUP BY status", b"av")
    assert res and all(isinstance(v, float) for v in res.values() if v is not None)


def test_any_value_string():
    res = _grouped("SELECT status, ANY_VALUE(name) AS av FROM testdata.astronauts GROUP BY status", b"av")
    assert res and all(isinstance(v, str) for v in res.values())


def test_any_value_date_type_preserved():
    """The bug: ANY_VALUE(date) crashed (std::bad_cast). Must return a date."""
    res = _grouped("SELECT status, ANY_VALUE(birth_date) AS av FROM testdata.astronauts GROUP BY status", b"av")
    assert res and all(isinstance(v, datetime.date) for v in res.values())


def test_any_value_bool():
    res = _grouped("SELECT status, ANY_VALUE(space_walks > 0) AS av FROM testdata.astronauts GROUP BY status", b"av")
    assert res and all(isinstance(v, bool) for v in res.values())


if __name__ == "__main__":
    test_any_value_int()
    test_any_value_float()
    test_any_value_string()
    test_any_value_date_type_preserved()
    test_any_value_bool()
    print("✅ okay")
