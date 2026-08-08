"""
Regression tests for grouped MIN/MAX over int-backed temporal types.

Before the type-preserving finalize, MIN/MAX(DATE/TIMESTAMP/TIME) GROUP BY ran
through MinMaxObjectCollector, which boxed the column to Python, string-encoded
the datetimes, then tagged the result buffer as the temporal type — producing
corrupt values (e.g. `year -286034 out of range`) and the wrong result TYPE.

These types now route to the nogil MinMaxInt64Collector with type-preserving
finalize (the min/max is computed on the raw int representation, then the int64
result is reinterpreted back to the source type carrying its unit).

Ground-truth values cross-checked against DuckDB over the same parquet.
"""

import datetime
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


def _grouped(sql, *cols):
    sess = opteryx.session()
    out = {}
    for m in sess.execute_to_morsels(sql):
        key = m.column(b"status").to_pylist()
        vals = [m.column(c).to_pylist() for c in cols]
        for i, k in enumerate(key):
            k = k.decode() if isinstance(k, bytes) else k
            out[k] = tuple(v[i] for v in vals)
    return out


def test_grouped_min_max_date_values_and_type():
    """MIN/MAX(DATE) GROUP BY returns datetime.date with correct values."""
    res = _grouped(
        "SELECT status, MAX(birth_date) AS mx, MIN(birth_date) AS mn "
        "FROM testdata.astronauts GROUP BY status",
        b"mx",
        b"mn",
    )
    # Type must be date, not a corrupt int/string reinterpretation.
    for mx, mn in res.values():
        assert isinstance(mx, datetime.date), type(mx)
        assert isinstance(mn, datetime.date), type(mn)
        assert mn <= mx
    # Values cross-checked against DuckDB.
    assert res["Active"] == (datetime.date(1978, 10, 14), datetime.date(1955, 4, 20))
    assert res["Retired"] == (datetime.date(1968, 2, 10), datetime.date(1921, 7, 18))
    assert res["Deceased"] == (datetime.date(1963, 3, 12), datetime.date(1923, 3, 12))
    assert res["Management"] == (datetime.date(1968, 11, 20), datetime.date(1943, 7, 9))


def test_grouped_max_timestamp_type_preserved():
    """MAX(TIMESTAMP) GROUP BY returns datetime.datetime, not a raw epoch int."""
    res = _grouped(
        "SELECT status, MAX(CAST(birth_date AS TIMESTAMP)) AS mx "
        "FROM testdata.astronauts GROUP BY status",
        b"mx",
    )
    for (mx,) in res.values():
        assert isinstance(mx, datetime.datetime), type(mx)
    # Same calendar instant as the DATE MAX above.
    assert res["Active"][0].date() == datetime.date(1978, 10, 14)


def test_grouped_min_max_bool():
    """MIN/MAX(BOOL) GROUP BY returns bool (MIN=AND/false-dominates, MAX=OR/true)."""
    res = _grouped(
        "SELECT status, MIN(space_walks > 0) AS mn, MAX(space_walks > 0) AS mx "
        "FROM testdata.astronauts GROUP BY status",
        b"mn",
        b"mx",
    )
    for mn, mx in res.values():
        assert isinstance(mn, bool), type(mn)
        assert isinstance(mx, bool), type(mx)
    # Every status group has both walkers and non-walkers in this dataset.
    assert res["Active"] == (False, True)


# MIN/MAX(INTERVAL) had a test here. It drove the Cython GroupHashEngine and its
# _DeferredMin/MaxCollector directly, because interval columns cannot be produced
# through grouped-agg SQL in this build (interval arithmetic gaps). That engine was
# deleted when the Cython operator push paths were removed (execution is 100%
# native), taking the only coverage of grouped MIN/MAX over INTERVAL with it —
# there is no SQL-level expression of the same behaviour to port the test to.
# Re-add coverage here if grouped-agg SQL ever gains interval operands.


def test_grouped_min_max_int_unregressed():
    """Plain integer MIN/MAX (the shared collector) still returns ints."""
    res = _grouped(
        "SELECT status, MAX(year) AS mx, MIN(year) AS mn "
        "FROM testdata.astronauts GROUP BY status",
        b"mx",
        b"mn",
    )
    for mx, mn in res.values():
        assert isinstance(mx, int), type(mx)
        assert isinstance(mn, int), type(mn)
        assert mn <= mx


if __name__ == "__main__":
    test_grouped_min_max_date_values_and_type()
    test_grouped_max_timestamp_type_preserved()
    test_grouped_min_max_int_unregressed()
    print("✅ okay")
