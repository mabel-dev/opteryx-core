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


def test_grouped_min_max_interval():
    """MIN/MAX(INTERVAL) ordered by the engine's approximate fold (months*30d+ms),
    keeping the winning row's original (months, ms) slot; nulls skipped.

    Driven directly through GroupHashEngine because interval columns can't be
    produced via grouped-agg SQL in this build (interval arithmetic gaps).
    """
    import draken.draken_native as dn
    from draken.vectors.vector import Vector as V
    from draken.morsels.morsel import Morsel
    import opteryx.operators._operators as ops

    g = V(dn.vector_from_sequence([0, 1, 0, 1, 0]))
    d = V(dn.vector_interval_from_sequence([(1, 500), (0, 200), (3, 0), None, (0, 999)]))
    m = Morsel.from_vectors([b"g", b"d"], [g, d])

    mn = ops._DeferredMinCollector(); mn.column_name = b"d"; mn.result_name = b"mn"
    mx = ops._DeferredMaxCollector(); mx.column_name = b"d"; mx.result_name = b"mx"
    eng = ops.GroupHashEngine([b"g"], [mn, mx], True, False)
    eng.ingest(m)
    res = {}
    for out in eng.finalize_morsels():
        for gg, a, b in zip(out.column(b"g").to_pylist(),
                            out.column(b"mn").to_pylist(),
                            out.column(b"mx").to_pylist()):
            res[gg] = (a, b)
    # group 0: folds 2_592_000_500 / 7_776_000_000 / 999 → MIN (0,999), MAX (3,0)
    assert res[0] == ((0, 999), (3, 0)), res[0]
    # group 1: only non-null (0,200); the None row is skipped.
    assert res[1] == ((0, 200), (0, 200)), res[1]


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
