"""Regression test for keying (GROUP BY / DISTINCT / JOIN) on a BOOLEAN key.

Pre-fix, the draken ops table had no hash kernel registered for DRAKEN_BOOL.
Any group-by / distinct / join keyed on a boolean column routed through
draken_hash → hash_shaped → draken_hash, which threw

    ValueError: draken_hash: unsupported type

even though the key store already had a bool storage path. A bool is bit-packed
(1 bit per logical row), so it needs its own hash kernel that reads
``(data[code >> 3] >> (code & 7)) & 1`` via the uniform ``data[selection[i]]``
access pattern and bakes NULL_HASH for null rows, matching the other kernels.

$planets has 9 rows; ``id`` is 1..9 (INT8), ``gravity`` is FLOAT64 with no nulls.

NOTE: forms that re-project the computed key in the SELECT list, e.g.
``SELECT (id>5) AS k, COUNT(*) FROM $planets GROUP BY (id>5)``, hit a separate,
pre-existing planner bug (the projection re-evaluates the key expression after
aggregation, when the source column is gone) that also fails for non-bool keys
like ``(id+1)``. Those forms are intentionally NOT exercised here.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import opteryx


def _rows(sql):
    s = opteryx.session()
    out = []
    for m in s.execute_to_morsels(sql):
        cols = {(n.decode() if isinstance(n, bytes) else n): m.column(n).to_pylist()
                for n in m.column_names}
        out = [tuple(r) for r in zip(*cols.values())]
    s.close()
    return out


def test_group_by_bool_key_int_source():
    # id>5 -> True for ids 6..9 (4 rows), False for ids 1..5 (5 rows).
    got = sorted(c for (c,) in _rows("SELECT COUNT(*) c FROM $planets GROUP BY (id>5)"))
    assert got == [4, 5], got


def test_group_by_bool_key_float_source():
    # gravity>5 partitions all 9 rows; counts must sum to 9 (no nulls in gravity).
    got = sorted(c for (c,) in _rows("SELECT COUNT(*) c FROM $planets GROUP BY (gravity>5)"))
    assert sum(got) == 9, got
    assert got == [3, 6], got


def test_group_by_bool_key_with_nulls():
    sql = (
        "SELECT k, COUNT(*) c FROM "
        "(VALUES (true),(false),(NULL),(true),(NULL)) AS t(k) "
        "GROUP BY k"
    )
    got = {k: c for k, c in _rows(sql)}
    # true:2, false:1, null:2 — null forms its own group (NULL_HASH baked).
    assert got == {True: 2, False: 1, None: 2}, got


def test_distinct_bool_key():
    got = sorted(b for (b,) in _rows("SELECT DISTINCT (id>5) AS k FROM $planets"))
    assert got == [False, True], got


def test_join_on_bool_key():
    sql = (
        "SELECT a.k, b.v FROM (VALUES (true),(false)) AS a(k) "
        "JOIN (VALUES (true,10),(false,20)) AS b(k,v) ON a.k=b.k"
    )
    got = sorted(_rows(sql), key=str)
    assert got == [(False, 20), (True, 10)], got


if __name__ == "__main__":
    for fn in [
        test_group_by_bool_key_int_source,
        test_group_by_bool_key_float_source,
        test_group_by_bool_key_with_nulls,
        test_distinct_bool_key,
        test_join_on_bool_key,
    ]:
        fn()
        print("PASS", fn.__name__)
