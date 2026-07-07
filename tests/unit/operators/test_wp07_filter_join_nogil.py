"""
WP-07 — filter_join (LEFT SEMI / ANTI / ANTI-NULL-AWARE) true-nogil probe path.

These assert BYTE-IDENTICAL results (exact row multisets) for every shape the
converted `push_left` handles: the CarcharSet hash path and the PerfectHashSet
narrow-int path, single- and multi-column keys, empty build/probe sides,
duplicate (one-to-many) probe rows, the three null-semantics modes, and the
implicit numeric join-key cast. $planets carries ids 1..9 (contiguous), so the
expected sets below are pinned to `WHERE id <= N` bounds, not a row count.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pyarrow as pa  # test-only dep (allowed in tests/)
import pyarrow.parquet as pq
import pytest

import opteryx


def _write(dataset_dir, columns):
    """Write one parquet file. `columns` = {name: (pyarrow_type, py_list)}."""
    os.makedirs(dataset_dir, exist_ok=True)
    arrays = {name: pa.array(vals, type=typ) for name, (typ, vals) in columns.items()}
    pq.write_table(pa.table(arrays), os.path.join(dataset_dir, "part.parquet"))
    return dataset_dir


def _rows(sql):
    """Run `sql` and return an order-insensitive multiset of rows (each row a
    tuple of per-column reprs). A dropped/extra row, wrong pairing, or wrong null
    all change the set."""
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        names = list(morsel.column_names)
        for i in range(morsel.num_rows):
            out.append(tuple(
                repr(None if morsel.column(n) is None else morsel.column(n)[i])
                for n in names
            ))
    return sorted(out)


def _ids(sql):
    """Single-column int result → sorted list of python ints."""
    return sorted(int(r[0].strip("'")) if r[0].startswith("'") else int(r[0]) for r in _rows(sql))


def test_semi_in_carchar():
    # LEFT SEMI (IN), int64 key, CarcharSet path.
    assert _ids("SELECT p.id FROM $planets AS p WHERE p.id IN (SELECT q.id FROM $planets AS q WHERE q.id <= 4)") == [1, 2, 3, 4]


def test_anti_not_in_carchar_no_null():
    # LEFT ANTI-NULL-AWARE (NOT IN), no null on the right → clean anti.
    assert _ids("SELECT p.id FROM $planets AS p WHERE p.id NOT IN (SELECT q.id FROM $planets AS q WHERE q.id <= 4)") == [5, 6, 7, 8, 9]


def test_intersect():
    assert _ids("SELECT p.id FROM $planets AS p WHERE p.id <= 5 INTERSECT SELECT q.id FROM $planets AS q WHERE q.id >= 3") == [3, 4, 5]


def test_except():
    assert _ids("SELECT p.id FROM $planets AS p WHERE p.id <= 5 EXCEPT SELECT q.id FROM $planets AS q WHERE q.id >= 3") == [1, 2]


def test_semi_empty_build_side():
    # Empty build (right) side → semi produces nothing.
    assert _ids("SELECT p.id FROM $planets AS p WHERE p.id IN (SELECT q.id FROM $planets AS q WHERE q.id > 100)") == []


def test_not_in_empty_build_side():
    # NOT IN over an empty (null-free) build side → every left row survives.
    assert _ids("SELECT p.id FROM $planets AS p WHERE p.id NOT IN (SELECT q.id FROM $planets AS q WHERE q.id > 100)") == list(range(1, 10))


def test_not_in_with_null_on_right_is_empty():
    # NOT IN with a NULL anywhere on the right → every left row is UNKNOWN → empty.
    sql = ("SELECT p.id FROM $planets AS p WHERE p.id NOT IN ("
           "SELECT CASE WHEN q.id = 1 THEN NULL ELSE q.id END AS k FROM $planets AS q WHERE q.id <= 4)")
    assert _ids(sql) == []


def test_in_with_null_on_right_excludes_null_rows():
    # IN with a NULL on the right: the null contributes no match; non-null keys still match.
    sql = ("SELECT p.id FROM $planets AS p WHERE p.id IN ("
           "SELECT CASE WHEN q.id = 1 THEN NULL ELSE q.id END AS k FROM $planets AS q WHERE q.id <= 4)")
    assert _ids(sql) == [2, 3, 4]


def test_semi_multicolumn_key():
    # Multi-column composite key (INTERSECT over two columns).
    sql = ("SELECT p.id, p.name FROM $planets AS p WHERE p.id <= 4 "
           "INTERSECT SELECT q.id, q.name FROM $planets AS q WHERE q.id >= 3")
    assert _rows(sql) == sorted([("3", "'Earth'"), ("4", "'Mars'")])


def test_semi_one_to_many_keeps_all_probe_rows():
    # Duplicate probe keys: semi keeps ALL matching probe rows (not deduped).
    # id % 3 == 1 for id in {1,4,7} → three rows of g == 1.
    sql = ("SELECT g FROM (SELECT id % 3 AS g FROM $planets) AS t "
           "WHERE g IN (SELECT r.id FROM $planets AS r WHERE r.id = 1)")
    assert _ids(sql) == [1, 1, 1]


# ---- PerfectHashSet narrow-int path + null semantics via parquet fixtures ----

def test_phash_int8_semi(tmp_path):
    left = _write(str(tmp_path / "l"), {"k": (pa.int8(), [1, 2, 3, 4, 5])})
    right = _write(str(tmp_path / "r"), {"k": (pa.int8(), [2, 3, 4])})
    assert _ids(f"SELECT k FROM '{left}' WHERE k IN (SELECT k FROM '{right}')") == [2, 3, 4]


def test_phash_int8_anti(tmp_path):
    left = _write(str(tmp_path / "l"), {"k": (pa.int8(), [1, 2, 3, 4, 5])})
    right = _write(str(tmp_path / "r"), {"k": (pa.int8(), [2, 3, 4])})
    assert _ids(f"SELECT k FROM '{left}' WHERE k NOT IN (SELECT k FROM '{right}')") == [1, 5]


def test_phash_not_in_with_right_null_is_empty(tmp_path):
    # Right build side (int8) carries a NULL → NOT IN is UNKNOWN for every left row.
    left = _write(str(tmp_path / "l"), {"k": (pa.int8(), [1, 2, 3])})
    right = _write(str(tmp_path / "r"), {"k": (pa.int8(), [2, None])})
    assert _ids(f"SELECT k FROM '{left}' WHERE k NOT IN (SELECT k FROM '{right}')") == []


def test_phash_in_with_right_null_excludes_null(tmp_path):
    left = _write(str(tmp_path / "l"), {"k": (pa.int8(), [1, 2, 3])})
    right = _write(str(tmp_path / "r"), {"k": (pa.int8(), [2, None])})
    assert _ids(f"SELECT k FROM '{left}' WHERE k IN (SELECT k FROM '{right}')") == [2]


def test_phash_probe_null_triggers_carchar_demotion(tmp_path):
    # Probe (left) side carries a NULL: the PerfectHashSet can't answer it, so the
    # operator demotes to the rebuilt CarcharSet path. NULL IN (...) is UNKNOWN.
    left = _write(str(tmp_path / "l"), {"k": (pa.int8(), [1, 2, None, 3])})
    right = _write(str(tmp_path / "r"), {"k": (pa.int8(), [2, 3])})
    assert _ids(f"SELECT k FROM '{left}' WHERE k IN (SELECT k FROM '{right}')") == [2, 3]


def test_implicit_numeric_key_cast(tmp_path):
    # Left key INTEGER, right key DOUBLE → implicit numeric coercion (cast plan
    # → cxx_cast_column_c). Left {1,2,3} cast to double, matched against {2.0,3.0,4.0}.
    left = _write(str(tmp_path / "l"), {"k": (pa.int32(), [1, 2, 3])})
    right = _write(str(tmp_path / "r"), {"k": (pa.float64(), [2.0, 3.0, 4.0])})
    assert _ids(f"SELECT k FROM '{left}' WHERE k IN (SELECT k FROM '{right}')") == [2, 3]


if __name__ == "__main__":
    import tests

    tests.run_tests()
