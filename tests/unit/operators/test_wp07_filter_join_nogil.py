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
    # Left key INTEGER, right key DOUBLE → implicit numeric coercion. A join keys on
    # raw column buffers, so INT64 2 and FLOAT64 2.0 hash differently; without the
    # coercion this returned NO rows — a silent wrong answer, not an error. The
    # compiler materializes a CAST column on the narrower side and keys on that
    # (_join_key_coercions / _coerce_join_keys in managers/execution/compiler.py).
    left = _write(str(tmp_path / "l"), {"k": (pa.int32(), [1, 2, 3])})
    right = _write(str(tmp_path / "r"), {"k": (pa.float64(), [2.0, 3.0, 4.0])})
    assert _ids(f"SELECT k FROM '{left}' WHERE k IN (SELECT k FROM '{right}')") == [2, 3]


def test_implicit_numeric_key_cast_anti(tmp_path):
    # The ANTI direction is the dangerous one: an uncoerced key made NOT IN return
    # rows it should have EXCLUDED (all of {1,2,3} instead of just {1}).
    left = _write(str(tmp_path / "l"), {"k": (pa.int32(), [1, 2, 3])})
    right = _write(str(tmp_path / "r"), {"k": (pa.float64(), [2.0, 3.0, 4.0])})
    assert _ids(f"SELECT k FROM '{left}' WHERE k NOT IN (SELECT k FROM '{right}')") == [1]


def test_implicit_numeric_key_cast_exists(tmp_path):
    # Correlated EXISTS/NOT EXISTS key on the same path and need the same coercion.
    left = _write(str(tmp_path / "l"), {"k": (pa.int32(), [1, 2, 3])})
    right = _write(str(tmp_path / "r"), {"k": (pa.float64(), [2.0, 3.0, 4.0])})
    assert _ids(
        f"SELECT k FROM '{left}' AS a WHERE EXISTS "
        f"(SELECT 1 FROM '{right}' AS b WHERE a.k = b.k)"
    ) == [2, 3]
    assert _ids(
        f"SELECT k FROM '{left}' AS a WHERE NOT EXISTS "
        f"(SELECT 1 FROM '{right}' AS b WHERE a.k = b.k)"
    ) == [1]


def test_implicit_numeric_key_cast_does_not_match_fractional(tmp_path):
    # The coercion widens the INT side to DOUBLE — it must not round the DOUBLE side
    # down to an int, which would make 2.5 match 2. Nothing may match here.
    left = _write(str(tmp_path / "l"), {"k": (pa.int32(), [1, 2, 3])})
    right = _write(str(tmp_path / "r"), {"k": (pa.float64(), [2.5, 3.5])})
    assert _ids(f"SELECT k FROM '{left}' WHERE k IN (SELECT k FROM '{right}')") == []


def test_implicit_numeric_key_cast_leaves_output_columns_alone(tmp_path):
    # The CAST column is internal to the join: it is appended after the leg's real
    # columns and excluded from the payload/output layout, so the projection is
    # unchanged. A leak would show up as an extra column here.
    left = _write(
        str(tmp_path / "l"), {"k": (pa.int32(), [1, 2, 3]), "v": (pa.string(), ["a", "b", "c"])}
    )
    right = _write(str(tmp_path / "r"), {"k": (pa.float64(), [2.0, 3.0])})
    session = opteryx.session()
    names = None
    for morsel in session.execute_to_morsels(
        f"SELECT * FROM '{left}' WHERE k IN (SELECT k FROM '{right}')"
    ):
        if morsel.num_rows:
            names = list(morsel.column_names)
    assert names == [b"k", b"v"], names


def test_implicit_numeric_key_cast_inner_join(tmp_path):
    """INNER JOIN keys on the same coerced column and is no longer refused.

    `DrakenInnerJoinNode.supports` used to DECLINE a mixed-numeric key pair, which
    surfaced as `UnsupportedSyntaxError: Draken inner join does not support this
    query shape` — the loud counterpart of the semi-join's silent wrong answer.
    With the coercion in place that shape is answerable, so the decline was dropped.
    """
    left = _write(str(tmp_path / "l"), {"k": (pa.int32(), [1, 2, 3])})
    right = _write(str(tmp_path / "r"), {"k": (pa.float64(), [2.0, 3.0, 4.0])})

    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(
        f"SELECT l.k FROM '{left}' AS l INNER JOIN '{right}' AS r ON l.k = r.k"
    ):
        if morsel.num_rows:
            rows += morsel.column(morsel.column_names[0]).to_pylist()
    assert sorted(rows) == [2, 3]


@pytest.mark.parametrize("build_type", ["int8", "int16", "int32", "int64",
                                        "uint8", "uint16", "uint32", "uint64"])
@pytest.mark.parametrize("probe_type", ["int8", "int16", "int32", "int64",
                                        "uint8", "uint16", "uint32", "uint64"])
def test_join_key_integer_widths_interoperate(tmp_path, build_type, probe_type):
    """INTEGER x INTEGER join keys match across every signed/unsigned width WITHOUT
    a coercion — the native key hash canonicalises integer width.

    `_join_key_coercions` relies on exactly this to skip INTEGER x INTEGER pairs
    (it coerces on physical-type mismatch otherwise). If the native hash ever stops
    canonicalising, this goes red instead of the skip quietly becoming a silent
    wrong answer.
    """
    types = {
        "int8": pa.int8(), "int16": pa.int16(), "int32": pa.int32(), "int64": pa.int64(),
        "uint8": pa.uint8(), "uint16": pa.uint16(), "uint32": pa.uint32(), "uint64": pa.uint64(),
    }
    left = _write(str(tmp_path / "l"), {"k": (types[build_type], [1, 2, 3])})
    right = _write(str(tmp_path / "r"), {"k": (types[probe_type], [2, 3, 4])})
    assert _ids(f"SELECT k FROM '{left}' WHERE k IN (SELECT k FROM '{right}')") == [2, 3]


if __name__ == "__main__":
    import tests

    tests.run_tests()
