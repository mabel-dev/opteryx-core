"""WP-01 — native (zero-Python) parquet scan for string columns.

The plan-time gate (`_native_scan_plan` in compiler.py) admits VARCHAR /
NVARCHAR / VARBINARY projections to `NativeParquetScanSource`, decoded natively
(DK_VARCHAR / DK_VARCHAR_DICT / DK_POOL-string).

The correctness contract: for every string shape, the native scan must return
exactly the values the test wrote (nulls included, row pairing intact across
columns) and tag each column with the DrakenType the schema binder declares for
its parquet type (connectors/_rugo_schema.py: UTF8 string → VARCHAR, un-annotated
byte_array → VARBINARY, INT64 → INT64, DOUBLE → FLOAT64). Each test writes a
controlled parquet (pyarrow, for explicit encoding control — tests may use
pyarrow to WRITE), and the ORACLE is the plain-Python value lists it wrote.
A scan has no ORDER BY, so rows are compared as a multiset.

Also asserts every admitted shape selects the native Source.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pyarrow as pa  # test-only dep (allowed in tests/)
import pyarrow.parquet as pq
import pytest

import opteryx
from draken.draken_native import DrakenType

#: The DrakenType the binder declares for each pyarrow type these tests write.
_EXPECTED_TYPE = {
    pa.string(): DrakenType.VARCHAR,
    pa.binary(): DrakenType.VARBINARY,
    pa.int64(): DrakenType.INT64,
    pa.float64(): DrakenType.FLOAT64,
}


def _write_parquet(dataset_dir, columns, use_dictionary=True, row_group_size=None):
    """Write one parquet file into `dataset_dir` (opteryx resolves a FROM target as
    a directory of parquet files). `columns` = {name: (pyarrow_type, py_list)}.
    Returns the directory path to use in the SQL FROM clause."""
    os.makedirs(dataset_dir, exist_ok=True)
    arrays = {name: pa.array(vals, type=typ) for name, (typ, vals) in columns.items()}
    table = pa.table(arrays)
    kw = {"use_dictionary": use_dictionary}
    if row_group_size is not None:
        kw["row_group_size"] = row_group_size
    pq.write_table(table, os.path.join(dataset_dir, "part.parquet"), **kw)
    return dataset_dir


def _multiset(rows):
    """Order-insensitive, exact-multiset form of a list of row tuples."""
    return sorted(rows, key=repr)


def _drain(sql):
    """Drain `sql`; return (row tuples, {column: DrakenType}, source list)."""
    session = opteryx.session()
    rows, types, names = [], {}, None
    for morsel in session.execute_to_morsels(sql):
        if names is None:
            names = list(morsel.column_names)
        for name in names:
            types[name.decode()] = morsel.column(name).type
        columns = [morsel.column(name).to_pylist() for name in names]
        rows.extend(zip(*columns))
    src = list(session.telemetry["scan_sources"].values())
    return rows, types, src


def _assert_native_matches_oracle(tmp_path, columns, sql_cols, **write_kw):
    """Write the columns, scan them natively, and assert the rows equal the written
    Python values (as a multiset of full rows) and every column carries the
    declared DrakenType."""
    ds = _write_parquet(str(tmp_path / "wp01"), columns, **write_kw)
    sql = "SELECT %s FROM '%s'" % (sql_cols, ds)
    names = [c.strip() for c in sql_cols.split(",")]

    rows, types, src = _drain(sql)
    expected = list(zip(*(columns[n][1] for n in names)))

    assert src == ["NativeParquetScanSource"], src
    assert len(rows) == len(expected), (len(rows), len(expected))
    assert _multiset(rows) == _multiset(expected), "native output differs from the written values"
    if rows:
        assert types == {n: _EXPECTED_TYPE[columns[n][0]] for n in names}, types
    return len(rows)


# --- required string shapes --------------------------------------------------

def test_single_string_dict(tmp_path):
    cols = {"s": (pa.string(), ["apple", "banana", "apple", "cherry"] * 25)}
    assert _assert_native_matches_oracle(tmp_path, cols, "s", use_dictionary=True) == 100


def test_single_string_plain(tmp_path):
    cols = {"s": (pa.string(), ["apple", "banana", "cherry", "date"] * 25)}
    assert _assert_native_matches_oracle(tmp_path, cols, "s", use_dictionary=False) == 100


def test_multi_string(tmp_path):
    cols = {
        "a": (pa.string(), ["x", "yy", "zzz"] * 40),
        "b": (pa.string(), ["one", "two", "three"] * 40),
    }
    _assert_native_matches_oracle(tmp_path, cols, "a, b")


def test_string_with_nulls(tmp_path):
    cols = {"s": (pa.string(), (["a", None, "ccc", None, "e"] * 20))}
    _assert_native_matches_oracle(tmp_path, cols, "s")


def test_empty_strings(tmp_path):
    cols = {"s": (pa.string(), (["", "a", "", "bb", ""] * 20))}
    _assert_native_matches_oracle(tmp_path, cols, "s")


def test_non_ascii_utf8(tmp_path):
    vals = ["café", "naïve", "日本語", "Ω≈ç√", "emoji😀", "Ａ"] * 20
    cols = {"s": (pa.string(), vals)}
    _assert_native_matches_oracle(tmp_path, cols, "s")


def test_oversized_german_string_slots(tmp_path):
    # Values > STR_INLINE_MAX (12 bytes) live in the arena (long-form slot). Mix
    # long + inline + null to exercise the arena consolidation + offset rebasing.
    vals = [
        "this is a very long string well over twelve bytes",
        "short",
        "another substantially long value exceeding the inline slot limit",
        None,
        "tiny",
    ] * 30
    cols = {"s": (pa.string(), vals)}
    _assert_native_matches_oracle(tmp_path, cols, "s")


def test_all_null_string_column(tmp_path):
    # An all-null VARCHAR column must decode to a FULL-LENGTH all-null vector — not a
    # zero-length one that collapses the morsel to 0 rows. Regression: the plain
    # string deserializer used the compact present-only record count (0 here) as the
    # vector length, dropping every null row. Asserting the row count (100) as well as
    # the values matters: a 0-length column would yield an empty, vacuous comparison.
    cols = {"s": (pa.string(), [None] * 100)}
    assert _assert_native_matches_oracle(tmp_path, cols, "s") == 100


def test_all_null_string_with_int(tmp_path):
    # All-null VARCHAR projected next to a fully-populated int column: the string
    # column must carry the int column's length (200), not collapse the morsel. This
    # is the shape the single-column all-null test could not catch (a lone 0-length
    # column just yields 0 rows on both paths; here the length mismatch is visible).
    cols = {
        "s": (pa.string(), [None] * 200),
        "n": (pa.int64(), list(range(200))),
    }
    assert _assert_native_matches_oracle(tmp_path, cols, "n, s") == 200


def test_partial_null_string_plain(tmp_path):
    # Nullable, NON-dictionary (plain) VARCHAR: Parquet omits null rows from the value
    # stream, so the plain records are compact (present-only). The deserializer must
    # SCATTER them to positional slots by the null bitmap. Regression: it treated the
    # compact records as positional, silently dropping the null rows (200 → 120).
    cols = {"s": (pa.string(), (["a", None, "ccc", None, "e"] * 40))}
    assert _assert_native_matches_oracle(tmp_path, cols, "s", use_dictionary=False) == 200


def test_all_null_string_as_filter(tmp_path):
    # An all-null VARCHAR used as a filter-only (role-3) column: `s = 'x'` is NULL
    # (never TRUE) for every row → 0 survivors. The native ExprFilter must evaluate
    # the predicate over the (now correct-length, all-null) vector cleanly rather
    # than choke on a degenerate one (was: engine err_op=11).
    cols = {
        "s": (pa.string(), [None] * 200),
        "n": (pa.int64(), list(range(200))),
    }
    ds = _write_parquet(str(tmp_path / "wp01_nullfilter"), cols)
    sql = "SELECT n FROM '%s' WHERE s = 'x'" % ds

    rows, _types, src = _drain(sql)
    # oracle: `NULL = 'x'` is NULL, never TRUE, so no written row survives.
    expected = [(n,) for s, n in zip(cols["s"][1], cols["n"][1]) if s is not None and s == "x"]

    assert expected == []
    assert rows == expected
    assert src == ["NativeParquetScanSource"], src


def test_all_constant_string_column(tmp_path):
    cols = {"s": (pa.string(), ["constant"] * 100)}
    _assert_native_matches_oracle(tmp_path, cols, "s")


def test_varbinary_column(tmp_path):
    # parquet byte_array with NO string logical annotation → VARBINARY. Verifies
    # the declared type tag is carried through (not silently coerced to VARCHAR).
    cols = {"s": (pa.binary(), [b"\x00\x01", b"raw", b"\xff\xfe\xfd", b"x"] * 25)}
    _assert_native_matches_oracle(tmp_path, cols, "s")


def test_zero_row_row_group(tmp_path):
    # A parquet file whose single row group has zero rows. This once raised on every
    # Source (an engine-wide empty-file limitation); the native scan now reads it, so
    # the contract is the correct answer: zero rows, still on the native Source.
    cols = {"s": (pa.string(), [])}
    assert _assert_native_matches_oracle(tmp_path, cols, "s") == 0


def test_mixed_int_float_string(tmp_path):
    cols = {
        "i": (pa.int64(), list(range(100))),
        "f": (pa.float64(), [x / 3.0 for x in range(100)]),
        "s": (pa.string(), ["v%d" % (x % 7) for x in range(100)]),
    }
    _assert_native_matches_oracle(tmp_path, cols, "i, f, s")


# --- predicate handling: relocation (WP-02) ---------------------------------

def test_pushed_numeric_predicate_relocates_native(tmp_path):
    # WP-02: a c-native pushed predicate relocates to a native downstream
    # ExprFilter and the scan goes native.
    cols = {
        "s": (pa.string(), ["a", "b", "c", "d"] * 25),
        "n": (pa.int64(), list(range(100))),
    }
    ds = _write_parquet(str(tmp_path / "pred"), cols)
    sql = "SELECT s FROM '%s' WHERE n > 50" % ds
    session = opteryx.session()
    rows = 0
    for m in session.execute_to_morsels(sql):
        rows += m.num_rows
    src = list(session.telemetry["scan_sources"].values())
    assert src == ["NativeParquetScanSource"], src
    assert rows == 49


def test_regex_predicate_now_native_and_still_correct(tmp_path):
    # A pushed regex predicate (once the R4 `unlowerable_predicate` residual) runs
    # on the native regex kernels and must select exactly the rows matching /a/.
    cols = {
        "s": (pa.string(), ["ax", "by", "cz", "dw"] * 25),
        "n": (pa.int64(), list(range(100))),
    }
    ds = _write_parquet(str(tmp_path / "pred_fc"), cols)
    sql = "SELECT s FROM '%s' WHERE s RLIKE 'a'" % ds
    session = opteryx.session()
    rows = 0
    for m in session.execute_to_morsels(sql):
        rows += m.num_rows
    src = list(session.telemetry["scan_sources"].values())
    assert src == ["NativeParquetScanSource"], src
    assert rows == 25  # only 'ax' matches /a/


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
