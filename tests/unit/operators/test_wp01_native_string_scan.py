"""WP-01 — native (zero-Python) parquet scan for string columns.

The plan-time gate (`_native_scan_plan` in compiler.py) now admits VARCHAR /
NVARCHAR / VARBINARY projections to `NativeParquetScanSource`, decoded natively
(DK_VARCHAR / DK_VARCHAR_DICT / DK_POOL-string) instead of re-entering Python
per morsel through the scan-pull trampoline.

The core correctness contract is A/B EQUALITY: for every string shape, the native
path must produce byte-identical output — values, order, nulls, AND the vector's
DrakenType tag — to the old trampoline path. Each test writes a controlled parquet
(pyarrow, for explicit encoding control — tests may use pyarrow), then runs the
same SQL twice: once native (gate on) and once with the gate forced closed
(`native_scan_supported` → False, routing to StreamingScanSource), and diffs a
digest of every value plus the column type.

Also asserts the gate FLIPS to the native Source for admitted shapes, and stays
FAIL-CLOSED (StreamingScanSource) for an out-of-scope shape (a pushed predicate).
"""

import hashlib
import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pyarrow as pa  # test-only dep (allowed in tests/)
import pyarrow.parquet as pq
import pytest

import opteryx
from opteryx.connectors.parquet_io import pool_reader


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


def _digest(sql, force_trampoline, monkeypatch):
    """Drain `sql` and return (rows, source_list, digest). The digest folds in
    every column's DrakenType tag and every value (in row/morsel order), so a
    wrong type tag or a dropped/re-ordered row changes it."""
    if force_trampoline:
        monkeypatch.setattr(pool_reader, "native_scan_supported", lambda *a, **k: False)
    session = opteryx.session()
    h = hashlib.sha256()
    rows = 0
    col_names = None
    for morsel in session.execute_to_morsels(sql):
        if col_names is None:
            col_names = list(morsel.column_names)
            for name in col_names:
                c = morsel.column(name)
                h.update(b"|type:%r" % (None if c is None else c.type))
        rows += morsel.num_rows
        for name in col_names:
            c = morsel.column(name)
            for i in range(morsel.num_rows):
                h.update(b"\x1f")
                h.update(repr(None if c is None else c[i]).encode("utf-8", "surrogatepass"))
    src = list(session.telemetry["scan_sources"].values())
    return rows, src, h.hexdigest()


def _assert_native_ab(tmp_path, monkeypatch, columns, sql_cols, **write_kw):
    """Write the columns, then assert native == trampoline (byte-identical, incl.
    type) AND the native path selected NativeParquetScanSource."""
    ds = _write_parquet(str(tmp_path / "wp01"), columns, **write_kw)
    sql = "SELECT %s FROM '%s'" % (sql_cols, ds)

    nat_rows, nat_src, nat_dig = _digest(sql, False, monkeypatch)
    monkeypatch.undo()
    tmp_rows, tmp_src, tmp_dig = _digest(sql, True, monkeypatch)

    assert nat_src == ["NativeParquetScanSource"], nat_src
    assert tmp_src == ["StreamingScanSource"], tmp_src
    assert nat_rows == tmp_rows
    assert nat_dig == tmp_dig, "native output differs from trampoline"
    return nat_rows


# --- required string shapes --------------------------------------------------

def test_single_string_dict(tmp_path, monkeypatch):
    cols = {"s": (pa.string(), ["apple", "banana", "apple", "cherry"] * 25)}
    assert _assert_native_ab(tmp_path, monkeypatch, cols, "s", use_dictionary=True) == 100


def test_single_string_plain(tmp_path, monkeypatch):
    cols = {"s": (pa.string(), ["apple", "banana", "cherry", "date"] * 25)}
    assert _assert_native_ab(tmp_path, monkeypatch, cols, "s", use_dictionary=False) == 100


def test_multi_string(tmp_path, monkeypatch):
    cols = {
        "a": (pa.string(), ["x", "yy", "zzz"] * 40),
        "b": (pa.string(), ["one", "two", "three"] * 40),
    }
    _assert_native_ab(tmp_path, monkeypatch, cols, "a, b")


def test_string_with_nulls(tmp_path, monkeypatch):
    cols = {"s": (pa.string(), (["a", None, "ccc", None, "e"] * 20))}
    _assert_native_ab(tmp_path, monkeypatch, cols, "s")


def test_empty_strings(tmp_path, monkeypatch):
    cols = {"s": (pa.string(), (["", "a", "", "bb", ""] * 20))}
    _assert_native_ab(tmp_path, monkeypatch, cols, "s")


def test_non_ascii_utf8(tmp_path, monkeypatch):
    vals = ["café", "naïve", "日本語", "Ω≈ç√", "emoji😀", "Ａ"] * 20
    cols = {"s": (pa.string(), vals)}
    _assert_native_ab(tmp_path, monkeypatch, cols, "s")


def test_oversized_german_string_slots(tmp_path, monkeypatch):
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
    _assert_native_ab(tmp_path, monkeypatch, cols, "s")


def test_all_null_string_column(tmp_path, monkeypatch):
    # An all-null VARCHAR column must decode to a FULL-LENGTH all-null vector — not a
    # zero-length one that collapses the morsel to 0 rows. Regression: the plain
    # string deserializer used the compact present-only record count (0 here) as the
    # vector length, dropping every null row. Asserting the row count (100), not just
    # A/B parity, is essential — both paths were previously wrong-but-equal (0 == 0).
    cols = {"s": (pa.string(), [None] * 100)}
    assert _assert_native_ab(tmp_path, monkeypatch, cols, "s") == 100


def test_all_null_string_with_int(tmp_path, monkeypatch):
    # All-null VARCHAR projected next to a fully-populated int column: the string
    # column must carry the int column's length (200), not collapse the morsel. This
    # is the shape the single-column all-null test could not catch (a lone 0-length
    # column just yields 0 rows on both paths; here the length mismatch is visible).
    cols = {
        "s": (pa.string(), [None] * 200),
        "n": (pa.int64(), list(range(200))),
    }
    assert _assert_native_ab(tmp_path, monkeypatch, cols, "n, s") == 200


def test_partial_null_string_plain(tmp_path, monkeypatch):
    # Nullable, NON-dictionary (plain) VARCHAR: Parquet omits null rows from the value
    # stream, so the plain records are compact (present-only). The deserializer must
    # SCATTER them to positional slots by the null bitmap. Regression: it treated the
    # compact records as positional, silently dropping the null rows (200 → 120).
    cols = {"s": (pa.string(), (["a", None, "ccc", None, "e"] * 40))}
    assert _assert_native_ab(tmp_path, monkeypatch, cols, "s", use_dictionary=False) == 200


def test_all_null_string_as_filter(tmp_path, monkeypatch):
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

    nat_rows, nat_src, _ = _digest(sql, False, monkeypatch)
    monkeypatch.undo()
    tmp_rows, tmp_src, _ = _digest(sql, True, monkeypatch)

    assert nat_rows == 0
    assert tmp_rows == 0
    assert nat_src == ["NativeParquetScanSource"], nat_src
    assert tmp_src == ["StreamingScanSource"], tmp_src


def test_all_constant_string_column(tmp_path, monkeypatch):
    cols = {"s": (pa.string(), ["constant"] * 100)}
    _assert_native_ab(tmp_path, monkeypatch, cols, "s")


def test_varbinary_column(tmp_path, monkeypatch):
    # parquet byte_array with NO string logical annotation → VARBINARY. Verifies
    # the declared type tag is carried through (not silently coerced to VARCHAR).
    cols = {"s": (pa.binary(), [b"\x00\x01", b"raw", b"\xff\xfe\xfd", b"x"] * 25)}
    _assert_native_ab(tmp_path, monkeypatch, cols, "s")


def test_zero_row_row_group(tmp_path, monkeypatch):
    # A parquet file whose single row group has zero rows. NOTE: opteryx currently
    # cannot scan such a file on EITHER Source — both the native pull and the
    # trampoline raise the same engine error (verified: the numeric native scan and
    # StreamingScanSource fail identically). That is a pre-existing, engine-wide
    # empty-file limitation, NOT a WP-01 string-path regression. This test pins the
    # PARITY: the widened gate must not make the string case diverge from the
    # (already-broken) baseline — both paths must fail the same way. If a later
    # package teaches the engine to scan an empty row group, flip this to assert
    # both return 0 rows byte-identically.
    cols = {"s": (pa.string(), [])}
    ds = _write_parquet(str(tmp_path / "empty"), cols)
    sql = "SELECT s FROM '%s'" % ds

    with pytest.raises(Exception):
        _digest(sql, False, monkeypatch)   # native path
    monkeypatch.undo()
    with pytest.raises(Exception):
        _digest(sql, True, monkeypatch)    # forced trampoline


def test_mixed_int_float_string(tmp_path, monkeypatch):
    cols = {
        "i": (pa.int64(), list(range(100))),
        "f": (pa.float64(), [x / 3.0 for x in range(100)]),
        "s": (pa.string(), ["v%d" % (x % 7) for x in range(100)]),
    }
    _assert_native_ab(tmp_path, monkeypatch, cols, "i, f, s")


# --- predicate handling: relocation (WP-02) vs fail-closed -------------------

def test_pushed_numeric_predicate_relocates_native(tmp_path, monkeypatch):
    # WP-02 SUPERSEDES the WP-01 boundary: a c-native pushed predicate no longer
    # forces the trampoline — it relocates to a native downstream ExprFilter and
    # the scan goes native. (Was asserted to stay StreamingScanSource under WP-01.)
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


def test_regex_predicate_now_native_and_still_correct(tmp_path, monkeypatch):
    # A pushed regex predicate used to fail CLOSED to the trampoline (the R4
    # `unlowerable_predicate` residual). The native regex kernels closed that
    # category, so it now goes native — and must still select exactly the same
    # rows, which is the part of the original assertion that still matters.
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
