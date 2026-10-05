"""WP-02 — native predicate relocation.

A pushed `WHERE` predicate that lowers to a c-native span is admitted to
`NativeParquetScanSource`, and the per-row residual is relocated to a native
downstream `ExprFilter` (+ a `Select` back to the projection when the predicate
reads a column that is not projected — a "role-3" filter-only column). Row-group /
bloom PRUNING stays at the scan, so bytes-read / row-groups-scanned are unchanged.

The correctness gate is an INDEPENDENT plain-Python oracle: each test evaluates its
WHERE predicate in Python over the values it wrote, with SQL three-valued logic
(any comparison with NULL is NULL, and only TRUE keeps a row), and the native
survivor set must equal it — values, nulls, row pairing, and each column's
DrakenType as the binder declares it (connectors/_rugo_schema.py: UTF8 string →
VARCHAR, INT64 → INT64, DOUBLE → FLOAT64). Comparison is ORDER-INSENSITIVE: a
filtered scan has no ORDER BY and the native Source pulls row groups concurrently.

See docs/WP02_PREDICATE_RELOCATION_DESIGN.md.
"""

import os
import re
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pyarrow as pa  # test-only dep (allowed in tests/)
import pyarrow.parquet as pq
import pytest

import opteryx
from draken.draken_native import DrakenType
from opteryx.connectors.parquet_io import pool_reader

#: The DrakenType the binder declares for each pyarrow type these tests write.
_EXPECTED_TYPE = {
    pa.string(): DrakenType.VARCHAR,
    pa.int64(): DrakenType.INT64,
    pa.float64(): DrakenType.FLOAT64,
}


def _write(dataset_dir, columns, use_dictionary=True, row_group_size=None):
    """Write one parquet file. `columns` = {name: (pyarrow_type, py_list)}."""
    os.makedirs(dataset_dir, exist_ok=True)
    arrays = {name: pa.array(vals, type=typ) for name, (typ, vals) in columns.items()}
    kw = {"use_dictionary": use_dictionary}
    if row_group_size is not None:
        kw["row_group_size"] = row_group_size
    pq.write_table(pa.table(arrays), os.path.join(dataset_dir, "part.parquet"), **kw)
    return dataset_dir


# ── SQL three-valued logic, in plain Python ──────────────────────────────────
# Each returns True / False / None (NULL). A row survives WHERE only on True.

def _cmp(a, b, op):
    if a is None or b is None:
        return None
    return op(a, b)


def _and(*xs):
    if any(x is False for x in xs):
        return False
    if any(x is None for x in xs):
        return None
    return True


def _or(*xs):
    if any(x is True for x in xs):
        return True
    if any(x is None for x in xs):
        return None
    return False


def _not(x):
    return None if x is None else not x


def _in(a, items):
    # no NULL in any IN list used here, so a non-NULL operand is plainly in/out.
    return None if a is None else a in items


def _multiset(rows):
    return tuple(sorted(rows, key=repr))


def _drain(sql):
    """Drain `sql`; return (types, survivor multiset, source list). Each row is a
    tuple of Python values, so a dropped row or a broken column<->column pairing
    changes the multiset."""
    session = opteryx.session()
    rows, types = [], {}
    for morsel in session.execute_to_morsels(sql):
        names = list(morsel.column_names)
        for n in names:
            types[n.decode()] = morsel.column(n).type
        rows.extend(zip(*(morsel.column(n).to_pylist() for n in names)))
    src = list(session.telemetry["scan_sources"].values())
    return types, _multiset(rows), src


def _assert_oracle(tmp_path, columns, sql_tail, predicate=None, *, write_kw=None):
    """Write the columns, run `SELECT {sql_tail}` natively, and assert the survivor
    set equals the Python oracle: the projected values of every written row for
    which `predicate(row)` is TRUE (`row` = {column: value}; None = no WHERE).
    Also asserts the scan selected NativeParquetScanSource. Returns the rows."""
    ds = _write(str(tmp_path / "wp02"), columns, **(write_kw or {}))
    # sql_tail is "<projection> WHERE <predicate>" (or just "<projection>"); the
    # WHERE must land AFTER the FROM clause.
    proj, _, where = sql_tail.partition(" WHERE ")
    assert (predicate is None) == (not where), "oracle predicate must match the SQL"
    sql = "SELECT %s FROM '%s'" % (proj, ds)
    if where:
        sql += " WHERE %s" % where
    names = [c.strip() for c in proj.split(",")]

    n_rows = len(next(iter(columns.values()))[1])
    expected = []
    for i in range(n_rows):
        row = {name: vals[i] for name, (_t, vals) in columns.items()}
        if predicate is None or predicate(row) is True:
            expected.append(tuple(row[n] for n in names))

    types, rows, src = _drain(sql)

    assert src == ["NativeParquetScanSource"], src
    assert rows == _multiset(expected), "native survivor set differs from the Python oracle"
    if rows:
        assert types == {n: _EXPECTED_TYPE[columns[n][0]] for n in names}, types
    return rows


# ── a reusable string+numeric table ──────────────────────────────────────────
_LABELS = ["apple", "banana", "cherry", "date"]


def _mixed(n=400, row_group_size=None):
    # row_group_size divides n evenly in the pruning tests (n=500, rgs of 100 → 5).
    return {
        "s": (pa.string(), [_LABELS[i % 4] for i in range(n)]),
        "n": (pa.int64(), list(range(n))),
        "f": (pa.float64(), [i / 3.0 for i in range(n)]),
    }, {"row_group_size": row_group_size} if row_group_size else {}


# ── required predicate shapes ────────────────────────────────────────────────

def test_numeric_comparison(tmp_path):
    cols, wk = _mixed()
    rows = _assert_oracle(tmp_path, cols, "s, n WHERE n > 200",
                          lambda r: _cmp(r["n"], 200, lambda a, b: a > b), write_kw=wk)
    assert len(rows) == 199


def test_string_comparison(tmp_path):
    cols, wk = _mixed()
    rows = _assert_oracle(tmp_path, cols, "s, n WHERE s = 'banana'",
                          lambda r: _cmp(r["s"], "banana", lambda a, b: a == b), write_kw=wk)
    assert len(rows) == 100


def test_in_list(tmp_path):
    cols, wk = _mixed()
    rows = _assert_oracle(tmp_path, cols, "n WHERE n IN (1, 5, 9, 399)",
                          lambda r: _in(r["n"], (1, 5, 9, 399)), write_kw=wk)
    assert len(rows) == 4


def test_not_in_list(tmp_path):
    cols, wk = _mixed()
    rows = _assert_oracle(tmp_path, cols, "n WHERE n NOT IN (1, 5, 9)",
                          lambda r: _not(_in(r["n"], (1, 5, 9))), write_kw=wk)
    assert len(rows) == 397


def test_like(tmp_path):
    cols, wk = _mixed()
    rows = _assert_oracle(tmp_path, cols, "s WHERE s LIKE 'ba%'",
                          lambda r: None if r["s"] is None else r["s"].startswith("ba"),
                          write_kw=wk)
    assert len(rows) == 100


def test_is_null(tmp_path):
    cols = {"s": (pa.string(), ["a", None, "c", None, "e"] * 40),
            "n": (pa.int64(), list(range(200)))}
    rows = _assert_oracle(tmp_path, cols, "n WHERE s IS NULL", lambda r: r["s"] is None)
    assert len(rows) == 80


def test_is_not_null(tmp_path):
    cols = {"s": (pa.string(), ["a", None, "c", None, "e"] * 40),
            "n": (pa.int64(), list(range(200)))}
    rows = _assert_oracle(tmp_path, cols, "n WHERE s IS NOT NULL", lambda r: r["s"] is not None)
    assert len(rows) == 120


def test_nested_and(tmp_path):
    cols, wk = _mixed()
    _assert_oracle(tmp_path, cols, "s, n WHERE n > 100 AND s = 'cherry'",
                   lambda r: _and(_cmp(r["n"], 100, lambda a, b: a > b),
                                  _cmp(r["s"], "cherry", lambda a, b: a == b)),
                   write_kw=wk)


def test_nested_or(tmp_path):
    cols, wk = _mixed()
    _assert_oracle(tmp_path, cols, "s, n WHERE n < 10 OR s = 'date'",
                   lambda r: _or(_cmp(r["n"], 10, lambda a, b: a < b),
                                 _cmp(r["s"], "date", lambda a, b: a == b)),
                   write_kw=wk)


def test_cross_type_comparison(tmp_path):
    # int column compared to a float literal — operand coercion must be numeric.
    cols, wk = _mixed()
    rows = _assert_oracle(tmp_path, cols, "n WHERE n > 200.5",
                          lambda r: _cmp(r["n"], 200.5, lambda a, b: a > b), write_kw=wk)
    assert len(rows) == 199


def test_all_null_input(tmp_path):
    # predicate over an all-null column: three-valued logic keeps nothing (NULL = x
    # is NULL, not TRUE).
    cols = {"m": (pa.int64(), [None] * 200), "n": (pa.int64(), list(range(200)))}
    assert _assert_oracle(tmp_path, cols, "n WHERE m = 5",
                          lambda r: _cmp(r["m"], 5, lambda a, b: a == b)) == ()


def test_all_null_varchar_input(tmp_path):
    # Same three-valued logic as test_all_null_input, over an all-null VARCHAR filter
    # column. This is the shape that once tripped the all-null string decode (native
    # returned 0 rows / raised err_op=11 in ExprFilter); `s = 'x'` must keep nothing.
    cols = {"s": (pa.string(), [None] * 200), "n": (pa.int64(), list(range(200)))}
    assert _assert_oracle(tmp_path, cols, "n WHERE s = 'x'",
                          lambda r: _cmp(r["s"], "x", lambda a, b: a == b)) == ()


def test_all_constant_input(tmp_path):
    cols = {"s": (pa.string(), ["k"] * 200), "n": (pa.int64(), list(range(200)))}
    rows = _assert_oracle(tmp_path, cols, "n WHERE s = 'k'",
                          lambda r: _cmp(r["s"], "k", lambda a, b: a == b))
    assert len(rows) == 200


def test_predicate_prunes_all_rows(tmp_path):
    cols, wk = _mixed(row_group_size=100)
    assert _assert_oracle(tmp_path, cols, "s WHERE n < 0",
                          lambda r: _cmp(r["n"], 0, lambda a, b: a < b), write_kw=wk) == ()


def test_predicate_keeps_all_rows(tmp_path):
    cols, wk = _mixed(row_group_size=100)
    rows = _assert_oracle(tmp_path, cols, "s WHERE n >= 0",
                          lambda r: _cmp(r["n"], 0, lambda a, b: a >= b), write_kw=wk)
    assert len(rows) == 400


# ── column roles ─────────────────────────────────────────────────────────────

def test_role3_filter_only_column(tmp_path):
    # `n` is referenced by WHERE but NOT projected → role-3: the native scan reads
    # the read-set {s, n}, filters, and a trailing Select drops `n`.
    cols, wk = _mixed()
    rows = _assert_oracle(tmp_path, cols, "s WHERE n > 200",
                          lambda r: _cmp(r["n"], 200, lambda a, b: a > b), write_kw=wk)
    assert len(rows) == 199
    assert all(len(r) == 1 for r in rows)  # only `s` emitted


def test_role2_projected_and_filtered_no_select(tmp_path):
    # every predicate column is projected → read-set == emit-set → no Select node
    # (the degeneracy collapse).
    cols, wk = _mixed()
    _assert_oracle(tmp_path, cols, "n WHERE n > 200",
                   lambda r: _cmp(r["n"], 200, lambda a, b: a > b), write_kw=wk)


def test_multi_column_predicate(tmp_path):
    cols, wk = _mixed()
    rows = _assert_oracle(tmp_path, cols, "s, n, f WHERE n > 100 AND f < 90.0",
                          lambda r: _and(_cmp(r["n"], 100, lambda a, b: a > b),
                                         _cmp(r["f"], 90.0, lambda a, b: a < b)),
                          write_kw=wk)
    assert rows, "predicate matched nothing — not a meaningful check"


def test_string_predicate_composes_with_wp01(tmp_path):
    # a purely-string projection + string predicate: WP-01 admits the string scan,
    # WP-02 relocates the string filter — both native, zero-Python.
    cols = {"s": (pa.string(), [_LABELS[i % 4] for i in range(200)])}
    rows = _assert_oracle(tmp_path, cols, "s WHERE s = 'apple'",
                          lambda r: _cmp(r["s"], "apple", lambda a, b: a == b))
    assert len(rows) == 50


def test_no_predicate_free_case(tmp_path):
    # no WHERE → no ExprFilter node at all; still native (WP-01).
    cols, wk = _mixed()
    assert len(_assert_oracle(tmp_path, cols, "s, n", write_kw=wk)) == 400


# ── regex predicates: were fail-closed (R4), now relocated natively ──────────

@pytest.mark.parametrize("where, predicate", [
    ("s RLIKE 'a'", lambda r: None if r["s"] is None else re.search("a", r["s"]) is not None),
    ("s NOT RLIKE 'a.*'",
     lambda r: None if r["s"] is None else re.search("a.*", r["s"]) is None),
])
def test_regex_predicate_relocates_natively(tmp_path, where, predicate):
    """A pushed regex predicate (once the R4 `unlowerable_predicate` residual) now
    relocates like any other predicate. A relocated filter that dropped or
    mis-evaluated rows would be a silent wrong answer, so the survivors are checked
    against Python's `re` over the written values."""
    cols, wk = _mixed()
    rows = _assert_oracle(tmp_path, cols, "s WHERE %s" % where, predicate, write_kw=wk)
    assert rows, "predicate matched nothing — not a meaningful check"


# ── pruning is preserved (row groups / bytes unchanged) ──────────────────────

def _native_facts(sql):
    session = opteryx.session()
    for _ in session.execute_to_morsels(sql):
        pass
    facts = session._telemetry._reading.get("native_scan_facts") or {}
    return next(iter(facts.values()), {}), session


def test_pruning_selective_predicate(tmp_path):
    # 5 row groups of 100. `n IN (1,2,3, 401,402)` lives only in rg0 and rg4 →
    # 3 row groups pruned, 2 read; the pruned+read invariant must hold.
    cols, wk = _mixed(n=500, row_group_size=100)
    ds = _write(str(tmp_path / "prune"), cols, **wk)
    sql = "SELECT s FROM '%s' WHERE n IN (1, 2, 3, 401, 402)" % ds
    facts, session = _native_facts(sql)
    assert list(session.telemetry["scan_sources"].values()) == ["NativeParquetScanSource"]
    assert facts["row_groups_read"] == 2
    assert facts["row_groups_pruned"] == 3
    assert facts["row_groups_read"] + facts["row_groups_pruned"] == 5


def test_pruning_prunes_all_row_groups(tmp_path):
    cols, wk = _mixed(n=500, row_group_size=100)
    ds = _write(str(tmp_path / "prune0"), cols, **wk)
    facts, _ = _native_facts("SELECT s FROM '%s' WHERE n < 0" % ds)
    assert facts["row_groups_read"] == 0
    assert facts["row_groups_pruned"] == 5


def test_pruning_none_when_predicate_matches_everything(tmp_path):
    cols, wk = _mixed(n=500, row_group_size=100)
    ds = _write(str(tmp_path / "prune_none"), cols, **wk)
    facts, _ = _native_facts("SELECT s FROM '%s' WHERE n >= 0" % ds)
    assert facts["row_groups_read"] == 5
    assert facts["row_groups_pruned"] == 0


def test_pruning_matches_direct_source_plan(tmp_path):
    # Direct source-level parity: the native scan plan's surviving row groups equals
    # the full row-group count minus what min/max pruning excludes.
    cols, wk = _mixed(n=500, row_group_size=100)
    ds = _write(str(tmp_path / "prune_src"), cols, **wk)
    path = os.path.join(ds, "part.parquet")
    pruned = pool_reader.open_native_scan_plan(path and [path], ["s", "n"],
                                               predicates=[("n", "Gt", 250)])
    full = pool_reader.open_native_scan_plan([path], ["s", "n"], predicates=None)
    try:
        assert full.row_group_count == 5
        assert pruned.row_group_count + pruned.pruned_row_group_count == full.row_group_count
        assert pruned.pruned_row_group_count > 0  # pruning actually happened
    finally:
        pruned.close()
        full.close()


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
