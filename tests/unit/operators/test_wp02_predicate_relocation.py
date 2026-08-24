"""WP-02 — native predicate relocation.

A pushed `WHERE` predicate no longer forces the GIL trampoline. When the predicate
lowers to a c-native span, the scan is admitted to `NativeParquetScanSource` and
the per-row residual is relocated to a native downstream `ExprFilter` (+ a `Select`
back to the projection when the predicate reads a column that is not projected —
a "role-3" filter-only column). Row-group / bloom PRUNING stays at the scan, so
bytes-read / row-groups-scanned are unchanged. A predicate that cannot be lowered
FAILS CLOSED to `StreamingScanSource` with the predicate on the old path.

The correctness gate is A/B PARITY: the relocated-native path must produce the
same survivor SET (values + null + DrakenType tag, row-pairing intact) as the
forced-trampoline path. Comparison is ORDER-INSENSITIVE: a filtered scan has no
ORDER BY, and the native Source pulls row groups concurrently, so emission order
legitimately differs between the two paths while the survivor set is identical.

See docs/WP02_PREDICATE_RELOCATION_DESIGN.md.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pyarrow as pa  # test-only dep (allowed in tests/)
import pyarrow.parquet as pq
import pytest

import opteryx
import opteryx.config as config
from opteryx.connectors.parquet_io import pool_reader

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../../dev"))
import instrument_engine as IE  # noqa: E402


def _write(dataset_dir, columns, use_dictionary=True, row_group_size=None):
    """Write one parquet file. `columns` = {name: (pyarrow_type, py_list)}."""
    os.makedirs(dataset_dir, exist_ok=True)
    arrays = {name: pa.array(vals, type=typ) for name, (typ, vals) in columns.items()}
    kw = {"use_dictionary": use_dictionary}
    if row_group_size is not None:
        kw["row_group_size"] = row_group_size
    pq.write_table(pa.table(arrays), os.path.join(dataset_dir, "part.parquet"), **kw)
    return dataset_dir


def _drain(sql, force_trampoline, monkeypatch):
    """Drain `sql`; return (survivor_set, source_list). The survivor set is an
    ORDER-INSENSITIVE multiset of full rows, each row a tuple of (per-column
    repr) plus the column's DrakenType tag folded in — so a wrong type, a dropped
    row, or a broken column<->column row pairing all change the set. Returns a
    Counter-like sorted tuple so `==` is exact-multiset equality."""
    if force_trampoline:
        monkeypatch.setattr(pool_reader, "native_scan_supported", lambda *a, **k: False)
    session = opteryx.session()
    rows = []
    types = None
    for morsel in session.execute_to_morsels(sql):
        names = list(morsel.column_names)
        if types is None:
            types = tuple(
                (n, None if morsel.column(n) is None else morsel.column(n).type)
                for n in names
            )
        for i in range(morsel.num_rows):
            rows.append(tuple(
                repr(None if morsel.column(n) is None else morsel.column(n)[i])
                for n in names
            ))
    src = list(session.telemetry["scan_sources"].values())
    if force_trampoline:
        monkeypatch.undo()
    return (types, tuple(sorted(rows))), src


def _assert_parity(tmp_path, monkeypatch, columns, sql_tail, *, write_kw=None,
                   expect_native=True):
    """Write the columns, run `SELECT {sql_tail}` native and forced-trampoline,
    assert identical survivor set. When `expect_native`, also assert the native
    run selected NativeParquetScanSource and the trampoline run did not."""
    ds = _write(str(tmp_path / "wp02"), columns, **(write_kw or {}))
    # sql_tail is "<projection> WHERE <predicate>" (or just "<projection>"); the
    # WHERE must land AFTER the FROM clause.
    proj, _, where = sql_tail.partition(" WHERE ")
    sql = "SELECT %s FROM '%s'" % (proj, ds)
    if where:
        sql += " WHERE %s" % where

    nat, nat_src = _drain(sql, False, monkeypatch)
    tmp, tmp_src = _drain(sql, True, monkeypatch)

    assert nat == tmp, "relocated-native survivor set differs from trampoline"
    if expect_native:
        assert nat_src == ["NativeParquetScanSource"], nat_src
        assert tmp_src == ["StreamingScanSource"], tmp_src
    return nat[1]  # sorted rows


# ── a reusable string+numeric table ──────────────────────────────────────────
_LABELS = ["apple", "banana", "cherry", "date"]


def _mixed(n=400, row_group_size=None):
    # row_group_size divides n evenly in the pruning tests (n=500, rgs of 100 → 5).
    return {
        "s": (pa.string(), [_LABELS[i % 4] for i in range(n)]),
        "n": (pa.int64(), list(range(n))),
        "f": (pa.float64(), [i / 3.0 for i in range(n)]),
    }, {"row_group_size": row_group_size} if row_group_size else {}


# ── required predicate shapes (parity) ───────────────────────────────────────

def test_numeric_comparison(tmp_path, monkeypatch):
    cols, wk = _mixed()
    assert len(_assert_parity(tmp_path, monkeypatch, cols, "s, n WHERE n > 200", write_kw=wk)) == 199


def test_string_comparison(tmp_path, monkeypatch):
    cols, wk = _mixed()
    _assert_parity(tmp_path, monkeypatch, cols, "s, n WHERE s = 'banana'", write_kw=wk)


def test_in_list(tmp_path, monkeypatch):
    cols, wk = _mixed()
    _assert_parity(tmp_path, monkeypatch, cols, "n WHERE n IN (1, 5, 9, 399)", write_kw=wk)


def test_not_in_list(tmp_path, monkeypatch):
    cols, wk = _mixed()
    _assert_parity(tmp_path, monkeypatch, cols, "n WHERE n NOT IN (1, 5, 9)", write_kw=wk)


def test_like(tmp_path, monkeypatch):
    cols, wk = _mixed()
    _assert_parity(tmp_path, monkeypatch, cols, "s WHERE s LIKE 'ba%'", write_kw=wk)


def test_is_null(tmp_path, monkeypatch):
    cols = {"s": (pa.string(), ["a", None, "c", None, "e"] * 40),
            "n": (pa.int64(), list(range(200)))}
    _assert_parity(tmp_path, monkeypatch, cols, "n WHERE s IS NULL")


def test_is_not_null(tmp_path, monkeypatch):
    cols = {"s": (pa.string(), ["a", None, "c", None, "e"] * 40),
            "n": (pa.int64(), list(range(200)))}
    _assert_parity(tmp_path, monkeypatch, cols, "n WHERE s IS NOT NULL")


def test_nested_and(tmp_path, monkeypatch):
    cols, wk = _mixed()
    _assert_parity(tmp_path, monkeypatch, cols, "s, n WHERE n > 100 AND s = 'cherry'", write_kw=wk)


def test_nested_or(tmp_path, monkeypatch):
    cols, wk = _mixed()
    _assert_parity(tmp_path, monkeypatch, cols, "s, n WHERE n < 10 OR s = 'date'", write_kw=wk)


def test_cross_type_comparison(tmp_path, monkeypatch):
    # int column compared to a float literal — operand coercion must match.
    cols, wk = _mixed()
    _assert_parity(tmp_path, monkeypatch, cols, "n WHERE n > 200.5", write_kw=wk)


def test_all_null_input(tmp_path, monkeypatch):
    # predicate over an all-null column: three-valued logic keeps nothing (NULL = x
    # is NULL, not TRUE).
    cols = {"m": (pa.int64(), [None] * 200), "n": (pa.int64(), list(range(200)))}
    assert _assert_parity(tmp_path, monkeypatch, cols, "n WHERE m = 5") == ()


def test_all_null_varchar_input(tmp_path, monkeypatch):
    # Same three-valued-logic parity as test_all_null_input, but over an all-null
    # VARCHAR filter column. This is the shape that once tripped the all-null string
    # decode (native returned 0 rows / raised err_op=11 in ExprFilter); the decode is
    # now correct-length + all-null, so `s = 'x'` keeps nothing on both paths.
    cols = {"s": (pa.string(), [None] * 200), "n": (pa.int64(), list(range(200)))}
    assert _assert_parity(tmp_path, monkeypatch, cols, "n WHERE s = 'x'") == ()


def test_all_constant_input(tmp_path, monkeypatch):
    cols = {"s": (pa.string(), ["k"] * 200), "n": (pa.int64(), list(range(200)))}
    _assert_parity(tmp_path, monkeypatch, cols, "n WHERE s = 'k'")


def test_predicate_prunes_all_rows(tmp_path, monkeypatch):
    cols, wk = _mixed(row_group_size=100)
    assert _assert_parity(tmp_path, monkeypatch, cols, "s WHERE n < 0", write_kw=wk) == ()


def test_predicate_keeps_all_rows(tmp_path, monkeypatch):
    cols, wk = _mixed(row_group_size=100)
    assert len(_assert_parity(tmp_path, monkeypatch, cols, "s WHERE n >= 0", write_kw=wk)) == 400


# ── column roles ─────────────────────────────────────────────────────────────

def test_role3_filter_only_column(tmp_path, monkeypatch):
    # `n` is referenced by WHERE but NOT projected → role-3: the native scan reads
    # the read-set {s, n}, filters, and a trailing Select drops `n`.
    cols, wk = _mixed()
    rows = _assert_parity(tmp_path, monkeypatch, cols, "s WHERE n > 200", write_kw=wk)
    assert len(rows) == 199
    assert all(len(r) == 1 for r in rows)  # only `s` emitted


def test_role2_projected_and_filtered_no_select(tmp_path, monkeypatch):
    # every predicate column is projected → read-set == emit-set → no Select node
    # (the degeneracy collapse). Parity still holds.
    cols, wk = _mixed()
    _assert_parity(tmp_path, monkeypatch, cols, "n WHERE n > 200", write_kw=wk)


def test_multi_column_predicate(tmp_path, monkeypatch):
    cols, wk = _mixed()
    _assert_parity(tmp_path, monkeypatch, cols, "s, n, f WHERE n > 100 AND f < 90.0", write_kw=wk)


def test_string_predicate_composes_with_wp01(tmp_path, monkeypatch):
    # a purely-string projection + string predicate: WP-01 admits the string scan,
    # WP-02 relocates the string filter — both native, zero-Python.
    cols = {"s": (pa.string(), [_LABELS[i % 4] for i in range(200)])}
    _assert_parity(tmp_path, monkeypatch, cols, "s WHERE s = 'apple'")


def test_no_predicate_free_case(tmp_path, monkeypatch):
    # no WHERE → no ExprFilter node at all; still native (WP-01).
    cols, wk = _mixed()
    _assert_parity(tmp_path, monkeypatch, cols, "s, n", write_kw=wk)


# ── regex predicates: were fail-closed (R4), now relocated natively ──────────

@pytest.mark.parametrize("where", ["s RLIKE 'a'", "s NOT RLIKE 'a.*'"])
def test_regex_predicate_relocates_natively(tmp_path, monkeypatch, where):
    """A pushed regex predicate used to fail CLOSED — it did not lower to a c-native
    span, so the whole scan fell back (the R4 `unlowerable_predicate` residual). The
    native regex kernels closed that category: it now relocates like any other
    predicate. The parity half of the original assertion is the half that still
    matters, and matters MORE now — a relocated filter that dropped or mis-evaluated
    rows would be a silent wrong answer."""
    cols, wk = _mixed()
    ds = _write(str(tmp_path / "fc"), cols, **wk)
    sql = "SELECT s FROM '%s' WHERE %s" % (ds, where)

    nat, nat_src = _drain(sql, False, monkeypatch)   # natural path
    tmp, _ = _drain(sql, True, monkeypatch)          # forced trampoline baseline

    assert nat_src == ["NativeParquetScanSource"], nat_src   # DID go native
    assert nat == tmp                                        # and agrees with the old path
    assert nat, "predicate matched nothing — not a meaningful parity check"


# ── pruning is preserved (row groups / bytes unchanged) ──────────────────────

def _native_facts(sql):
    session = opteryx.session()
    for _ in session.execute_to_morsels(sql):
        pass
    facts = session._telemetry._reading.get("native_scan_facts") or {}
    return next(iter(facts.values()), {}), session


def test_pruning_selective_predicate(tmp_path, monkeypatch):
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


def test_pruning_prunes_all_row_groups(tmp_path, monkeypatch):
    cols, wk = _mixed(n=500, row_group_size=100)
    ds = _write(str(tmp_path / "prune0"), cols, **wk)
    facts, _ = _native_facts("SELECT s FROM '%s' WHERE n < 0" % ds)
    assert facts["row_groups_read"] == 0
    assert facts["row_groups_pruned"] == 5


def test_pruning_none_when_predicate_matches_everything(tmp_path, monkeypatch):
    cols, wk = _mixed(n=500, row_group_size=100)
    ds = _write(str(tmp_path / "prune_none"), cols, **wk)
    facts, _ = _native_facts("SELECT s FROM '%s' WHERE n >= 0" % ds)
    assert facts["row_groups_read"] == 5
    assert facts["row_groups_pruned"] == 0


def test_pruning_matches_direct_source_plan(tmp_path, monkeypatch):
    # Direct source-level parity: the native scan plan's surviving row groups equals
    # the full row-group count minus what min/max pruning excludes — computed from
    # the SAME `_rg_passes_predicates_native` the trampoline path uses.
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


# ── instrumentation: zero-Python on the relocated path ───────────────────────

def test_instrumentation_native_predicate_zero_gil(tmp_path, monkeypatch):
    """A string-column + predicate SELECT: NativeParquetScanSource, scan-stage GIL
    time ~0, no worker re-entry, and execute_bytecode unreachable (worker-purity
    guard with whitelist=() — any execution-time Python re-entry fails it)."""
    cols, wk = _mixed()
    ds = _write(str(tmp_path / "instr"), cols, **wk)
    sql = "SELECT s FROM '%s' WHERE n > 200" % ds

    monkeypatch.setattr(config, "OPTERYX_INSTRUMENT_ENGINE", True)
    session = opteryx.session()
    for _ in session.execute_to_morsels(sql):
        pass
    td = session.telemetry

    assert list(td["scan_sources"].values()) == ["NativeParquetScanSource"]
    assert td["gil_held_ns"] == 0
    assert td.get("worker_gil_sites", []) == []
    # execute_bytecode / _scan_pull_run must not have run on any worker thread.
    IE.assert_native_worker_purity(td, whitelist=())


def test_instrumentation_trampoline_calls_zero(tmp_path, monkeypatch):
    cols, wk = _mixed()
    ds = _write(str(tmp_path / "instr2"), cols, **wk)
    sql = "SELECT s, n FROM '%s' WHERE n > 200" % ds
    monkeypatch.setattr(config, "OPTERYX_INSTRUMENT_ENGINE", True)
    res = IE.measure_query_allocations(sql)
    assert res["trampoline_calls"] == 0


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
