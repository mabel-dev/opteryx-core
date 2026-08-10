"""A3 (R3) — admit the scan-fused TopN shape to the native parquet scan.

Background: `ORDER BY <col> LIMIT n` reading directly from a parquet Scan gets a
`topn_sort_name`/`topn_limit`/`topn_descending` spec stamped onto the scan by
`TopNScanPushdownStrategy` (opteryx/planner/optimizer/strategies/
topn_scan_pushdown.py). That spec is a TRAMPOLINE-ONLY decode-skip hint consumed
by `_apply_topn` in `parquet_read.pyx` — it only ever activates when a WHERE
predicate is ALSO pushed (two-pass late-materialization eligibility); the
no-predicate case never runs it even on the trampoline today. The actual
sort/limit/tie-break/null-order is always performed by the native `HeapSortNode`
-> `set_topn_sink` operator downstream of the scan (compiler.py's
`_compile_scan`), generically over the incoming layout, independent of which
scan Source feeds it.

A3 close-out: `_native_scan_plan` (opteryx/managers/execution/compiler.py) no
longer bails when `scan._topn_sort_name` is set AND there is no predicate. The
native scan simply ignores the hint and decodes its normal read-set; the
pre-existing native TopN sink does the real cut, so the result is
byte-identical (SET AND ORDER) to the forced-trampoline path. No new native
TopN/heap-select kernel was needed for this sub-case.

UPDATE (R3 close-out) — the composed shape (fused TopN WITH a predicate, e.g.
ClickBench Q24: `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime
LIMIT 10`) is ALSO native now, but by a different route. Admitting it as a plain
single-pass scan was tried and reverted: the two-pass late-mat decodes only the
predicate + sort-key columns for the whole table, then the rest of a wide
SELECT * only for the tiny surviving set, and losing that measured ~400% slower
on Q24. `LatmatScanSource` (src/cpp/engine/native_latmat_scan_source.hpp) now
performs both passes natively and KEEPS the skip — see
`test_topn_with_where_predicate_now_native` below, and
tests/unit/operators/test_wp_r3_latmat_scan.py for that Source's own correctness
matrix. This file remains the NO-predicate sub-case's harness.

Correctness gate (no-predicate sub-case): A/B parity, ORDER-SENSITIVE (this is
exactly what ORDER BY + LIMIT means) — the native run and the forced-
trampoline run must produce the identical row sequence, including
tie-breaking and null-ordering.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../..", "dev"))

import pyarrow as pa  # test-only dep (allowed in tests/)
import pyarrow.parquet as pq
import pytest

import opteryx
import opteryx.config as config
from opteryx.connectors.parquet_io import pool_reader

import instrument_engine as IE  # dev/instrument_engine.py


def _write(dataset_dir, columns, use_dictionary=True, row_group_size=None):
    """Write one parquet file. `columns` = {name: (pyarrow_type, py_list)}."""
    os.makedirs(dataset_dir, exist_ok=True)
    arrays = {name: pa.array(vals, type=typ) for name, (typ, vals) in columns.items()}
    kw = {"use_dictionary": use_dictionary}
    if row_group_size is not None:
        kw["row_group_size"] = row_group_size
    pq.write_table(pa.table(arrays), os.path.join(dataset_dir, "part.parquet"), **kw)
    return dataset_dir


def _drain_ordered(sql, force_trampoline, monkeypatch):
    """Drain `sql`; return (ordered_rows, source_list). `ordered_rows` is a LIST
    (not sorted) of per-row tuples, preserving emission order — ORDER BY + LIMIT
    output order is exactly what this harness verifies."""
    if force_trampoline:
        monkeypatch.setattr(pool_reader, "native_scan_supported", lambda *a, **k: False)
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        names = list(morsel.column_names)
        for i in range(morsel.num_rows):
            rows.append(tuple(
                repr(None if morsel.column(n) is None else morsel.column(n)[i])
                for n in names
            ))
    src = list(session.telemetry["scan_sources"].values())
    if force_trampoline:
        monkeypatch.undo()
    return rows, src


def _assert_topn_parity(tmp_path, name, columns, sql_tail, monkeypatch, *,
                         write_kw=None, expect_native=True):
    """Write `columns`, run `SELECT {sql_tail}` native and forced-trampoline,
    assert IDENTICAL row order. When `expect_native`, also assert the native run
    selected NativeParquetScanSource and the trampoline run did not."""
    ds = _write(str(tmp_path / name), columns, **(write_kw or {}))
    proj, _, order_tail = sql_tail.partition(" ORDER BY ")
    sql = "SELECT %s FROM '%s' ORDER BY %s" % (proj, ds, order_tail)

    nat, nat_src = _drain_ordered(sql, False, monkeypatch)
    tmp, tmp_src = _drain_ordered(sql, True, monkeypatch)

    assert nat == tmp, "native TopN row order differs from forced-trampoline"
    if expect_native:
        assert nat_src == ["NativeParquetScanSource"], nat_src
        assert tmp_src == ["StreamingScanSource"], tmp_src
    return nat


# ── a reusable table ─────────────────────────────────────────────────────────

def _table(n, row_group_size=None):
    # sort_key is unique per row (no ties) so strict ORDER-SENSITIVE parity is a
    # valid check — dedicated tests below cover ties/NULLs separately, where the
    # engine's own contract (native_sort.hpp) makes boundary order unspecified.
    return {
        "s": (pa.string(), ["row-%04d" % i for i in range(n)]),
        "sort_key": (pa.int64(), [(i * 7919) % 1000003 for i in range(n)]),
        "flag": (pa.int64(), [i % 3 for i in range(n)]),
    }, ({"row_group_size": row_group_size} if row_group_size else {})


def test_topn_ascending_single_row_group(tmp_path, monkeypatch):
    cols, kw = _table(200)
    _assert_topn_parity(
        tmp_path, "asc_single", cols,
        "s, sort_key ORDER BY sort_key ASC LIMIT 10", monkeypatch, write_kw=kw)


def test_topn_descending_single_row_group(tmp_path, monkeypatch):
    cols, kw = _table(200)
    _assert_topn_parity(
        tmp_path, "desc_single", cols,
        "s, sort_key ORDER BY sort_key DESC LIMIT 10", monkeypatch, write_kw=kw)


def test_topn_n_less_than_row_group(tmp_path, monkeypatch):
    # 500 rows, one row group (default) — N << row group size.
    cols, kw = _table(500)
    _assert_topn_parity(
        tmp_path, "n_lt_rg", cols,
        "s, sort_key ORDER BY sort_key ASC LIMIT 5", monkeypatch, write_kw=kw)


def test_topn_n_greater_than_row_group_spans_multiple(tmp_path, monkeypatch):
    # 2000 rows, row_group_size=100 -> 20 row groups; N spans several of them.
    cols, kw = _table(2000, row_group_size=100)
    _assert_topn_parity(
        tmp_path, "n_gt_rg", cols,
        "s, sort_key ORDER BY sort_key ASC LIMIT 250", monkeypatch, write_kw=kw)


def test_topn_ties_on_sort_key(tmp_path, monkeypatch):
    # A constant sort key forces every row into a tie at the boundary. Boundary
    # order is unspecified by the engine's own contract (native_sort.hpp), so
    # the correctness bar is the survivor SET: with every row tied, any 20 of
    # the 300 rows are a valid top-20 — so what must hold is just "20 rows,
    # all with sort_key==42", not a specific 20.
    n = 300
    cols = {
        "s": (pa.string(), ["row-%04d" % i for i in range(n)]),
        "sort_key": (pa.int64(), [42] * n),
    }
    ds = _write(str(tmp_path / "ties"), cols, row_group_size=50)
    sql = "SELECT s, sort_key FROM '%s' ORDER BY sort_key ASC LIMIT 20" % ds
    nat, nat_src = _drain_ordered(sql, False, monkeypatch)
    tmp, tmp_src = _drain_ordered(sql, True, monkeypatch)
    assert len(nat) == 20 and len(tmp) == 20
    assert all(row[1] == "42" for row in nat)
    assert all(row[1] == "42" for row in tmp)
    assert nat_src == ["NativeParquetScanSource"], nat_src
    assert tmp_src == ["StreamingScanSource"], tmp_src


def test_topn_nulls_in_sort_column(tmp_path, monkeypatch):
    # Verified from src/cpp/engine/native_sort.hpp's comparator: a NULL key
    # compares as the MINIMUM (sorts FIRST for ASC, LAST for DESC) — the same
    # rule in both the native TopN sink and (transitively, since the trampoline
    # feeds the identical downstream HeapSort) the forced-trampoline path. 75 of
    # 300 rows are NULL here — more than the LIMIT — so:
    #   ASC  LIMIT 15 -> all 15 results are NULL (a 75-way tie; WHICH 15 of the
    #                    75 survive is unspecified — compare count/value only).
    #   DESC LIMIT 15 -> NULLs sort last, so the 15 largest non-null values win;
    #                    non-null values are made unique below, so this SET is
    #                    fully determined and checked exactly.
    n = 300
    non_null = list(range(n))  # unique per row where present -> no value-ties
    cols = {
        "s": (pa.string(), ["row-%04d" % i for i in range(n)]),
        "sort_key": (pa.int64(), [None if i % 4 == 0 else non_null[i] for i in range(n)]),
    }
    ds = _write(str(tmp_path / "nulls"), cols, row_group_size=60)

    sql_asc = "SELECT s, sort_key FROM '%s' ORDER BY sort_key ASC LIMIT 15" % ds
    nat, nat_src = _drain_ordered(sql_asc, False, monkeypatch)
    tmp, tmp_src = _drain_ordered(sql_asc, True, monkeypatch)
    assert len(nat) == 15 and len(tmp) == 15
    assert all(row[1] == "None" for row in nat), nat
    assert all(row[1] == "None" for row in tmp), tmp
    assert nat_src == ["NativeParquetScanSource"], nat_src
    assert tmp_src == ["StreamingScanSource"], tmp_src

    expect_desc = sorted((v for i, v in enumerate(non_null) if i % 4 != 0), reverse=True)[:15]
    sql_desc = "SELECT s, sort_key FROM '%s' ORDER BY sort_key DESC LIMIT 15" % ds
    nat, nat_src = _drain_ordered(sql_desc, False, monkeypatch)
    tmp, tmp_src = _drain_ordered(sql_desc, True, monkeypatch)
    assert nat == tmp
    assert [int(row[1]) for row in nat] == expect_desc
    assert nat_src == ["NativeParquetScanSource"], nat_src
    assert tmp_src == ["StreamingScanSource"], tmp_src


def test_topn_with_where_predicate_now_native(tmp_path, monkeypatch):
    """R3: the composed shape (fused TopN WITH a predicate) no longer falls back to
    the trampoline. It used to, because admitting it as a plain single-pass native
    scan lost the two-pass late-materialization decode-skip and measured ~400%
    slower on ClickBench Q24. It is now served by `LatmatScanSource`, which does
    both passes natively and keeps the skip — see
    tests/unit/operators/test_wp_r3_latmat_scan.py for that Source's own
    correctness matrix (ties, NULLs, row-group-spanning tie blocks, alignment).

    Kept here in its original A3 form because the ordering assertion is the point:
    the native and forced-trampoline runs must still agree on the exact row
    SEQUENCE for this fixture. `sort_key` is distinct here, so there are no ties to
    make the order legitimately ambiguous.

    NOTE the SQL shape matters for actually EXERCISING the fused path:
    `TopNScanPushdownStrategy` only stamps the scan when HeapSort reads
    directly from the Scan with no intervening Project — which requires the
    predicate column to ALSO be part of the projection (as `SELECT *` does
    here, mirroring Q24). A predicate on a column NOT in the SELECT list
    forces a Project between Scan and HeapSort (to drop that role-3 column)
    and the fusion never stamps the scan at all."""
    cols, kw = _table(1000, row_group_size=100)
    ds = _write(str(tmp_path / "with_predicate"), cols, **kw)
    sql = "SELECT * FROM '%s' WHERE flag = 1 ORDER BY sort_key ASC LIMIT 20" % ds
    nat, nat_src = _drain_ordered(sql, False, monkeypatch)
    tmp, tmp_src = _drain_ordered(sql, True, monkeypatch)
    assert nat == tmp
    assert nat_src == ["LatmatScanSource"], nat_src
    assert tmp_src == ["StreamingScanSource"], tmp_src


def test_topn_large_n_edge(tmp_path, monkeypatch):
    # LIMIT exceeds the total row count -> every row is returned, in sort order.
    cols, kw = _table(50)
    _assert_topn_parity(
        tmp_path, "large_n", cols,
        "s, sort_key ORDER BY sort_key ASC LIMIT 1000", monkeypatch, write_kw=kw)


def test_instrumentation_native_topn_zero_gil(tmp_path, monkeypatch):
    """A fused-TopN SELECT: NativeParquetScanSource, scan-stage GIL time ~0,
    no worker re-entry, and execute_bytecode/_scan_pull_run unreachable
    (worker-purity guard, whitelist=())."""
    cols, kw = _table(500, row_group_size=100)
    ds = _write(str(tmp_path / "instr"), cols, **kw)
    sql = "SELECT s, sort_key FROM '%s' ORDER BY sort_key ASC LIMIT 10" % ds

    monkeypatch.setattr(config, "OPTERYX_INSTRUMENT_ENGINE", True)
    session = opteryx.session()
    for _ in session.execute_to_morsels(sql):
        pass
    td = session.telemetry

    assert list(td["scan_sources"].values()) == ["NativeParquetScanSource"]
    assert td["gil_held_ns"] == 0
    assert td.get("worker_gil_sites", []) == []
    IE.assert_native_worker_purity(td, whitelist=())


def test_instrumentation_trampoline_calls_zero(tmp_path, monkeypatch):
    cols, kw = _table(500, row_group_size=100)
    ds = _write(str(tmp_path / "instr2"), cols, **kw)
    sql = "SELECT s, sort_key FROM '%s' ORDER BY sort_key ASC LIMIT 10" % ds
    monkeypatch.setattr(config, "OPTERYX_INSTRUMENT_ENGINE", True)
    res = IE.measure_query_allocations(sql)
    assert res["trampoline_calls"] == 0


def test_census_reports_no_fused_topn_residual():
    """R3 close-out: the census tally over the clickbench + tpch battery no longer
    reports ANY `fused_topn` residual. ClickBench Q24 (fused TopN WITH a predicate)
    was the single trigger and now runs on `LatmatScanSource`. It was also the last
    reachable residual of any kind in this battery, so the trampoline count is 0."""
    import native_residual_census as census  # dev/native_residual_census.py

    tally = census.census()
    assert tally.get("fused_topn") is None, tally
    assert tally["__trampoline__"] == 0, tally


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
