"""A3 (R3) — the scan-fused TopN shape on the native parquet scan.

`ORDER BY <col> LIMIT n` reading directly from a parquet Scan gets a
`topn_sort_name`/`topn_limit`/`topn_descending` spec stamped onto the scan by
`TopNScanPushdownStrategy` (opteryx/planner/optimizer/strategies/
topn_scan_pushdown.py). With NO predicate, `_native_scan_plan`
(opteryx/managers/execution/compiler.py) ignores the hint: NativeParquetScanSource
decodes its normal read-set and the native `HeapSortNode` -> `set_topn_sink`
operator downstream of the scan performs the sort/limit/tie-break/null-order.

The composed shape (fused TopN WITH a predicate, e.g. ClickBench Q24) is served by
`LatmatScanSource` (src/cpp/engine/native_latmat_scan_source.hpp), which runs the
two-pass late-materialization natively — see `test_topn_with_where_predicate_now_native`
below, and tests/unit/operators/test_wp_r3_latmat_scan.py for that Source's own
correctness matrix. This file is the NO-predicate sub-case's harness.

Correctness gate: the expected answer is computed in plain Python from the values
the test wrote (`_oracle`), ORDER-SENSITIVE — ORDER BY + LIMIT output order is
exactly what is verified. The engine's NULL rule (ruling 2026-09-27, resolved in
`sort_nulls_first`): NULL is the lowest value, so ASC -> NULLS FIRST and DESC ->
NULLS LAST by default. Where the sort key ties at the LIMIT boundary, which tied
rows come back is unspecified (native_sort.hpp), so those tests check the key
sequence and that every row is a real row of the table rather than one specific
row set.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../..", "dev"))

import pyarrow as pa  # test-only dep, used to WRITE parquet only
import pyarrow.parquet as pq
import pytest

import opteryx


def _write(dataset_dir, columns, use_dictionary=True, row_group_size=None):
    """Write one parquet file. `columns` = {name: (pyarrow_type, py_list)}."""
    os.makedirs(dataset_dir, exist_ok=True)
    arrays = {name: pa.array(vals, type=typ) for name, (typ, vals) in columns.items()}
    kw = {"use_dictionary": use_dictionary}
    if row_group_size is not None:
        kw["row_group_size"] = row_group_size
    pq.write_table(pa.table(arrays), os.path.join(dataset_dir, "part.parquet"), **kw)
    return dataset_dir


def _drain_ordered(sql):
    """Drain `sql`; return (ordered_rows, source_list).
    `ordered_rows` is a LIST of per-row tuples of Python values in emission order."""
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        cols = [morsel.column(n) for n in morsel.column_names]
        for i in range(morsel.num_rows):
            rows.append(tuple(c[i] for c in cols))
    telemetry = session.telemetry
    src = list(telemetry["scan_sources"].values())
    return rows, src


def _oracle(columns, projection, sort_key, descending, limit, where=None):
    """Plain-Python WHERE / ORDER BY / LIMIT over the written values, returning the
    projected rows in order. NULL lowest (engine default placement). Only valid for
    a sort key without ties among non-NULL values — the callers guarantee it."""
    n_rows = len(columns[sort_key][1])
    rows = []
    for i in range(n_rows):
        row = {name: vals[i] for name, (_typ, vals) in columns.items()}
        if where is None or where(row):
            rows.append(row)
    nulls = [r for r in rows if r[sort_key] is None]
    non_null = sorted((r for r in rows if r[sort_key] is not None),
                      key=lambda r: r[sort_key], reverse=descending)
    ordered = non_null + nulls if descending else nulls + non_null
    return [tuple(r[c] for c in projection) for r in ordered[:limit]]


def _assert_topn_matches_oracle(tmp_path, name, columns, sort_key, descending, limit,
                                *, write_kw=None):
    """Write `columns`, run `SELECT s, <sort_key> ... ORDER BY ... LIMIT n` natively
    and assert the IDENTICAL row sequence to the plain-Python oracle, on
    NativeParquetScanSource."""
    ds = _write(str(tmp_path / name), columns, **(write_kw or {}))
    sql = "SELECT s, %s FROM '%s' ORDER BY %s %s LIMIT %d" % (
        sort_key, ds, sort_key, "DESC" if descending else "ASC", limit)
    nat, nat_src = _drain_ordered(sql)
    expect = _oracle(columns, ("s", sort_key), sort_key, descending, limit)
    assert nat == expect, ("native TopN row sequence differs from the oracle", nat, expect)
    assert nat_src == ["NativeParquetScanSource"], nat_src
    return nat


# ── a reusable table ─────────────────────────────────────────────────────────

def _table(n, row_group_size=None):
    # sort_key is unique per row (no ties) so strict ORDER-SENSITIVE comparison is
    # valid — dedicated tests below cover ties/NULLs separately, where the engine's
    # own contract (native_sort.hpp) makes boundary order unspecified.
    return {
        "s": (pa.string(), ["row-%04d" % i for i in range(n)]),
        "sort_key": (pa.int64(), [(i * 7919) % 1000003 for i in range(n)]),
        "flag": (pa.int64(), [i % 3 for i in range(n)]),
    }, ({"row_group_size": row_group_size} if row_group_size else {})


def test_topn_ascending_single_row_group(tmp_path):
    cols, kw = _table(200)
    _assert_topn_matches_oracle(tmp_path, "asc_single", cols, "sort_key", False, 10,
                                write_kw=kw)


def test_topn_descending_single_row_group(tmp_path):
    cols, kw = _table(200)
    _assert_topn_matches_oracle(tmp_path, "desc_single", cols, "sort_key", True, 10,
                                write_kw=kw)


def test_topn_n_less_than_row_group(tmp_path):
    # 500 rows, one row group (default) — N << row group size.
    cols, kw = _table(500)
    _assert_topn_matches_oracle(tmp_path, "n_lt_rg", cols, "sort_key", False, 5,
                                write_kw=kw)


def test_topn_n_greater_than_row_group_spans_multiple(tmp_path):
    # 2000 rows, row_group_size=100 -> 20 row groups; N spans several of them.
    cols, kw = _table(2000, row_group_size=100)
    _assert_topn_matches_oracle(tmp_path, "n_gt_rg", cols, "sort_key", False, 250,
                                write_kw=kw)


def test_topn_ties_on_sort_key(tmp_path):
    # A constant sort key forces every row into a tie at the boundary. Boundary
    # order is unspecified by the engine's own contract (native_sort.hpp), so
    # with every row tied any 20 of the 300 rows are a valid top-20: what must
    # hold is "20 distinct real rows, all with sort_key==42".
    n = 300
    cols = {
        "s": (pa.string(), ["row-%04d" % i for i in range(n)]),
        "sort_key": (pa.int64(), [42] * n),
    }
    ds = _write(str(tmp_path / "ties"), cols, row_group_size=50)
    sql = "SELECT s, sort_key FROM '%s' ORDER BY sort_key ASC LIMIT 20" % ds
    nat, nat_src = _drain_ordered(sql)
    table_rows = set(zip(cols["s"][1], cols["sort_key"][1]))
    assert len(nat) == 20
    assert all(row[1] == 42 for row in nat), nat
    assert all(row in table_rows for row in nat), nat
    assert len(set(nat)) == 20, "a row was returned twice"
    assert nat_src == ["NativeParquetScanSource"], nat_src


def test_topn_nulls_in_sort_column(tmp_path):
    # NULL is the lowest value (sorts FIRST for ASC, LAST for DESC). 75 of 300
    # rows are NULL here — more than the LIMIT — so:
    #   ASC  LIMIT 15 -> all 15 results are NULL (a 75-way tie; WHICH 15 of the
    #                    75 survive is unspecified — check they are real NULL rows).
    #   DESC LIMIT 15 -> NULLs sort last, so the 15 largest non-null values win;
    #                    non-null values are unique, so the SEQUENCE is fully
    #                    determined and checked exactly against the oracle.
    n = 300
    cols = {
        "s": (pa.string(), ["row-%04d" % i for i in range(n)]),
        "sort_key": (pa.int64(), [None if i % 4 == 0 else i for i in range(n)]),
    }
    ds = _write(str(tmp_path / "nulls"), cols, row_group_size=60)
    table_rows = set(zip(cols["s"][1], cols["sort_key"][1]))

    sql_asc = "SELECT s, sort_key FROM '%s' ORDER BY sort_key ASC LIMIT 15" % ds
    nat, nat_src = _drain_ordered(sql_asc)
    assert len(nat) == 15
    assert all(row[1] is None for row in nat), nat
    assert all(row in table_rows for row in nat), nat
    assert len(set(nat)) == 15, "a row was returned twice"
    assert nat_src == ["NativeParquetScanSource"], nat_src

    sql_desc = "SELECT s, sort_key FROM '%s' ORDER BY sort_key DESC LIMIT 15" % ds
    nat, nat_src = _drain_ordered(sql_desc)
    assert nat == _oracle(cols, ("s", "sort_key"), "sort_key", True, 15)
    assert nat_src == ["NativeParquetScanSource"], nat_src


def test_topn_with_where_predicate_now_native(tmp_path):
    """R3: the composed shape (fused TopN WITH a predicate) is served by
    `LatmatScanSource`, which does both passes natively and keeps the decode-skip —
    see tests/unit/operators/test_wp_r3_latmat_scan.py for that Source's own
    correctness matrix (ties, NULLs, row-group-spanning tie blocks, alignment).

    The ordering assertion is the point: the exact row SEQUENCE must equal the
    plain-Python oracle. `sort_key` is distinct here, so there are no ties to make
    the order legitimately ambiguous.

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
    nat, nat_src = _drain_ordered(sql)
    expect = _oracle(cols, ("s", "sort_key", "flag"), "sort_key", False, 20,
                     where=lambda r: r["flag"] == 1)
    assert nat == expect, (nat, expect)
    assert nat_src == ["LatmatScanSource"], nat_src


def test_topn_large_n_edge(tmp_path):
    # LIMIT exceeds the total row count -> every row is returned, in sort order.
    cols, kw = _table(50)
    _assert_topn_matches_oracle(tmp_path, "large_n", cols, "sort_key", False, 1000,
                                write_kw=kw)


def test_census_reports_no_fused_topn_residual():
    """R3 close-out: the census tally over the clickbench + tpch battery no longer
    reports ANY `fused_topn` residual. ClickBench Q24 (fused TopN WITH a predicate)
    was the single trigger and now runs on `LatmatScanSource`. It was also the last
    reachable residual of any kind in this battery, so nothing is refused."""
    import native_residual_census as census  # dev/native_residual_census.py

    tally = census.census()
    assert tally.get("fused_topn") is None, tally
    assert tally["__refused__"] == 0, tally


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
