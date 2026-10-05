"""A2 — admit the zero-projection COUNT(*) WITH a pushed predicate to the native
parquet scan (residual reason `zero_projection` / R1).

Background: a bare `SELECT COUNT(*) FROM t` (NO predicate) never reaches a scan at
all — `StatisticsOnlyResponseStrategy` (opteryx/planner/optimizer/strategies/
statistics_only_response.py) rewrites it to a literal manifest-count projection over
the virtual `$one_row` relation at the optimizer level. The only SQL-reachable
`zero_projection` residual is `SELECT COUNT(*) FROM t WHERE <predicate>`: the
Aggregate has an empty projection but the predicate still needs its input column(s)
read and filtered.

A2 admits this shape natively with NO engine change: `_native_scan_plan`
(opteryx/managers/execution/compiler.py) already builds a READ-SET of role-3
predicate-input columns when `scan.columns` is empty (the list comprehension over
`scan.columns` degenerates to `[]`, and predicate columns are appended exactly as
they are for any other role-3 case). The trailing Select's `emit_indices`/`emit_ids`
are then both empty — `ColumnSelectOperator` (src/cpp/engine/engine.hpp) already
handles a zero-index Select: it emits a genuine zero-column morsel whose row count
rides on `zero_col_rows = in->num_rows()`, the contract `UngroupedAggSink`'s
CountStar reads. The only change was removing the
compiler's unconditional bail on `not scan.columns`; it now only bails when there is
ALSO no predicate (nothing to build a read-set from at all — a shape with no SQL
trigger for parquet scans, since the manifest-count rewrite runs whenever a manifest
is present).

Correctness gate: an INDEPENDENT plain-Python oracle — the native COUNT(*) must equal
the predicate evaluated in Python over the values this module wrote (the Python
per-morsel scan that used to serve as the A/B baseline was deleted, ruling
2026-10-03) — across single-file, multi-file/multi-row-group, and a zero-matching-rows
predicate. Every query must also select NativeParquetScanSource and record no
residual reason.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../..", "dev"))

import pyarrow as pa  # test-only dep (allowed in tests/) — WRITER only
import pyarrow.parquet as pq
import pytest

import opteryx


def _write_one(path, columns, use_dictionary=True, row_group_size=None):
    """Write one parquet file at `path`. `columns` = {name: (pyarrow_type, py_list)}."""
    arrays = {name: pa.array(vals, type=typ) for name, (typ, vals) in columns.items()}
    kw = {"use_dictionary": use_dictionary}
    if row_group_size is not None:
        kw["row_group_size"] = row_group_size
    pq.write_table(pa.table(arrays), path, **kw)


def _write_single_file(dataset_dir, columns, **kw):
    os.makedirs(dataset_dir, exist_ok=True)
    _write_one(os.path.join(dataset_dir, "part.parquet"), columns, **kw)
    return dataset_dir


def _write_multi_file(dataset_dir, columns, n_files, **kw):
    """Write `n_files` files, each carrying an independent slice of `columns` (so the
    dataset spans multiple files AND, via row_group_size, multiple row groups per
    file)."""
    os.makedirs(dataset_dir, exist_ok=True)
    names = list(columns)
    total = len(next(iter(columns.values()))[1])
    per_file = total // n_files
    for i in range(n_files):
        lo, hi = i * per_file, total if i == n_files - 1 else (i + 1) * per_file
        slice_cols = {n: (typ, vals[lo:hi]) for n, (typ, vals) in columns.items()}
        _write_one(os.path.join(dataset_dir, "part-%03d.parquet" % i), slice_cols, **kw)
    return dataset_dir


def _count_star(sql):
    """Run `sql` (a COUNT(*) query) natively; return the count. Asserts the scan
    selected NativeParquetScanSource and recorded no residual reason."""
    session = opteryx.session()
    values = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            values.append(morsel.column(morsel.column_names[0])[i])
    telemetry = session.telemetry
    assert list(telemetry["scan_sources"].values()) == ["NativeParquetScanSource"], (
        telemetry["scan_sources"])
    assert telemetry.get("scan_residual_reasons", {}) == {}, telemetry["scan_residual_reasons"]
    assert len(values) == 1, values
    return values[0]


def test_count_star_with_predicate_single_file(tmp_path):
    """The canonical A0/A1 `zero_projection` HAND_SET trigger — single file,
    single row group — now selects the native scan."""
    cols = {"v": (pa.int64(), list(range(500))),
            "flag": (pa.int64(), [i % 3 for i in range(500)])}
    ds = _write_single_file(str(tmp_path / "a2_single"), cols)
    expected = sum(1 for f in cols["flag"][1] if f > 0)
    val = _count_star("SELECT COUNT(*) FROM '%s' WHERE flag > 0" % ds)
    assert val == expected


def test_count_star_with_predicate_multi_row_group(tmp_path):
    """Single file, several row groups — row-group pruning + per-row-group filter
    accumulate to the correct total."""
    n_rows = 4000
    cols = {"v": (pa.int64(), list(range(n_rows))),
            "flag": (pa.int64(), [i % 5 for i in range(n_rows)])}
    ds = _write_single_file(str(tmp_path / "a2_rowgroups"), cols, row_group_size=250)
    expected = sum(1 for f in cols["flag"][1] if f == 0)
    val = _count_star("SELECT COUNT(*) FROM '%s' WHERE flag = 0" % ds)
    assert val == expected


def test_count_star_with_predicate_multi_file(tmp_path):
    """Dataset spans multiple files (and multiple row groups per file) — the count
    aggregates correctly across every scanned file."""
    n_rows = 6000
    cols = {"v": (pa.int64(), list(range(n_rows))),
            "flag": (pa.int64(), [i % 7 for i in range(n_rows)])}
    ds = _write_multi_file(str(tmp_path / "a2_multifile"), cols, n_files=6,
                            row_group_size=200)
    expected = sum(1 for f in cols["flag"][1] if f != 3)
    val = _count_star("SELECT COUNT(*) FROM '%s' WHERE flag <> 3" % ds)
    assert val == expected


def test_count_star_zero_matching_rows(tmp_path):
    """A predicate that matches nothing (row-group pruning may discard every group,
    or every row may survive pruning and then fail the per-row filter) — COUNT(*)
    correctly returns 0, natively."""
    n_rows = 1000
    cols = {"v": (pa.int64(), list(range(n_rows)))}
    ds = _write_single_file(str(tmp_path / "a2_zero"), cols, row_group_size=100)
    val = _count_star("SELECT COUNT(*) FROM '%s' WHERE v < 0" % ds)
    assert val == 0


def test_census_zero_projection_closed():
    """The census tally no longer reports `zero_projection` over the clickbench +
    tpch battery: the reachable trigger (COUNT(*) WITH a WHERE) now goes native."""
    import native_residual_census as census  # dev/native_residual_census.py

    tally = census.census()
    assert "zero_projection" not in tally, tally


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
