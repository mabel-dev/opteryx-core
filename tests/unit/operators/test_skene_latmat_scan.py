"""The two-pass late-materialization SKENE scan — `WHERE ... ORDER BY ... LIMIT`
over a wide projection, run by `NativeSkeneLatmatScanSource`
(src/cpp/engine/native_skene_latmat_scan_source.hpp).

    pass 1  decode ONLY the predicate columns + the sort key, for every file;
            keep each survivor's sort key and its row position
    reduce  find the LIMIT boundary in the sort key across ALL files; drop every
            survivor strictly worse than it (n rows plus ties at the boundary)
    pass 2  decode the FULL projection for the files still holding a candidate

The parquet twin's tests are tests/unit/operators/test_wp_r3_latmat_scan.py, and the
assertions here are deliberately the same three, for the same reason: `ORDER BY ...
LIMIT n` over a tie block wider than the cut has no defined answer beyond "n rows, and
every row at least as good as the n-th", so a sequence or row-multiset comparison would
assert something the engine never promised. Each case therefore checks

  1. the row COUNT matches the reference,
  2. the multiset of SORT KEYS matches it exactly — fully determined even with ties,
     and what pins "nothing strictly worse than the n-th got in, and nothing better
     got dropped",
  3. every returned row is a whole real survivor row, which is what catches a pass-2
     gather that pairs one row's key with another row's payload.

The reference arm is the same query with `config.features.skene_late_materialization`
off, i.e. the ordinary single-pass `NativeSkeneScanSource` — the un-pushed ground
truth, and the path every skene scan took before this landed.

One property has no parquet counterpart and is asserted separately: the Filter node
above the scan STAYS in the plan. That is what makes the reduction safe by
construction (the scan may only drop rows the Filter or the TopNSink would have
dropped anyway), and it is why this needed no change to skene's predicate-pushdown
decline in `FileSystemTable.can_push`. See `test_filter_node_is_not_consumed`.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pyarrow as pa  # test-only dep (allowed in tests/)
import pyarrow.parquet as pq
import pytest

import opteryx
import opteryx.config as config

# Matching rows are 1-in-4 — selective enough to clear the late-materialization
# selectivity gate.
NEEDLE = "%pick%"

# 250 rows/file over 3000 rows == 12 files. One .skene file IS one row group, so this
# is what makes every tie block below straddle FILE boundaries: a reduction that found
# its boundary per file instead of across all of them passes a single-file fixture and
# is still wrong.
N = 3000
ROWS_PER_FILE = 250
_MATCH = [i % 4 == 0 for i in range(N)]


def _write_skene(dataset_dir, columns, rows_per_file=ROWS_PER_FILE):
    """Write `columns` as a skene dataset: parquet via pyarrow, then one .skene per
    parquet row group — the same conversion dev/parquet_to_skene.py does."""
    import skene
    from rugo.parquet import read_parquet

    os.makedirs(dataset_dir, exist_ok=True)
    parquet_path = os.path.join(dataset_dir, "_source.parquet")
    arrays = {name: pa.array(values, type=typ) for name, (typ, values) in columns.items()}
    pq.write_table(pa.table(arrays), parquet_path, row_group_size=rows_per_file)
    with read_parquet(parquet_path) as reader:
        for index, morsel in enumerate(reader):
            payload = skene.write_morsel(morsel, read_acceleration=True, zstd_level=0)
            with open(os.path.join(dataset_dir, "part-rg%04d.skene" % index), "wb") as out:
                out.write(payload)
    # The scan reader is chosen from the dataset's file format and a dataset is
    # single-format: leaving the parquet behind would make this a MIXED manifest.
    os.remove(parquet_path)
    return dataset_dir


def _drain(sql, latmat, monkeypatch):
    """Run `sql` and return (rows, names, scan_sources). `latmat` False disables the
    two-pass path, which is the single-pass reference."""
    if not latmat:
        monkeypatch.setattr(config.features, "skene_late_materialization", False)
    session = opteryx.session()
    rows = []
    names = []
    for morsel in session.execute_to_morsels(sql):
        raw = list(morsel.column_names)   # bytes at the native boundary
        names = [n.decode("utf-8") if isinstance(n, bytes) else n for n in raw]
        for i in range(morsel.num_rows):
            rows.append(tuple(repr(morsel.column(n)[i]) for n in raw))
    src = list(session.telemetry["scan_sources"].values())
    if not latmat:
        monkeypatch.undo()
    return rows, names, src


def _assert_latmat_parity(tmp_path, name, columns, sql_tail, monkeypatch,
                          key_column="k", rows_per_file=ROWS_PER_FILE):
    path = _write_skene(os.path.join(str(tmp_path), name), columns,
                        rows_per_file=rows_per_file)
    sql = "SELECT " + sql_tail.format(DATASET="'%s'" % path)
    ref_rows, ref_names, ref_src = _drain(sql, latmat=False, monkeypatch=monkeypatch)
    nat_rows, nat_names, nat_src = _drain(sql, latmat=True, monkeypatch=monkeypatch)

    assert ref_src == ["NativeSkeneScanSource"], (
        "the reference run must be the single-pass skene scan, got %s" % ref_src)
    assert nat_src == ["NativeSkeneLatmatScanSource"], (
        "%s: expected the two-pass late-mat skene Source, got %s — this case is not "
        "exercising late materialization at all" % (name, nat_src))
    assert nat_names == ref_names, "%s: output column layout differs" % name

    # Every returned row must be a genuine survivor row, whole.
    where = sql_tail.split(" WHERE ", 1)[1].split(" ORDER BY ")[0]
    universe_rows, _, _ = _drain("SELECT * FROM '%s' WHERE %s" % (path, where),
                                 latmat=False, monkeypatch=monkeypatch)
    universe = set(universe_rows)

    k = ref_names.index(key_column)
    ref_keys = sorted(r[k] for r in ref_rows)
    assert len(nat_rows) == len(ref_rows), (
        "%s: row COUNT differs — %d vs reference %d"
        % (name, len(nat_rows), len(ref_rows)))
    keys = sorted(r[k] for r in nat_rows)
    assert keys == ref_keys, (
        "%s: sort-key multiset differs from the un-pushed plan\n  got: %s\n  ref: %s"
        % (name, keys, ref_keys))
    stray = [r for r in nat_rows if r not in universe]
    assert not stray, (
        "%s: %d returned row(s) are not rows of the table — the pass-2 gather is "
        "misaligned. First: %s" % (name, len(stray), stray[0]))
    return nat_rows, ref_names


def _tags():
    return [("pick-%d" % i) if m else ("skip-%d" % i) for i, m in enumerate(_MATCH)]


def _payload_columns():
    """Ten payload columns nothing in the query touches. Their whole job is to be
    DEFERRED: they are what pass 2 exists to avoid decoding, and they take the
    projection past `skene_late_materialization_min_deferred_columns` (8), which is
    the gate that keeps narrow projections on the single-pass path."""
    return {
        "p%d" % j: (pa.string(), ["p%d-%d" % (j, i) for i in range(N)])
        for j in range(10)
    }


def _base(key_type, key_values):
    columns = {"k": (key_type, key_values), "tag": (pa.string(), _tags())}
    columns.update(_payload_columns())
    return columns


def test_int_key_ascending(tmp_path, monkeypatch):
    """The plain shape: distinct integer key, wide projection, ASC."""
    _assert_latmat_parity(
        tmp_path, "int_asc", _base(pa.int64(), list(range(N))),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k LIMIT 10",
        monkeypatch)


def test_int_key_descending(tmp_path, monkeypatch):
    _assert_latmat_parity(
        tmp_path, "int_desc", _base(pa.int64(), list(range(N))),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k DESC LIMIT 10",
        monkeypatch)


def test_ties_straddling_files(tmp_path, monkeypatch):
    """A tie block far wider than the cut, spread across every file. The reduction
    must keep every row tied with the n-th (`!cmp(boundary, r)`), never n exactly —
    dropping a tied row here silently changes which rows the TopNSink can pick."""
    _assert_latmat_parity(
        tmp_path, "ties", _base(pa.int64(), [i % 3 for i in range(N)]),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k LIMIT 10",
        monkeypatch)


def test_null_key_ascending(tmp_path, monkeypatch):
    """draken orders NULL BELOW every value, so ASC puts NULLs FIRST — they are the
    answer, not rows to skip. The parquet twin shipped this bug twice; this Source
    cannot, because it reduces with draken's own comparator rather than a null rule
    written by hand — and this is the test that says so."""
    keys = [None if i % 7 == 0 else i for i in range(N)]
    _assert_latmat_parity(
        tmp_path, "null_asc", _base(pa.int64(), keys),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k LIMIT 10",
        monkeypatch)


def test_null_key_descending(tmp_path, monkeypatch):
    """The same column DESC: NULLs sort LAST, so none of them should come back while
    non-null survivors remain."""
    keys = [None if i % 7 == 0 else i for i in range(N)]
    _assert_latmat_parity(
        tmp_path, "null_desc", _base(pa.int64(), keys),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k DESC LIMIT 10",
        monkeypatch)


def test_string_key(tmp_path, monkeypatch):
    """A string sort key: `build_sort_keys` holds POINTERS into the pass-1 key
    morsels, so this is what proves those morsels outlive the reduction."""
    _assert_latmat_parity(
        tmp_path, "str_key",
        _base(pa.string(), ["k-%05d" % (N - i) for i in range(N)]),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k LIMIT 10",
        monkeypatch)


def test_float_key(tmp_path, monkeypatch):
    _assert_latmat_parity(
        tmp_path, "float_key",
        _base(pa.float64(), [float(N - i) / 8.0 for i in range(N)]),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k LIMIT 10",
        monkeypatch)


def test_sort_key_is_also_a_predicate_column(tmp_path, monkeypatch):
    """The sort key already IS a pass-1 column. It must not be added twice — a
    duplicated pass-1 entry would shift `pred_col_to_p1` and evaluate the predicate
    against the wrong column."""
    _assert_latmat_parity(
        tmp_path, "key_is_pred", _base(pa.int64(), list(range(N))),
        "* FROM {DATASET} WHERE k > 100 ORDER BY k LIMIT 10", monkeypatch)


def test_multiple_conjuncts(tmp_path, monkeypatch):
    """Two ANDed terms. They reach the physical Filter as one n-ary DNF node, which
    is why the selectivity estimate splits on `_inner_split` rather than estimating
    the tree — an AND/DNF node estimates as the 1.0 "unknown" default and would
    decline every multi-term query."""
    _assert_latmat_parity(
        tmp_path, "conjuncts", _base(pa.int64(), list(range(N))),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' AND k > 100 "
        "ORDER BY k LIMIT 10", monkeypatch)


def test_limit_exceeds_survivors(tmp_path, monkeypatch):
    """LIMIT larger than the number of matching rows: the reduction keeps everything
    and pass 2 opens every file that has a survivor. Correct, just not a saving."""
    _assert_latmat_parity(
        tmp_path, "big_limit", _base(pa.int64(), list(range(N))),
        "* FROM {DATASET} WHERE k > " + str(N - 5) + " ORDER BY k LIMIT 1000",
        monkeypatch)


def test_no_rows_match(tmp_path, monkeypatch):
    """Nothing survives pass 1 — the reduction has no candidates and pass 2 opens no
    file at all. The scan must finish clean and empty, not stall on the barrier."""
    path = _write_skene(os.path.join(str(tmp_path), "empty"),
                        _base(pa.int64(), list(range(N))))
    sql = ("SELECT * FROM '%s' WHERE tag LIKE '%%nothing%%' ORDER BY k LIMIT 10"
           % path)
    rows, _, src = _drain(sql, latmat=True, monkeypatch=monkeypatch)
    assert src == ["NativeSkeneLatmatScanSource"]
    assert rows == []


# --------------------------------------------------------------------------------
# Shapes that must DECLINE — each falls through to the single-pass Source, which is
# exactly the work skene did before this landed, never a wrong answer.
# --------------------------------------------------------------------------------

def _source_for(tmp_path, name, sql_tail, monkeypatch, columns=None):
    path = _write_skene(os.path.join(str(tmp_path), name),
                        columns or _base(pa.int64(), list(range(N))))
    sql = "SELECT " + sql_tail.format(DATASET="'%s'" % path)
    _rows, _names, src = _drain(sql, latmat=True, monkeypatch=monkeypatch)
    return src


def test_narrow_projection_declines(tmp_path, monkeypatch):
    """One projected column, and it is already a pass-1 column. Deferring nothing
    would cost a second open and a second decode to save nothing — this is the shape
    the reader-side-filter ruling was about, and the min-deferred gate is what keeps
    two passes away from it."""
    assert _source_for(tmp_path, "narrow", "k FROM {DATASET} WHERE tag LIKE '"
                       + NEEDLE + "' ORDER BY k LIMIT 10",
                       monkeypatch) == ["NativeSkeneScanSource"]


def test_no_predicate_declines(tmp_path, monkeypatch):
    """No WHERE at all: there is no Filter node above the scan, so there is no
    predicate to reduce with and every row is a candidate."""
    assert _source_for(tmp_path, "nowhere", "* FROM {DATASET} ORDER BY k LIMIT 10",
                       monkeypatch) == ["NativeSkeneScanSource"]


def test_no_limit_declines(tmp_path, monkeypatch):
    """ORDER BY with no LIMIT is a full Sort, not a top-n — there is no boundary to
    reduce to, so every surviving row has to be materialized anyway."""
    assert _source_for(tmp_path, "nolimit", "* FROM {DATASET} WHERE tag LIKE '"
                       + NEEDLE + "' ORDER BY k",
                       monkeypatch) == ["NativeSkeneScanSource"]


def test_feature_flag_off_declines(tmp_path, monkeypatch):
    monkeypatch.setattr(config.features, "skene_late_materialization", False)
    path = _write_skene(os.path.join(str(tmp_path), "flagoff"),
                        _base(pa.int64(), list(range(N))))
    session = opteryx.session()
    for _ in session.execute_to_morsels(
        "SELECT * FROM '%s' WHERE tag LIKE '%s' ORDER BY k LIMIT 10" % (path, NEEDLE)
    ):
        pass
    assert list(session.telemetry["scan_sources"].values()) == [
        "NativeSkeneScanSource"]


def test_filter_node_is_not_consumed(tmp_path, monkeypatch):
    """The Filter above the scan SURVIVES into the executed plan.

    This is the load-bearing safety property, not an incidental one. The scan is
    allowed to emit a superset of the answer (it drops only predicate failures and
    rows strictly worse than the n-th best), and the Filter re-running over the
    handful of candidates is what makes that superset harmless. It is also why this
    optimization needed no change to `FileSystemTable.can_push`, which still declines
    predicate pushdown for skene on its own measured grounds.
    """
    path = _write_skene(os.path.join(str(tmp_path), "keepfilter"),
                        _base(pa.int64(), list(range(N))))
    sql = ("SELECT * FROM '%s' WHERE tag LIKE '%s' ORDER BY k LIMIT 10"
           % (path, NEEDLE))
    # The scan really does take the two-pass path for this query...
    _rows, _names, src = _drain(sql, latmat=True, monkeypatch=monkeypatch)
    assert src == ["NativeSkeneLatmatScanSource"]
    # ...and the Filter is still there above it. (EXPLAIN renders the plan without
    # compiling a native scan, hence the separate run.)
    plan_rows = []
    session = opteryx.session()
    for morsel in session.execute_to_morsels("EXPLAIN " + sql):
        names = [n.decode("utf-8") if isinstance(n, bytes) else n
                 for n in morsel.column_names]
        for i in range(morsel.num_rows):
            plan_rows.append(repr(morsel.column(names[0])[i]))
    assert any("Filter" in row for row in plan_rows), (
        "the Filter node above the skene scan was consumed — the two-pass Source's "
        "correctness argument depends on it still being there: %s" % plan_rows)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
