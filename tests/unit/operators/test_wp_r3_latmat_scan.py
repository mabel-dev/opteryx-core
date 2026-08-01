"""R3 (`fused_topn`) — the composed `WHERE ... ORDER BY ... LIMIT` scan shape, run
natively by the two-pass late-materialization Source.

This is the shape A3 deliberately left on the trampoline: the scan carries a
`topn_sort_name`/`topn_limit` hint from `TopNScanPushdownStrategy` AND a pushed
predicate, so a decode-skip is genuinely load-bearing (ClickBench Q24 decodes only
`URL` + `EventTime` for the whole table, then ~100 more columns for the handful of
LIKE survivors). `LatmatScanSource` (src/cpp/engine/native_latmat_scan_source.hpp)
now does that natively:

    pass 1  decode predicate columns + sort key -> survivor bitmap per row group
    reduce  find the LIMIT boundary in the sort key across ALL row groups; drop every
            survivor strictly worse than it (n rows plus ties at the boundary)
    pass 2  decode the remaining projected columns, masked to those rows only

**What these tests assert, and why that reference.** The contract is that pushing the
top-n into the scan does not change the ANSWER — the same query with no
late-materialization at all must return the same rows. So each case runs twice in one
process: once natively (LatmatScanSource) and once with
`config.features.parquet_late_materialization` off, which takes the ordinary
single-pass `NativeParquetScanSource` and is therefore the un-pushed ground truth.

Comparison is NOT a sequence, and cannot even be a plain row multiset. `ORDER BY ...
LIMIT n` over a tie block wider than the cut has no defined answer beyond "n rows, and
every row at least as good as the n-th" — WHICH tied rows come back is unspecified, and
genuinely differs between a one-pass and a two-pass scan. So each case asserts the
three things SQL actually promises:

  1. the row COUNT matches the un-pushed plan,
  2. the multiset of SORT KEYS matches it exactly (this is fully determined, ties or
     not — it is what pins "no row strictly worse than the n-th got in, and none
     better got dropped"),
  3. every returned row is a real survivor row (a subset of `WHERE <pred>` with no
     ORDER BY / LIMIT), so a two-pass zip that pairs one row's key with another row's
     payload fails even when the counts and keys look right.

Together those are equivalent to correctness, and unlike a sequence comparison they do
not assert anything the engine never promised.

⚠ These tests deliberately do NOT compare against the TRAMPOLINE's answer. The
trampoline's `_apply_topn` (parquet_read.pyx) hard-codes "NULLs sort last" in both
directions, but draken orders NULL BELOW every value (`SortKeyCmp`: NULLs FIRST
ascending, LAST descending) — so for `ORDER BY <nullable> ASC LIMIT n` with more than
n non-null survivors it drops NULL rows that belong in the answer and returns rows the
un-pushed plan does not. That is a pre-existing trampoline bug, still open, and
`test_trampoline_null_asc_divergence_is_the_trampolines_bug` pins it so it stays
visible instead of silently masking a regression here. The native Source has no null
rule of its own at all — it reduces with draken's own comparator, the same one the
downstream TopNSink sorts with, so ties and NULLs are correct by construction.
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

# The predicate every case pushes. Matching rows are 1-in-4, which is selective
# enough to clear the late-materialization selectivity gate.
NEEDLE = "%pick%"


def _write(dataset_dir, columns, row_group_size=250):
    os.makedirs(dataset_dir, exist_ok=True)
    arrays = {name: pa.array(vals, type=typ) for name, (typ, vals) in columns.items()}
    pq.write_table(pa.table(arrays), os.path.join(dataset_dir, "part.parquet"),
                   row_group_size=row_group_size)
    return dataset_dir


def _drain(sql, latmat, monkeypatch):
    """Run `sql` and return (rows, names, scan_sources). `latmat` False disables
    late-materialization entirely, which is the un-pushed single-pass reference."""
    if not latmat:
        monkeypatch.setattr(config.features, "parquet_late_materialization", False)
    session = opteryx.session()
    rows = []
    names = []
    for morsel in session.execute_to_morsels(sql):
        raw = list(morsel.column_names)   # bytes at the native boundary
        names = [n.decode("utf-8") if isinstance(n, bytes) else n for n in raw]
        for i in range(morsel.num_rows):
            rows.append(tuple(repr(morsel.column(n)[i]) for n in raw))
    src = list(session._telemetry.as_dict()["scan_sources"].values())
    if not latmat:
        monkeypatch.undo()
    return rows, names, src


def _assert_latmat_parity(tmp_path, name, columns, sql_tail, monkeypatch,
                          key_column="k", row_group_size=250):
    """Write `columns`, run `SELECT {sql_tail}` natively and with
    late-materialization off, and assert the three properties in this module's
    docstring: same row count, same sort-key multiset, and every native row a real
    survivor row."""
    path = _write(os.path.join(str(tmp_path), name), columns,
                  row_group_size=row_group_size)
    sql = "SELECT " + sql_tail.format(DATASET=f"'{path}'")
    ref_rows, ref_names, ref_src = _drain(sql, latmat=False, monkeypatch=monkeypatch)
    nat_rows, nat_names, nat_src = _drain(sql, latmat=True, monkeypatch=monkeypatch)
    assert ref_src == ["NativeParquetScanSource"], (
        f"the reference run must be the single-pass native scan, got {ref_src}")
    assert nat_src == ["LatmatScanSource"], (
        f"{name}: expected the two-pass late-mat Source, got {nat_src} — this "
        "case is not exercising R3 at all")
    assert nat_names == ref_names, f"{name}: output column layout differs"

    assert len(nat_rows) == len(ref_rows), (
        f"{name}: row COUNT differs — native {len(nat_rows)} vs reference "
        f"{len(ref_rows)}")

    k = ref_names.index(key_column)
    nat_keys = sorted(r[k] for r in nat_rows)
    ref_keys = sorted(r[k] for r in ref_rows)
    assert nat_keys == ref_keys, (
        f"{name}: sort-key multiset differs from the un-pushed plan\n"
        f"  native: {nat_keys}\n  ref   : {ref_keys}")

    # Every returned row must be a genuine survivor row, whole. This is what catches a
    # pass-1/pass-2 misalignment: swap two rows' payloads and the counts and the key
    # multiset are both still perfect, but the rows themselves stop existing.
    where = sql_tail.split(" WHERE ", 1)[1].split(" ORDER BY ")[0]
    universe_sql = f"SELECT * FROM '{path}' WHERE {where}"
    universe_rows, _, _ = _drain(universe_sql, latmat=False, monkeypatch=monkeypatch)
    universe = set(universe_rows)
    stray = [r for r in nat_rows if r not in universe]
    assert not stray, (
        f"{name}: {len(stray)} returned row(s) are not rows of the table — the "
        f"pass-1/pass-2 zip is misaligned. First: {stray[0]}")
    return nat_rows, ref_names


# --------------------------------------------------------------------------------
# Fixtures: one 3000-row file at 250 rows/row-group == 12 row groups, so every tie
# block below genuinely straddles row-group boundaries (a per-row-group reduction
# would pass a single-row-group fixture and still be wrong).
# --------------------------------------------------------------------------------

N = 3000
_MATCH = [i % 4 == 0 for i in range(N)]


def _tags():
    return [("pick-%d" % i) if m else ("skip-%d" % i) for i, m in enumerate(_MATCH)]


def _payload_columns():
    """Projected-but-not-read-in-pass-1 columns — these are what pass 2 fetches, and
    what a misaligned zip would corrupt. Deliberately mixed width/encoding: a long
    (arena-resident) string, a float, an int, and a bool."""
    return {
        "pay_str": (pa.string(), ["payload-%d-long-enough-to-live-in-the-arena" % i
                                  for i in range(N)]),
        "pay_f64": (pa.float64(), [float(i) * 1.5 for i in range(N)]),
        "pay_i64": (pa.int64(), [i * 7 for i in range(N)]),
        "pay_bool": (pa.bool_(), [i % 3 == 0 for i in range(N)]),
    }


def _dataset(sort_values, sort_type=pa.int64()):
    cols = {"tag": (pa.string(), _tags()), "k": (sort_type, sort_values)}
    cols.update(_payload_columns())
    return cols


# --------------------------------------------------------------------------------


def test_latmat_ascending_unique_keys(tmp_path, monkeypatch):
    """The baseline shape: distinct keys, no NULLs, ascending."""
    _assert_latmat_parity(
        tmp_path, "asc_unique", _dataset([N - i for i in range(N)]),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k LIMIT 10",
        monkeypatch)


def test_latmat_descending_unique_keys(tmp_path, monkeypatch):
    _assert_latmat_parity(
        tmp_path, "desc_unique", _dataset([N - i for i in range(N)]),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k DESC LIMIT 10",
        monkeypatch)


@pytest.mark.parametrize("direction", ["", " DESC"])
def test_latmat_ties_span_the_boundary(tmp_path, monkeypatch, direction):
    """A tie block sitting exactly ON the n-th best value, spread across many row
    groups. The reduction must keep the WHOLE tie block (dropping part of it can
    change which n rows the downstream TopNSink finally keeps), so the two paths hand
    the sink the same candidate set."""
    # Every matching row gets key 100 except a few strictly-better ones, so the
    # boundary for LIMIT 10 lands inside the 100-block, which spans all 12 row groups.
    keys = []
    better = 0
    for i in range(N):
        if not _MATCH[i]:
            keys.append(999999)
        elif better < 4:
            keys.append(1)
            better += 1
        else:
            keys.append(100)
    _assert_latmat_parity(
        tmp_path, "ties_boundary" + direction.strip(), _dataset(keys),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k" + direction
        + " LIMIT 10", monkeypatch)


def test_latmat_all_null_sort_key(tmp_path, monkeypatch):
    """Every survivor's sort key is NULL — one giant tie block, both directions
    equivalent. Nothing may be dropped for being 'worse' than a NULL."""
    keys = [None if m else 5 for m in _MATCH]
    _assert_latmat_parity(
        tmp_path, "all_null", _dataset(keys),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k LIMIT 10",
        monkeypatch)


@pytest.mark.parametrize("direction", ["", " DESC"])
def test_latmat_fewer_non_null_than_n(tmp_path, monkeypatch, direction):
    """Fewer than n NON-NULL survivors, so NULL rows have to enter the answer —
    ascending they are the BEST key (draken orders NULL below every value) and
    descending they are the worst but still needed to fill n."""
    keys = []
    nonnull = 0
    for i in range(N):
        if not _MATCH[i]:
            keys.append(7)
        elif nonnull < 3:
            keys.append(1000 + nonnull)
            nonnull += 1
        else:
            keys.append(None)
    _assert_latmat_parity(
        tmp_path, "few_nonnull" + direction.strip(), _dataset(keys),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k" + direction
        + " LIMIT 10", monkeypatch)


def test_latmat_nulls_and_values_mixed_ascending(tmp_path, monkeypatch):
    """The case that exposes the trampoline's null rule: MORE than n non-null
    survivors AND some NULLs. Ascending, the NULLs are the best rows and must all
    survive the reduction."""
    keys = []
    seen = 0
    for i in range(N):
        if not _MATCH[i]:
            keys.append(500000 + i)
        else:
            keys.append(None if seen < 3 else 1000 + seen)
            seen += 1
    rows, names = _assert_latmat_parity(
        tmp_path, "mixed_nulls_asc", _dataset(keys),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k LIMIT 10",
        monkeypatch)
    # Belt and braces: the three NULL-key rows must actually be in the answer, so a
    # future regression that drops them cannot pass by also breaking the reference.
    assert sum(1 for r in rows if r[names.index("k")] == "None") == 3


def test_latmat_limit_larger_than_survivor_count(tmp_path, monkeypatch):
    """N above the number of surviving rows — no boundary exists, so every survivor
    must reach pass 2. (`nth_element` is never called on this path.)"""
    _assert_latmat_parity(
        tmp_path, "big_limit", _dataset([i for i in range(N)]),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k LIMIT 900",
        monkeypatch)


def test_latmat_string_sort_key(tmp_path, monkeypatch):
    """A VARCHAR sort key: the reduction takes draken's string key path (pointer +
    length, memcmp collation) rather than the normalized-uint64 one, and long values
    live in the arena the pass-1 morsels must keep alive across the barrier."""
    keys = [None if (i % 400 == 0) else ("key-%06d-and-a-long-tail-value" % (N - i))
            for i in range(N)]
    _assert_latmat_parity(
        tmp_path, "string_key", _dataset(keys, sort_type=pa.string()),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k LIMIT 10",
        monkeypatch)


def test_latmat_float_sort_key(tmp_path, monkeypatch):
    """A FLOAT64 sort key, including negatives and -0.0 — draken's normalized float
    key is order-preserving across the sign bit, which a naive bit compare is not."""
    keys = [None if (i % 500 == 0) else (float(i) - 1500.0) * 0.25 for i in range(N)]
    keys[4] = -0.0
    keys[8] = 0.0
    _assert_latmat_parity(
        tmp_path, "float_key", _dataset(keys, sort_type=pa.float64()),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k DESC LIMIT 10",
        monkeypatch)


def test_latmat_zero_survivors(tmp_path, monkeypatch):
    """A predicate no row matches: pass 1 finds nothing, so there is no boundary, no
    pass-2 work, and the Source must finish cleanly rather than deadlock or emit."""
    path = _write(os.path.join(str(tmp_path), "no_match"),
                  _dataset([i for i in range(N)]))
    sql = (f"SELECT * FROM '{path}' WHERE tag LIKE '%nothing-matches-this%' "
           "ORDER BY k LIMIT 10")
    nat_rows, _, nat_src = _drain(sql, latmat=True, monkeypatch=monkeypatch)
    assert nat_src == ["LatmatScanSource"], nat_src
    assert nat_rows == []


def test_latmat_sort_key_is_also_the_predicate_column(tmp_path, monkeypatch):
    """The sort key and the predicate column are the SAME column, so pass 1 reads one
    column and the output takes it from pass 1 while every other column comes from
    pass 2."""
    keys = [i for i in range(N)]
    cols = {"k": (pa.int64(), keys)}
    cols.update(_payload_columns())
    path = _write(os.path.join(str(tmp_path), "same_col"), cols)
    sql = f"SELECT * FROM '{path}' WHERE k < 900 ORDER BY k DESC LIMIT 10"
    ref_rows, _, ref_src = _drain(sql, latmat=False, monkeypatch=monkeypatch)
    nat_rows, _, nat_src = _drain(sql, latmat=True, monkeypatch=monkeypatch)
    assert ref_src == ["NativeParquetScanSource"], ref_src
    assert nat_src == ["LatmatScanSource"], nat_src
    assert sorted(nat_rows) == sorted(ref_rows)


def test_latmat_pass2_columns_stay_aligned_with_their_own_rows(tmp_path, monkeypatch):
    """The failure mode a row-count-only test cannot see: pass 2 decodes only masked
    rows, so if the mask and the pass-1 survivor order disagree by even one row, every
    output row pairs one row's key with another row's payload. The payload columns are
    deterministic functions of the row index, so this checks the pairing directly."""
    keys = [i for i in range(N)]
    path = _write(os.path.join(str(tmp_path), "alignment"), _dataset(keys))
    sql = (f"SELECT * FROM '{path}' WHERE tag LIKE '{NEEDLE}' ORDER BY k DESC LIMIT 25")
    rows, _, src = _drain(sql, latmat=True, monkeypatch=monkeypatch)
    assert src == ["LatmatScanSource"], src
    assert len(rows) == 25
    for tag, k, pay_str, pay_f64, pay_i64, pay_bool in rows:
        i = int(k)
        assert tag == repr("pick-%d" % i)
        assert pay_str == repr("payload-%d-long-enough-to-live-in-the-arena" % i)
        assert pay_f64 == repr(float(i) * 1.5)
        assert pay_i64 == repr(i * 7)
        assert pay_bool == repr(i % 3 == 0)


def test_trampoline_null_asc_divergence_is_the_trampolines_bug(tmp_path, monkeypatch):
    """PINS a pre-existing TRAMPOLINE bug, so it stays visible and cannot be mistaken
    for a regression in the native path.

    `_apply_topn` (parquet_read.pyx) drops every NULL survivor once more than n
    non-null survivors exist, on the premise that "NULLs sort last". draken sorts NULL
    BELOW every value, so ascending they sort FIRST — they are exactly the rows that
    belong at the top of the answer. The trampoline therefore returns rows the
    un-pushed plan does not.

    If this test starts failing because the trampoline now AGREES, the bug has been
    fixed upstream and this test should be deleted."""
    keys = []
    seen = 0
    for i in range(N):
        if not _MATCH[i]:
            keys.append(500000 + i)
        else:
            keys.append(None if seen < 3 else 1000 + seen)
            seen += 1
    path = _write(os.path.join(str(tmp_path), "tramp_null"), _dataset(keys))
    sql = f"SELECT * FROM '{path}' WHERE tag LIKE '{NEEDLE}' ORDER BY k LIMIT 10"

    ref_rows, names, ref_src = _drain(sql, latmat=False, monkeypatch=monkeypatch)
    assert ref_src == ["NativeParquetScanSource"], ref_src
    ki = names.index("k")

    monkeypatch.setattr(pool_reader, "native_scan_supported", lambda *a, **k: False)
    tramp_rows, _, tramp_src = _drain(sql, latmat=True, monkeypatch=monkeypatch)
    monkeypatch.undo()
    assert tramp_src == ["StreamingScanSource"], tramp_src

    assert sum(1 for r in ref_rows if r[ki] == "None") == 3
    assert sum(1 for r in tramp_rows if r[ki] == "None") == 0, (
        "the trampoline's _apply_topn now keeps NULL rows for ASC — the bug this "
        "test pins has been fixed; delete this test")
    assert sorted(tramp_rows) != sorted(ref_rows)


if __name__ == "__main__":  # pragma: no cover
    import pytest as _p
    raise SystemExit(_p.main([__file__, "-q"]))
