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

Each case runs a THIRD arm: the trampoline scan (forced via `native_scan_supported`),
which has its own independent implementation of the same reduction (`_apply_topn` in
parquet_read.pyx). Both reduced paths are checked against the same un-pushed
reference, which is what stops the two from drifting apart.

That third arm exists because they HAD drifted. `_apply_topn` used to hard-code "NULLs
sort last" in both directions, but draken orders NULL BELOW every value (`SortKeyCmp`:
NULLs FIRST ascending, LAST descending) — so for `ORDER BY <nullable> ASC LIMIT n` with
more than n non-null survivors it dropped NULL rows that belong in the answer. Both
sides are fixed now and hold the rule in ONE place each: the native Source reduces with
draken's own comparator, and `_apply_topn` reduces on `_topn_rank`, which encodes the
same ordering — neither writes a null branch by hand. See
`test_trampoline_apply_topn_keeps_null_rows_ascending`.
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


def _drain(sql, latmat, monkeypatch, trampoline=False):
    """Run `sql` and return (rows, names, scan_sources). `latmat` False disables
    late-materialization entirely, which is the un-pushed single-pass reference.
    `trampoline` forces the scan off the native Sources onto `StreamingScanSource`,
    which is how the trampoline's OWN two-pass path (`_apply_topn`) is exercised."""
    if not latmat:
        monkeypatch.setattr(config.features, "parquet_late_materialization", False)
    if trampoline:
        monkeypatch.setattr(pool_reader, "native_scan_supported", lambda *a, **k: False)
    session = opteryx.session()
    rows = []
    names = []
    for morsel in session.execute_to_morsels(sql):
        raw = list(morsel.column_names)   # bytes at the native boundary
        names = [n.decode("utf-8") if isinstance(n, bytes) else n for n in raw]
        for i in range(morsel.num_rows):
            rows.append(tuple(repr(morsel.column(n)[i]) for n in raw))
    src = list(session.telemetry["scan_sources"].values())
    if not latmat or trampoline:
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
    tra_rows, tra_names, tra_src = _drain(sql, latmat=True, monkeypatch=monkeypatch,
                                          trampoline=True)
    assert ref_src == ["NativeParquetScanSource"], (
        f"the reference run must be the single-pass native scan, got {ref_src}")
    assert nat_src == ["LatmatScanSource"], (
        f"{name}: expected the two-pass late-mat Source, got {nat_src} — this "
        "case is not exercising R3 at all")
    assert tra_src == ["StreamingScanSource"], (
        f"{name}: expected the trampoline for the third arm, got {tra_src}")
    assert nat_names == ref_names, f"{name}: output column layout differs"
    assert tra_names == ref_names, f"{name}: trampoline output column layout differs"

    # Every returned row must be a genuine survivor row, whole. This is what catches a
    # pass-1/pass-2 misalignment: swap two rows' payloads and the counts and the key
    # multiset are both still perfect, but the rows themselves stop existing.
    where = sql_tail.split(" WHERE ", 1)[1].split(" ORDER BY ")[0]
    universe_sql = f"SELECT * FROM '{path}' WHERE {where}"
    universe_rows, _, _ = _drain(universe_sql, latmat=False, monkeypatch=monkeypatch)
    universe = set(universe_rows)

    k = ref_names.index(key_column)
    ref_keys = sorted(r[k] for r in ref_rows)
    # Both reduced paths — the native two-pass Source and the trampoline's own
    # `_apply_topn` — must agree with the un-pushed plan. They share the ordering
    # rule (NULL below every value) but implement it independently, so checking
    # both is what keeps them from drifting apart again.
    for arm, rows in (("native", nat_rows), ("trampoline", tra_rows)):
        assert len(rows) == len(ref_rows), (
            f"{name} [{arm}]: row COUNT differs — {len(rows)} vs reference "
            f"{len(ref_rows)}")
        keys = sorted(r[k] for r in rows)
        assert keys == ref_keys, (
            f"{name} [{arm}]: sort-key multiset differs from the un-pushed plan\n"
            f"  got: {keys}\n  ref: {ref_keys}")
        stray = [r for r in rows if r not in universe]
        assert not stray, (
            f"{name} [{arm}]: {len(stray)} returned row(s) are not rows of the "
            f"table — the pass-1/pass-2 zip is misaligned. First: {stray[0]}")
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


@pytest.mark.parametrize("direction", ["", " DESC"])
def test_latmat_nan_sort_key(tmp_path, monkeypatch, direction):
    """A FLOAT sort key containing NaN. draken sorts NaN highest regardless of sign
    (`sort_num_key` -> UINT64_MAX), so it is the best DESC key and the worst ASC
    key — the opposite corner from NULL, which sorts lowest in both directions.
    Mirrors `test_latmat_nulls_and_values_mixed_ascending`'s shape (more than n
    non-null-and-non-NaN survivors, so a boundary genuinely has to be found)."""
    keys = []
    seen = 0
    for i in range(N):
        if not _MATCH[i]:
            keys.append(500000.0 + i)
        elif seen < 3:
            keys.append(float("nan"))
            seen += 1
        else:
            keys.append(1000.0 + seen)
            seen += 1
    _assert_latmat_parity(
        tmp_path, "nan_key" + direction.strip(), _dataset(keys, sort_type=pa.float64()),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k" + direction
        + " LIMIT 10", monkeypatch)


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


def test_trampoline_apply_topn_keeps_null_rows_ascending(tmp_path, monkeypatch):
    """Regression test for the `_apply_topn` NULL-ordering fix (parquet_read.pyx).

    `_apply_topn` used to hard-code "NULLs sort last" in BOTH directions and drop
    every NULL survivor once more than n non-null survivors existed. draken sorts
    NULL BELOW every value, so ascending they sort FIRST — they are exactly the rows
    that belong at the top of the answer, and the trampoline was returning rows the
    un-pushed plan does not (this fixture: `[1003…1012]` instead of
    `[NULL, NULL, NULL, 1003…1009]`).

    The parity helper above already runs the trampoline arm across the whole matrix;
    this pins the specific shape the bug was found on, by VALUE, so a regression
    cannot hide behind a coincidentally-matching key multiset."""
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

    tramp_rows, _, tramp_src = _drain(sql, latmat=True, monkeypatch=monkeypatch,
                                      trampoline=True)
    assert tramp_src == ["StreamingScanSource"], tramp_src

    # The three NULL-key rows are the three BEST rows ascending, so all three must
    # be in a LIMIT 10 answer — on both paths.
    assert sum(1 for r in ref_rows if r[ki] == "None") == 3
    assert sum(1 for r in tramp_rows if r[ki] == "None") == 3, (
        "_apply_topn dropped NULL survivors again — ascending, NULL is the BEST "
        "key (draken SortKeyCmp orders NULL below every value)")
    assert sorted(tramp_rows) == sorted(ref_rows)


def test_trampoline_apply_topn_keeps_nan_rows(tmp_path, monkeypatch):
    """Regression test for the `_apply_topn` NaN-ordering fix (parquet_read.pyx).

    `_apply_topn` compared candidate values against the boundary with plain
    `<=`/`>=`, which Python defines as False for EVERY comparison involving NaN.
    A NaN survivor therefore always failed the boundary test regardless of
    direction, AND the initial `nlargest`/`nsmallest` boundary selection itself
    saw the same broken comparisons — so this wasn't just "NaN rows get dropped",
    it corrupted the whole reduction. Observed on this exact fixture: an ASC
    top-10 over 3 NaN keys + several thousand real ones collapsed to a SINGLE
    returned row.

    The parity helper above already runs the trampoline arm across the whole
    matrix (including `test_latmat_nan_sort_key`); this pins the specific shape
    the bug was found on, by VALUE, the same way the NULL regression test does."""
    keys = []
    seen = 0
    for i in range(N):
        if not _MATCH[i]:
            keys.append(500000.0 + i)
        elif seen < 3:
            keys.append(float("nan"))
            seen += 1
        else:
            keys.append(1000.0 + seen)
            seen += 1
    path = _write(os.path.join(str(tmp_path), "tramp_nan"),
                  _dataset(keys, sort_type=pa.float64()))
    sql = f"SELECT * FROM '{path}' WHERE tag LIKE '{NEEDLE}' ORDER BY k LIMIT 10"

    ref_rows, names, ref_src = _drain(sql, latmat=False, monkeypatch=monkeypatch)
    assert ref_src == ["NativeParquetScanSource"], ref_src
    ki = names.index("k")

    tramp_rows, _, tramp_src = _drain(sql, latmat=True, monkeypatch=monkeypatch,
                                      trampoline=True)
    assert tramp_src == ["StreamingScanSource"], tramp_src

    assert len(ref_rows) == 10
    assert sum(1 for r in ref_rows if r[ki] == "nan") == 0, (
        "fixture assumption broken: ascending, NaN is the WORST key and should "
        "not appear in a top-10 unless fewer than 10 non-NaN survivors exist")
    assert len(tramp_rows) == 10, (
        f"_apply_topn corrupted the reduction on a NaN sort key again — expected "
        f"10 rows, got {len(tramp_rows)}: {tramp_rows}")
    assert sorted(tramp_rows) == sorted(ref_rows)


# --------------------------------------------------------------------------------
# The pass-1 predicate push, and the type tag it runs under.
#
# A parquet column declared `binary` with no UTF8 annotation binds VARBINARY, not
# VARCHAR — which is how the ClickBench `hits` files as downloaded declare `URL`. The
# worker-side push used to refuse that outright, so the whole predicate ran serially
# on the pass-1 thread while the decode workers idled (ClickBench Q24: 2.5s at 3.4x
# parallelism, vs 0.9s at 9.9x once admitted). It is admitted now, and the tag the
# predicate runs under is stamped from the plan rather than inferred from the decoded
# buffers (Pass1PredCtx.col_type) — because VARCHAR and VARBINARY share a byte layout
# but not their semantics, so inferring is how a fast path becomes a wrong one.
# --------------------------------------------------------------------------------


def _binary_dataset(sort_values):
    """The standard fixture with the PREDICATE column declared parquet `binary`,
    which binds VARBINARY."""
    cols = {"tag": (pa.binary(), [t.encode("utf-8") for t in _tags()]),
            "k": (pa.int64(), sort_values)}
    cols.update(_payload_columns())
    return cols


def test_latmat_varbinary_predicate_column(tmp_path, monkeypatch):
    """A VARBINARY predicate column answers exactly what the un-pushed plan does."""
    _assert_latmat_parity(
        tmp_path, "varbinary_pred", _binary_dataset([N - i for i in range(N)]),
        "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k LIMIT 10",
        monkeypatch)


def test_varbinary_predicate_is_pushed_to_the_workers(tmp_path, monkeypatch):
    """...and it reaches the workers, rather than passing the parity test by quietly
    running on the serial fallback. The gate is the only guard on the push, so a
    True return from it IS the push."""
    from opteryx.managers.execution import compiler as _compiler
    from opteryx.connectors.parquet_io import pass1_predicate_gate as _gate

    verdicts = []
    real = _gate.pass1_worker_predicate_admissible

    def spy(column_types):
        types = list(column_types)
        out = real(types)
        verdicts.append((tuple(str(t.physical) for t in types if t is not None), out))
        return out

    monkeypatch.setattr(_gate, "pass1_worker_predicate_admissible", spy)
    monkeypatch.setattr(_compiler, "pass1_worker_predicate_admissible", spy,
                        raising=False)

    path = _write(os.path.join(str(tmp_path), "varbinary_push"),
                  _binary_dataset([N - i for i in range(N)]))
    sql = (f"SELECT * FROM '{path}' WHERE tag LIKE '{NEEDLE}' ORDER BY k LIMIT 10")
    _rows, _names, src = _drain(sql, latmat=True, monkeypatch=monkeypatch)

    assert src == ["LatmatScanSource"], f"not exercising the latmat scan at all: {src}"
    assert verdicts, "the push gate was never consulted — the predicate was not pushed"
    assert all(v for _types, v in verdicts), (
        f"a VARBINARY predicate column was refused the worker push: {verdicts}")


def test_pass1_gate_admits_descriptor_free_types_and_refuses_the_rest():
    """The gate's rule, stated directly: a type whose whole meaning is its DrakenVector
    tag may be pushed (the eval entry stamps the plan's tag on the view); a type that
    carries a logical descriptor alongside the column may not, at any tag."""
    from draken.draken_native import DrakenType

    from opteryx.connectors.parquet_io.pass1_predicate_gate import (
        pass1_worker_predicate_admissible,
    )
    from opteryx.types.logical_type import DECIMAL, TIMESTAMP, ColumnType
    from draken.draken_native import TimestampUnit

    varchar = ColumnType(physical=DrakenType.VARCHAR)
    varbinary = ColumnType(physical=DrakenType.VARBINARY)
    int64 = ColumnType(physical=DrakenType.INT64)

    assert pass1_worker_predicate_admissible([varbinary])
    assert pass1_worker_predicate_admissible([varbinary, varchar, int64])

    # An untyped column has no tag to stamp — fail closed.
    assert not pass1_worker_predicate_admissible([None])
    assert not pass1_worker_predicate_admissible([varbinary, None])

    # Descriptor-carrying types stay out: scale and unit live outside the vector.
    for descriptor_carrying in (DECIMAL(18, 4), DECIMAL(30, 4), TIMESTAMP(TimestampUnit.SECONDS)):
        assert not pass1_worker_predicate_admissible([descriptor_carrying])
        assert not pass1_worker_predicate_admissible([varbinary, descriptor_carrying])


if __name__ == "__main__":  # pragma: no cover
    import pytest as _p
    raise SystemExit(_p.main([__file__, "-q"]))
