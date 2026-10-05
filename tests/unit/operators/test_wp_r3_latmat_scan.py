"""R3 (`fused_topn`) — the composed `WHERE ... ORDER BY ... LIMIT` scan shape, run
natively by the two-pass late-materialization Source.

The scan carries a `topn_sort_name`/`topn_limit` hint from `TopNScanPushdownStrategy`
AND a pushed predicate, so a decode-skip is genuinely load-bearing (ClickBench Q24
decodes only `URL` + `EventTime` for the whole table, then ~100 more columns for the
handful of LIKE survivors). `LatmatScanSource`
(src/cpp/engine/native_latmat_scan_source.hpp) does that natively:

    pass 1  decode predicate columns + sort key -> survivor bitmap per row group
    reduce  find the LIMIT boundary in the sort key across ALL row groups; drop every
            survivor strictly worse than it (n rows plus ties at the boundary)
    pass 2  decode the remaining projected columns, masked to those rows only

**The oracle.** Every fixture is written from plain Python lists, so the expected
answer is computed from those same lists in plain Python — WHERE, ORDER BY and LIMIT
evaluated by `_oracle` below, independent of any engine code path. The ordering rules
it encodes are the engine's documented ones, cited where they come from:

  * NULL is the LOWEST value: ASC -> NULLS FIRST, DESC -> NULLS LAST, unless an
    explicit NULLS FIRST/LAST is given (ruling 2026-09-27; resolved once in
    `sort_nulls_first`, logical_planner_builders.py; carried as `sort_nulls_first`
    on LatmatScanSource).
  * NaN sorts HIGHEST regardless of sign (`sort_num_key` in draken/morsels/sort.hpp:
    `if (d != d) return UINT64_MAX;`), and -0.0 is canonicalized to 0.0 there, so
    the two tie.
  * Strings order by byte (memcmp) collation — identical to Python `str` ordering for
    the ASCII values used here.

**What is compared, and why.** `ORDER BY ... LIMIT n` over a tie block wider than the
cut has no defined answer beyond "n rows, and every row at least as good as the n-th"
— WHICH tied rows come back is unspecified. So each case asserts what SQL promises:

  1. the row COUNT is min(n, survivors),
  2. the SORT-KEY SEQUENCE, in emission order, equals the oracle's exactly (this is
     fully determined, ties or not),
  3. every returned row is a whole, real survivor row and none is returned twice — so
     a two-pass zip that pairs one row's key with another row's payload fails even
     when the count and keys look right. Where a key is unique among survivors, 2+3
     pin the exact row.
"""

import math
import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pyarrow as pa  # test-only dep, used to WRITE parquet only
import pyarrow.parquet as pq
import pytest

import opteryx
import opteryx.config as config

# The predicate every case pushes. Matching rows are 1-in-4, which is selective
# enough to clear the late-materialization selectivity gate.
NEEDLE = "%pick%"


def _write(dataset_dir, columns, row_group_size=250):
    os.makedirs(dataset_dir, exist_ok=True)
    arrays = {name: pa.array(vals, type=typ) for name, (typ, vals) in columns.items()}
    pq.write_table(pa.table(arrays), os.path.join(dataset_dir, "part.parquet"),
                   row_group_size=row_group_size)
    return dataset_dir


def _norm(value):
    """Canonical cell for comparison: NaN != NaN in Python, so it becomes a
    sentinel; -0.0 becomes 0.0 (draken canonicalizes it, see module docstring)."""
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if value == 0.0:
            return 0.0
    return value


def _drain(sql, latmat, monkeypatch):
    """Run `sql`; return (rows, names, scan_sources). `rows` are
    tuples of normalized Python values in EMISSION order."""
    if not latmat:
        monkeypatch.setattr(config.features, "parquet_late_materialization", False)
    session = opteryx.session()
    rows = []
    names = []
    for morsel in session.execute_to_morsels(sql):
        raw = list(morsel.column_names)   # bytes at the native boundary
        names = [n.decode("utf-8") if isinstance(n, bytes) else n for n in raw]
        cols = [morsel.column(n) for n in raw]
        for i in range(morsel.num_rows):
            rows.append(tuple(_norm(c[i]) for c in cols))
    telemetry = session.telemetry
    src = list(telemetry["scan_sources"].values())
    if not latmat:
        monkeypatch.undo()
    return rows, names, src


def _sort_rank(value):
    """Rank of a NON-NULL key: NaN above every number (draken `sort_num_key`)."""
    if isinstance(value, float) and math.isnan(value):
        return (1, 0.0)
    return (0, value)


def _oracle(columns, where, key_column, descending, nulls_first, limit):
    """Plain-Python WHERE / ORDER BY / LIMIT over the written values.

    Returns (survivor_rows, expected_key_sequence), both normalized. `nulls_first`
    None means the engine default: NULL lowest, i.e. first ASC, last DESC."""
    names = list(columns)
    n_rows = len(columns[names[0]][1])
    survivors = []
    for i in range(n_rows):
        row = {name: columns[name][1][i] for name in names}
        if where(row):
            survivors.append(row)
    if nulls_first is None:
        nulls_first = not descending
    keys = [r[key_column] for r in survivors]
    non_null = sorted((k for k in keys if k is not None), key=_sort_rank,
                      reverse=descending)
    nulls = [None] * (len(keys) - len(non_null))
    ordered = nulls + non_null if nulls_first else non_null + nulls
    survivor_rows = [tuple(_norm(r[name]) for name in names) for r in survivors]
    return survivor_rows, [_norm(k) for k in ordered[:limit]]


def _assert_latmat_matches_oracle(tmp_path, name, columns, where, monkeypatch, *,
                                  sql_where, descending=False, nulls=None,
                                  limit=10, key_column="k", row_group_size=250):
    """Write `columns`, run `SELECT * ... WHERE {sql_where} ORDER BY k ... LIMIT n`
    natively, and assert the three properties in this module's docstring against
    the plain-Python oracle. `where` is the Python equivalent of `sql_where`;
    `nulls` is None (engine default), "FIRST" or "LAST"."""
    path = _write(os.path.join(str(tmp_path), name), columns,
                  row_group_size=row_group_size)
    order = key_column + (" DESC" if descending else "")
    if nulls is not None:
        order += " NULLS " + nulls
    sql = f"SELECT * FROM '{path}' WHERE {sql_where} ORDER BY {order} LIMIT {limit}"
    rows, names, src = _drain(sql, latmat=True, monkeypatch=monkeypatch)

    assert src == ["LatmatScanSource"], (
        f"{name}: expected the two-pass late-mat Source, got {src} — this case is "
        "not exercising R3 at all")
    assert names == list(columns), f"{name}: output column layout differs: {names}"

    nulls_first = None if nulls is None else (nulls == "FIRST")
    survivor_rows, expect_keys = _oracle(columns, where, key_column, descending,
                                         nulls_first, limit)
    k = names.index(key_column)
    assert len(rows) == len(expect_keys), (
        f"{name}: row COUNT {len(rows)}, oracle {len(expect_keys)}")
    got_keys = [r[k] for r in rows]
    assert got_keys == expect_keys, (
        f"{name}: sort-key sequence differs from the oracle\n"
        f"  got:    {got_keys}\n  oracle: {expect_keys}")
    universe = set(survivor_rows)
    stray = [r for r in rows if r not in universe]
    assert not stray, (
        f"{name}: {len(stray)} returned row(s) are not survivor rows of the table — "
        f"the pass-1/pass-2 zip is misaligned. First: {stray[0]}")
    assert len(set(rows)) == len(rows), f"{name}: a survivor row was returned twice"
    return rows, names


def _tag_matches(row):
    """Python equivalent of `tag LIKE '%pick%'` (str or bytes tag)."""
    tag = row["tag"]
    return ("pick" in tag) if isinstance(tag, str) else (b"pick" in tag)


_LIKE = "tag LIKE '" + NEEDLE + "'"


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
    _assert_latmat_matches_oracle(
        tmp_path, "asc_unique", _dataset([N - i for i in range(N)]), _tag_matches,
        monkeypatch, sql_where=_LIKE)


def test_latmat_descending_unique_keys(tmp_path, monkeypatch):
    _assert_latmat_matches_oracle(
        tmp_path, "desc_unique", _dataset([N - i for i in range(N)]), _tag_matches,
        monkeypatch, sql_where=_LIKE, descending=True)


@pytest.mark.parametrize("descending", [False, True])
def test_latmat_ties_span_the_boundary(tmp_path, monkeypatch, descending):
    """A tie block sitting exactly ON the n-th best value, spread across many row
    groups. The reduction must keep the WHOLE tie block (dropping part of it can
    change which n rows the downstream TopNSink finally keeps)."""
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
    _assert_latmat_matches_oracle(
        tmp_path, "ties_boundary" + ("_desc" if descending else ""), _dataset(keys),
        _tag_matches, monkeypatch, sql_where=_LIKE, descending=descending)


def test_latmat_all_null_sort_key(tmp_path, monkeypatch):
    """Every survivor's sort key is NULL — one giant tie block, both directions
    equivalent. Nothing may be dropped for being 'worse' than a NULL."""
    keys = [None if m else 5 for m in _MATCH]
    _assert_latmat_matches_oracle(
        tmp_path, "all_null", _dataset(keys), _tag_matches, monkeypatch,
        sql_where=_LIKE)


@pytest.mark.parametrize("descending", [False, True])
def test_latmat_fewer_non_null_than_n(tmp_path, monkeypatch, descending):
    """Fewer than n NON-NULL survivors, so NULL rows have to enter the answer —
    ascending they are the BEST key (NULL lowest) and descending they are the worst
    but still needed to fill n."""
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
    _assert_latmat_matches_oracle(
        tmp_path, "few_nonnull" + ("_desc" if descending else ""), _dataset(keys),
        _tag_matches, monkeypatch, sql_where=_LIKE, descending=descending)


def test_latmat_nulls_and_values_mixed_ascending(tmp_path, monkeypatch):
    """MORE than n non-null survivors AND some NULLs. Ascending, the NULLs are the
    best rows and must all survive the reduction."""
    keys = []
    seen = 0
    for i in range(N):
        if not _MATCH[i]:
            keys.append(500000 + i)
        else:
            keys.append(None if seen < 3 else 1000 + seen)
            seen += 1
    rows, names = _assert_latmat_matches_oracle(
        tmp_path, "mixed_nulls_asc", _dataset(keys), _tag_matches, monkeypatch,
        sql_where=_LIKE)
    # Stated by value as well: the three NULL-key rows are in the answer.
    assert sum(1 for r in rows if r[names.index("k")] is None) == 3


def test_latmat_limit_larger_than_survivor_count(tmp_path, monkeypatch):
    """N above the number of surviving rows — no boundary exists, so every survivor
    must reach pass 2. (`nth_element` is never called on this path.)"""
    _assert_latmat_matches_oracle(
        tmp_path, "big_limit", _dataset([i for i in range(N)]), _tag_matches,
        monkeypatch, sql_where=_LIKE, limit=900)


def test_latmat_string_sort_key(tmp_path, monkeypatch):
    """A VARCHAR sort key: the reduction takes draken's string key path (pointer +
    length, memcmp collation) rather than the normalized-uint64 one, and long values
    live in the arena the pass-1 morsels must keep alive across the barrier."""
    keys = [None if (i % 400 == 0) else ("key-%06d-and-a-long-tail-value" % (N - i))
            for i in range(N)]
    _assert_latmat_matches_oracle(
        tmp_path, "string_key", _dataset(keys, sort_type=pa.string()), _tag_matches,
        monkeypatch, sql_where=_LIKE)


def test_latmat_float_sort_key(tmp_path, monkeypatch):
    """A FLOAT64 sort key, including negatives and -0.0 — draken's normalized float
    key is order-preserving across the sign bit, which a naive bit compare is not."""
    keys = [None if (i % 500 == 0) else (float(i) - 1500.0) * 0.25 for i in range(N)]
    keys[4] = -0.0
    keys[8] = 0.0
    _assert_latmat_matches_oracle(
        tmp_path, "float_key", _dataset(keys, sort_type=pa.float64()), _tag_matches,
        monkeypatch, sql_where=_LIKE, descending=True)


@pytest.mark.parametrize("descending", [False, True])
def test_latmat_nan_sort_key(tmp_path, monkeypatch, descending):
    """A FLOAT sort key containing NaN. draken sorts NaN highest regardless of sign
    (`sort_num_key` -> UINT64_MAX), so it is the best DESC key and the worst ASC
    key — the opposite corner from NULL. More than n non-NaN survivors, so a
    boundary genuinely has to be found: ASC returns no NaN, DESC returns all 3
    first."""
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
    rows, names = _assert_latmat_matches_oracle(
        tmp_path, "nan_key" + ("_desc" if descending else ""),
        _dataset(keys, sort_type=pa.float64()), _tag_matches, monkeypatch,
        sql_where=_LIKE, descending=descending)
    k = names.index("k")
    assert sum(1 for r in rows if r[k] == "NaN") == (3 if descending else 0)


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
    cols = {"k": (pa.int64(), [i for i in range(N)])}
    cols.update(_payload_columns())
    _assert_latmat_matches_oracle(
        tmp_path, "same_col", cols, lambda row: row["k"] < 900, monkeypatch,
        sql_where="k < 900", descending=True)


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
        assert tag == "pick-%d" % i
        assert pay_str == "payload-%d-long-enough-to-live-in-the-arena" % i
        assert pay_f64 == float(i) * 1.5
        assert pay_i64 == i * 7
        assert pay_bool == (i % 3 == 0)


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
    """A VARBINARY predicate column answers exactly what the oracle does."""
    _assert_latmat_matches_oracle(
        tmp_path, "varbinary_pred", _binary_dataset([N - i for i in range(N)]),
        _tag_matches, monkeypatch, sql_where=_LIKE)


def test_varbinary_predicate_is_pushed_to_the_workers(tmp_path, monkeypatch):
    """...and it reaches the workers, rather than passing the oracle test by quietly
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


# --------------------------------------------------------------------------------
# Explicit NULLS FIRST / NULLS LAST.
# --------------------------------------------------------------------------------


def _null_placement_keys():
    """Nullable sort key with MORE than n non-null survivors AND more than n NULL
    survivors, spread over every row group — so the placement alone decides whether
    the top 10 is all NULLs or all values, and a reduction that ranks NULL on the
    wrong end returns a different key sequence."""
    keys = []
    for i in range(N):
        if not _MATCH[i]:
            keys.append(7)
        elif (i // 4) % 2 == 0:
            keys.append(None)
        else:
            keys.append(10000 + i)
    return keys


@pytest.mark.parametrize(
    "descending, nulls, expect_nulls",
    [
        (False, None, 10),      # default ASC: NULL lowest -> first
        (False, "FIRST", 10),
        (False, "LAST", 0),
        (True, None, 0),        # default DESC: NULL lowest -> last
        (True, "LAST", 0),
        (True, "FIRST", 10),
    ],
)
def test_latmat_explicit_null_placement(tmp_path, monkeypatch, descending, nulls,
                                        expect_nulls):
    """ORDER BY <nullable> [ASC|DESC] [NULLS FIRST|LAST] LIMIT n through the native
    LatmatScanSource against the oracle — and by VALUE, so an oracle and engine that
    agreed on the wrong placement could not pass on the oracle alone."""
    name = "nulls_%s_%s" % ("desc" if descending else "asc", nulls or "default")
    rows, names = _assert_latmat_matches_oracle(
        tmp_path, name, _dataset(_null_placement_keys()), _tag_matches, monkeypatch,
        sql_where=_LIKE, descending=descending, nulls=nulls)
    k = names.index("k")
    assert len(rows) == 10
    assert sum(1 for r in rows if r[k] is None) == expect_nulls, (
        (descending, nulls), [r[k] for r in rows])


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
