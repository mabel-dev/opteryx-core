"""NativeSkeneScanSource's reader-side predicate filter.

Architect ruling, 2026-08-21: skene ACCEPTS predicate pushdown, and the predicate
is evaluated inside the Source's decode workers. `FileSystemTable.can_push`
accepting means `PredicatePushdownStrategy` CONSUMES the Filter node, so this
Source is the only thing that applies the predicate — a row it wrongly keeps or
drops is a wrong answer with nothing downstream to catch it.

The reference arm is the SAME query with `disable_predicate_pushdown` on: the
Filter node survives, the scan reads everything, and the engine's ExprFilter does
the work. That is the un-pushed ground truth, and it is the plan every skene scan
ran before this landed.

What each assertion is protecting:

  * answers match the un-pushed arm — the whole point, and the only check that
    catches a predicate applied to the wrong column after the read-set
    permutation, or a conjunct silently not applied;
  * the pushed arm really is pushed (scan_sources + the pushdown telemetry), so
    a routing change cannot make these tests pass vacuously by quietly going back
    to a Filter node;
  * a PREDICATE-ONLY column does not leak out of the scan. The read set is
    projection ∪ predicate columns and the Source narrows it back itself
    (`emit_indices`), so the output column list is the projection — including the
    zero-column `COUNT(*) WHERE ...` shape, whose row count rides on
    zero_col_rows.

Fixture note: the predicate is evaluated per ROW GROUP, on whichever worker
claimed it, so the fixture is several row groups with survivors spread unevenly
across them — including row groups with NO survivors, which a single-row-group
fixture cannot express. They are packed two-files-worth interleaved, so that
plan-time FILE pruning cannot stand in for what the reader does; see
`_write_skene`.
"""

import decimal
import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pyarrow as pa  # test-only dep (allowed in tests/)
import pyarrow.parquet as pq
import pytest

import opteryx
import opteryx.config as config

N = 1200
ROWS_PER_GROUP = 100
ROW_GROUP_COUNT = N // ROWS_PER_GROUP     # 12
FILE_COUNT = 2                            # row groups alternate between them


def _write_skene(dataset_dir, columns, rows_per_group=ROWS_PER_GROUP):
    """Write `columns` as a skene dataset of ROW_GROUPS_PER_FILE-packed files, with
    consecutive row groups landing in ALTERNATING files.

    The interleave is the whole point. The obvious fixture — one row group per file,
    values ascending across files — cannot test row-group skipping at all: plan-time
    manifest FILE pruning already drops exactly the same files, and a passing test
    would be measuring that instead. Alternating row groups between two files gives
    each FILE bounds spanning nearly the whole range, so file pruning has almost
    nothing to do and every skip observed here is the reader's own.

    Row groups are built by writing one parquet with `row_group_size=rows_per_group`
    and reading it back a morsel at a time — the same conversion dev/parquet_to_skene.py
    does, which is also where SkeneWriter.add_row_group packing comes from.
    """
    import skene
    from rugo.parquet import read_parquet

    os.makedirs(dataset_dir, exist_ok=True)
    parquet_path = os.path.join(dataset_dir, "_source.parquet")
    arrays = {name: pa.array(values, type=typ) for name, (typ, values) in columns.items()}
    pq.write_table(pa.table(arrays), parquet_path, row_group_size=rows_per_group)
    writers = [skene.SkeneWriter(read_acceleration=True, codec="none", zstd_level=0)
               for _ in range(FILE_COUNT)]
    with read_parquet(parquet_path) as reader:
        for index, morsel in enumerate(reader):
            writers[index % FILE_COUNT].add_row_group(morsel)
    for index, writer in enumerate(writers):
        writer.write_to(os.path.join(dataset_dir, "part-%04d.skene" % index))
    # The scan reader is chosen from the dataset's file format and a dataset is
    # single-format: leaving the parquet behind would make this a MIXED manifest.
    os.remove(parquet_path)
    return dataset_dir


def _columns():
    """A column of each pushable shape, plus a payload nothing filters on.

    `k` is 0..1199 in row-group order, so row group r covers exactly
    [100r, 100r+99] — disjoint, contiguous, and predictable, which is what lets the
    zone-map tests below assert an exact skip count instead of "some".

    `bucket` is `k // 100`, i.e. CONSTANT within a row group. That is the degenerate
    and most valuable case for a zone map: one row group can hold every matching
    row, and the other eleven are provably empty.

    `amount` is `k / 100` as DECIMAL(12,2), so it shares `k`'s disjoint layout —
    the decimal zone-map tests need bounds they can predict exactly.

    `tag` and `maybe` are deliberately uncorrelated with row-group position, so a
    predicate on them prunes nothing and exercises the filter rather than the map.
    """
    return {
        "k": (pa.int64(), list(range(N))),
        "bucket": (pa.int64(), [i // ROWS_PER_GROUP for i in range(N)]),
        "tag": (pa.string(), ["pick-%d" % i if i % 4 == 0 else "skip-%d" % i
                              for i in range(N)]),
        # DECIMAL(12,2) as i/100, so row group r spans exactly [r.00, r.99] —
        # disjoint per row group, like `k`, which is what makes an exact skip count
        # assertable for the decimal zone-map tests below.
        "amount": (pa.decimal128(12, 2),
                   [decimal.Decimal(i).scaleb(-2) for i in range(N)]),
        "flag": (pa.bool_(), [i % 3 == 0 for i in range(N)]),
        "maybe": (pa.int64(), [None if i % 5 == 0 else i for i in range(N)]),
        "payload": (pa.string(), ["payload-%d" % i for i in range(N)]),
        # Four more payload columns nothing filters on. Their only job is to take a
        # `SELECT *` past `skene_late_materialization_min_deferred_columns` (8), so
        # this one fixture can also exercise the two-pass Source's zone-map pruning
        # without duplicating the interleaved packing `_write_skene` does.
        **{"p%d" % j: (pa.string(), ["p%d-%d" % (j, i) for i in range(N)])
           for j in range(4)},
    }


def _drain(sql, pushdown, monkeypatch):
    """(rows, names, scan_sources, pushed_count, scan_facts) for `sql`. `pushdown`
    False turns PredicatePushdownStrategy off, which is the un-pushed reference arm.
    `scan_facts` is the single scan's native_scan_facts entry, or None when the plan
    has anything other than exactly one native scan."""
    if not pushdown:
        monkeypatch.setattr(config.features, "disable_predicate_pushdown", True)
    session = opteryx.session()
    rows = []
    names = []
    for morsel in session.execute_to_morsels(sql):
        raw = list(morsel.column_names)   # bytes at the native boundary
        names = [n.decode("utf-8") if isinstance(n, bytes) else n for n in raw]
        if not raw:
            # The genuine zero-column shape: the row count is the answer.
            rows.append(("<zero-col>", morsel.num_rows))
            continue
        for i in range(morsel.num_rows):
            rows.append(tuple(repr(morsel.column(n)[i]) for n in raw))
    reading = session.telemetry
    sources = sorted((reading.get("scan_sources") or {}).values())
    pushed = reading.get("optimization_predicate_pushdown_into_scan") or 0
    facts = list((session._telemetry._reading.get("native_scan_facts") or {}).values())
    if not pushdown:
        monkeypatch.undo()
    return rows, names, sources, pushed, (facts[0] if len(facts) == 1 else None)


@pytest.fixture(scope="module")
def dataset(tmp_path_factory):
    return _write_skene(str(tmp_path_factory.mktemp("skene_pushdown")), _columns())


def _parity(dataset, sql_tail, monkeypatch, expect_source="NativeSkeneScanSource"):
    sql = "SELECT " + sql_tail.format(DATASET="'%s'" % dataset)
    ref_rows, ref_names, ref_sources, ref_pushed, _ref_facts = _drain(sql, False, monkeypatch)
    got_rows, got_names, got_sources, got_pushed, got_facts = _drain(sql, True, monkeypatch)

    assert ref_pushed == 0, (
        "the reference arm pushed %d predicate(s) — pushdown was not actually "
        "disabled, so this is not an un-pushed ground truth" % ref_pushed)
    assert got_pushed > 0, (
        "nothing was pushed into the scan for %r — this case is not exercising "
        "the reader-side filter at all" % sql)
    assert got_sources == [expect_source], (
        "expected %s, got %s" % (expect_source, got_sources))
    assert got_names == ref_names, "output column layout differs from the un-pushed plan"
    assert sorted(got_rows) == sorted(ref_rows), (
        "the pushed plan disagrees with the un-pushed plan — nothing above this "
        "scan re-applies the predicate, so this is a wrong answer\n"
        "  pushed  : %d rows\n  un-pushed: %d rows" % (len(got_rows), len(ref_rows)))
    return got_rows, got_names, got_facts


def test_integer_comparison(dataset, monkeypatch):
    _parity(dataset, "k, payload FROM {DATASET} WHERE k > 900", monkeypatch)


def test_string_equality(dataset, monkeypatch):
    _parity(dataset, "k FROM {DATASET} WHERE tag = 'pick-8'", monkeypatch)


def test_string_like(dataset, monkeypatch):
    _parity(dataset, "k FROM {DATASET} WHERE tag LIKE 'pick-1%'", monkeypatch)


def test_decimal_comparison(dataset, monkeypatch):
    """DECIMAL is in SKENE_PUSHABLE_TYPES and not in the shared PUSHABLE_TYPES.
    Without it a mixed WHERE splits into a pushed program plus a residual Filter —
    two passes where one ran — which is the mechanism behind TPC-H Q06's measured
    loss on the plan-level-acceptance prototype."""
    _parity(dataset, "k, amount FROM {DATASET} WHERE amount > 4.25", monkeypatch)


def test_mixed_types_push_as_one_program(dataset, monkeypatch):
    """Every conjunct of a mixed INTEGER + DECIMAL + VARCHAR WHERE pushes, so no
    residual Filter is left behind. This is the "do not ship the split" case."""
    sql = ("SELECT k FROM '%s' WHERE k > 100 AND amount < 40.00 AND tag LIKE 'pick%%'"
           % dataset)
    _rows, _names, _sources, pushed, _facts = _drain(sql, True, monkeypatch)
    assert pushed == 3, (
        "expected all three conjuncts pushed into the scan, got %d — a partial "
        "push leaves a residual Filter and runs two filter passes" % pushed)


def test_boolean_column(dataset, monkeypatch):
    # `WHERE flag` alone is refused by the binder (bare column name) and
    # `IS TRUE` is not in PUSHABLE_OPS, so the pushable spelling of a boolean
    # predicate is the explicit comparison.
    _parity(dataset, "k FROM {DATASET} WHERE flag = true", monkeypatch)


def test_is_null_and_is_not_null(dataset, monkeypatch):
    _parity(dataset, "k FROM {DATASET} WHERE maybe IS NULL", monkeypatch)
    _parity(dataset, "k FROM {DATASET} WHERE maybe IS NOT NULL", monkeypatch)


def test_in_list(dataset, monkeypatch):
    _parity(dataset, "k FROM {DATASET} WHERE bucket IN (0, 3, 11)", monkeypatch)


def test_between(dataset, monkeypatch):
    _parity(dataset, "k FROM {DATASET} WHERE k BETWEEN 250 AND 260", monkeypatch)


def test_whole_row_groups_have_no_survivors(dataset, monkeypatch):
    """`bucket = 0` lives only in the first file, so ten of the twelve row groups
    filter down to nothing. A Source that emits a zero-row morsel instead of
    claiming the next work item, or that treats an empty result as end-of-scan,
    fails here and passes on a fixture where every row group survives."""
    rows, _names, _facts = _parity(dataset, "k FROM {DATASET} WHERE bucket = 0", monkeypatch)
    assert len(rows) == 100


def test_no_rows_survive_anywhere(dataset, monkeypatch):
    """Every row group decodes and every row fails. Deliberately an UNANCHORED LIKE:
    a bounds-comparable predicate (`k > 100000`) is pruned away at plan time by
    ManifestPruningStrategy, leaving an empty relation and no scan to test."""
    rows, _names, _facts = _parity(dataset, "k FROM {DATASET} WHERE tag LIKE '%zzz%'",
                           monkeypatch)
    assert rows == []


def test_every_row_survives(dataset, monkeypatch):
    rows, _names, _facts = _parity(dataset, "k FROM {DATASET} WHERE k >= 0", monkeypatch)
    assert len(rows) == N


def test_predicate_only_column_does_not_leak(dataset, monkeypatch):
    """`tag` is filtered on and NOT projected. The scan reads it (it cannot filter
    otherwise) and must drop it again before emitting — the read set is an internal
    detail, and a leaked column changes the result schema."""
    _rows, names, _facts = _parity(dataset, "k, payload FROM {DATASET} WHERE tag = 'pick-4'",
                           monkeypatch)
    assert len(names) == 2, "the scan emitted %r — the predicate-only column leaked" % names


def test_count_star_with_predicate(dataset, monkeypatch):
    """Zero projection plus a predicate: the read set is the predicate's columns
    alone and the emit set is EMPTY, so the Source has to produce a genuine
    zero-column morsel whose (post-filter) row count rides on zero_col_rows."""
    rows, names, _facts = _parity(dataset, "COUNT(*) FROM {DATASET} WHERE tag LIKE 'pick%'",
                          monkeypatch)
    assert names == ["COUNT(*)"]
    assert rows == [("300",)]


def test_aggregate_over_predicate_only_column(dataset, monkeypatch):
    _parity(dataset, "bucket, COUNT(*), SUM(k) FROM {DATASET} WHERE tag LIKE 'pick%' "
                     "GROUP BY bucket ORDER BY bucket", monkeypatch)


def test_disjunction_is_not_pushed_and_stays_correct(dataset, monkeypatch):
    """An OR is NOT pushable today: `PredicatePushable.can_push` admits only the
    simple node kinds, and a disjunction is not one of them. It therefore stays a
    Filter above the scan — a missed optimization, not a dropped predicate, which
    is exactly the property this asserts. Pinning it here means widening the gate
    to cover OR has to come past this test rather than silently."""
    sql = ("SELECT k FROM '%s' WHERE bucket = 0 OR k > 1150" % dataset)
    rows, _names, _sources, pushed, _facts = _drain(sql, True, monkeypatch)
    ref_rows, _ref_names, _ref_sources, _ref_pushed, _rf = _drain(sql, False, monkeypatch)
    assert pushed == 0
    assert sorted(rows) == sorted(ref_rows)
    assert len(rows) == 149


def test_order_by_limit_over_a_narrow_projection(dataset, monkeypatch):
    """Filtered top-N over a projection too narrow for the two-pass path: it stays
    on the single-pass Source, which applies the predicate itself."""
    _parity(dataset, "k FROM {DATASET} WHERE tag LIKE 'pick%' ORDER BY k LIMIT 10",
            monkeypatch)


# ── row-group zone maps ───────────────────────────────────────────────────────
#
# `k` runs 0..1199 in row-group order, so row group r covers exactly [100r, 100r+99]
# and the number of row groups a range predicate can possibly match is arithmetic.
# `bucket` is constant within a row group — the degenerate case, where one row group
# holds every matching row.
#
# The assertions are on `row_groups_read`, NOT on `row_groups_pruned`. Read is the
# invariant: it is the count of row groups that can contain a matching row, and it
# is the same number however the work was divided into files. Pruned is read
# subtracted from whatever survived plan-time FILE pruning, so it moves when file
# pruning happens to catch a file first — an assertion on it would be an assertion
# about two mechanisms at once. `pruned > 0` is still asserted, because a test that
# only checked `read` would pass if the reader skipped nothing and the files simply
# happened to be small.


def _pruning(dataset, where, monkeypatch):
    """(rows, facts) for `SELECT k FROM <dataset> WHERE <where>`, asserting the
    pushed answer matches the un-pushed one first — a skip count is only interesting
    once the answer is known to be right."""
    rows, _names, facts = _parity(dataset, "k FROM {DATASET} WHERE " + where, monkeypatch)
    assert facts is not None, "expected exactly one native scan in the plan"
    return rows, facts


def test_zone_map_skips_row_groups(dataset, monkeypatch):
    """`bucket` is constant per row group, so exactly one of the twelve can hold a
    `bucket = 7` row and the other eleven are never decoded."""
    rows, facts = _pruning(dataset, "bucket = 7", monkeypatch)
    assert len(rows) == 100
    assert facts["row_groups_read"] == 1, facts
    assert facts["row_groups_pruned"] > 0, facts


def test_zone_map_range_skips_the_row_groups_below_it(dataset, monkeypatch):
    """`k > 950` can only match row groups 9, 10 and 11 (900-999, 1000-1099,
    1100-1199); every row group below is excluded by its own max."""
    rows, facts = _pruning(dataset, "k > 950", monkeypatch)
    assert len(rows) == 249
    assert facts["row_groups_read"] == 3, facts
    assert facts["row_groups_pruned"] > 0, facts


def test_zone_map_conjunction_intersects(dataset, monkeypatch):
    """Two range terms are ANDed and each may exclude a row group on its own, so
    the survivors are the intersection: row groups 2 and 3."""
    rows, facts = _pruning(dataset, "k >= 250 AND k < 350", monkeypatch)
    assert len(rows) == 100
    assert facts["row_groups_read"] == 2, facts


def test_zone_map_between(dataset, monkeypatch):
    """BETWEEN lowers to a GtEq term AND an LtEq term, and both must reach the zone
    map — carrying only one half would leave every row group on the other side of
    the window unpruned, which `row_groups_read == 1` is what catches."""
    rows, facts = _pruning(dataset, "k BETWEEN 500 AND 599", monkeypatch)
    assert len(rows) == 100
    assert facts["row_groups_read"] == 1, facts


def test_zone_map_boundaries_are_inclusive_where_the_operator_is(dataset, monkeypatch):
    """Row group 1 holds k=100..199. It must SURVIVE `k >= 199` and `k <= 100` (its
    max and its min are matching rows) and be EXCLUDED by `k > 199` and `k < 100`.

    This is the assertion that catches an off-by-one in `skene_zone_excludes` —
    `<=` where `<` belongs drops a row group holding the boundary row. The parity
    check inside `_pruning` catches the wrong ANSWER; the read counts here say
    which side of the boundary went wrong.
    """
    rows, facts = _pruning(dataset, "k >= 199", monkeypatch)
    assert len(rows) == 1001 and facts["row_groups_read"] == 11, facts
    rows, facts = _pruning(dataset, "k > 199", monkeypatch)
    assert len(rows) == 1000 and facts["row_groups_read"] == 10, facts
    rows, facts = _pruning(dataset, "k <= 100", monkeypatch)
    assert len(rows) == 101 and facts["row_groups_read"] == 2, facts
    rows, facts = _pruning(dataset, "k < 100", monkeypatch)
    assert len(rows) == 100 and facts["row_groups_read"] == 1, facts


def test_zone_map_prunes_every_row_group(dataset, monkeypatch):
    """`bucket = 7 AND k < 100` is satisfiable by NO row group — bucket 7 lives in
    row group 7 and k < 100 lives in row group 0 — so nothing is decoded at all.

    Deliberately a conjunction whose two halves are individually satisfiable by the
    surviving FILE's bounds, so plan-time file pruning cannot do this on its own and
    an empty claim list is genuinely the reader's doing. It is also the shape that
    catches a Source treating "no work items" as an error rather than an empty
    answer.
    """
    rows, facts = _pruning(dataset, "bucket = 7 AND k < 100", monkeypatch)
    assert rows == []
    assert facts["row_groups_read"] == 0, facts
    assert facts["row_groups_pruned"] > 0, facts


def test_zone_map_declines_on_a_column_it_cannot_bound(dataset, monkeypatch):
    """An unanchored LIKE resolves to no zone term, so nothing is pruned and every
    row group is read — a missed skip, never a wrong answer. Pinned so a future
    term extractor cannot start pruning on a predicate it has not proved sound."""
    rows, facts = _pruning(dataset, "tag LIKE '%pick-3%'", monkeypatch)
    assert rows
    assert facts["row_groups_read"] == ROW_GROUP_COUNT, facts
    assert facts["row_groups_pruned"] == 0, facts


def test_reported_counts_are_the_run_time_ones(dataset, monkeypatch):
    """With no predicate there is nothing to prune, and the reported counts must be
    the ones the reader actually saw rather than the plan-time placeholder they
    start as — `_fold_skene_scan_facts` is what replaces them."""
    sql = "SELECT k FROM '%s'" % dataset
    rows, _names, _sources, pushed, facts = _drain(sql, True, monkeypatch)
    assert pushed == 0
    assert len(rows) == N
    assert facts["row_groups_read"] == ROW_GROUP_COUNT, facts
    assert facts["row_groups_pruned"] == 0, facts


# ── DECIMAL zone maps ─────────────────────────────────────────────────────────
#
# `amount` is DECIMAL(12,2) and `i / 100` — so row group r spans exactly
# [r.00, r.99] and, like `k`, its bounds are disjoint and predictable. skene stores
# a decimal column's bounds as the UNSCALED mantissa at the column's scale, which is
# why the literal has to be put on that same gridline before it is comparable; see
# `rescale_decimal_literal`.
#
# This was unreachable until 2026-08-21: `ColumnType.ordinalize` refused DECIMAL
# outright, so the columns TPC-H actually filters on got no pruning at all.


def test_decimal_zone_map_skips_row_groups(dataset, monkeypatch):
    """A decimal range prunes on the same footer bounds an integer range does."""
    rows, facts = _pruning(dataset, "amount >= 9.00", monkeypatch)
    assert len(rows) == 300
    assert facts["row_groups_read"] == 3, facts


def test_decimal_zone_map_boundary_is_exact(dataset, monkeypatch):
    """Row group 5 spans 5.00..5.99. It must survive `amount <= 5.00` (5.00 is its
    minimum) and be excluded by `amount < 5.00`.

    The mantissa conversion is what this pins: 5.00 at scale 2 is 500, and any
    literal-side scale error (500 read as 5, or as 50000) moves the boundary by two
    orders of magnitude and this is where it shows.
    """
    rows, facts = _pruning(dataset, "amount <= 5.00", monkeypatch)
    assert len(rows) == 501 and facts["row_groups_read"] == 6, facts
    rows, facts = _pruning(dataset, "amount < 5.00", monkeypatch)
    assert len(rows) == 500 and facts["row_groups_read"] == 5, facts


def test_decimal_zone_map_equality(dataset, monkeypatch):
    """An exact decimal equality reaches the zone map: only the row group whose
    bounds straddle 7.50 can hold it."""
    rows, facts = _pruning(dataset, "amount = 7.50", monkeypatch)
    assert len(rows) == 1
    assert facts["row_groups_read"] == 1, facts


def test_decimal_zone_map_integer_literal(dataset, monkeypatch):
    """A bare integer against a DECIMAL column is the TPC-H shape
    (`l_quantity < 24`) and must rescale, not be taken at face value: 3 at scale 2
    is 300, not 3."""
    rows, facts = _pruning(dataset, "amount < 3", monkeypatch)
    assert len(rows) == 300
    assert facts["row_groups_read"] == 3, facts


def test_decimal_zone_map_declines_an_off_gridline_equality(dataset, monkeypatch):
    """`amount = 7.505` cannot be put on a scale-2 gridline without changing which
    rows it matches, so no term is produced and nothing is pruned — a missed skip,
    never a wrong answer. This is the case `rescale_decimal_literal` returns None
    for, and the assertion that keeps a future rounding rule from quietly deciding
    an equality target for itself."""
    rows, facts = _pruning(dataset, "amount = 7.505", monkeypatch)
    assert rows == []
    assert facts["row_groups_read"] == ROW_GROUP_COUNT, facts
    assert facts["row_groups_pruned"] == 0, facts


def test_two_pass_source_prunes_row_groups_too(dataset, monkeypatch):
    """The two-pass late-materialization Source shares the claim builder, so it
    prunes the same row groups the single-pass Source does.

    Worth pinning separately: pass 1 sweeps EVERY row group it claims, so a
    two-pass plan that skipped zone-map pruning would do strictly more decode work
    than the single-pass plan it was chosen over — the optimization making the
    query slower. Pruning at claim time also covers pass 2 for free, because pass
    2's work items are drawn from pass 1's survivors.
    """
    rows, _names, facts = _parity(
        dataset, "* FROM {DATASET} WHERE bucket = 7 ORDER BY k LIMIT 10",
        monkeypatch, expect_source="NativeSkeneLatmatScanSource")
    assert len(rows) == 10
    assert facts["row_groups_read"] == 1, facts
    assert facts["row_groups_pruned"] > 0, facts


# The two-pass late-materialization Source's own pushed-predicate behaviour is
# covered by test_skene_latmat_scan.py (which carries the wide fixture that path's
# min-deferred-columns gate needs); it is deliberately not duplicated here.


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
