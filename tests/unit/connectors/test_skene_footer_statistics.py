"""Skene footer NDV and null counts reaching the planner.

Two statistics a .skene footer has always stored were unreachable: `ndv` was
absent from the Cython `ColumnStatistics` declaration, so no Python caller could
see it at all, and the connector's SKENE branch read only the min/max flags and
passed `column_stats=None`. The consequences were measurable — skene columns got
`distinct_count` only from the integer bounds-span heuristic (None for every
VARCHAR), `null_fraction` was None for every skene column, and TopN manifest
pruning, which gates on `get_total_null_count`, could never fire on skene.

These tests pin the three things that must not silently regress: the flag-gated
exact-vs-estimate spelling of NDV, the FILE-level aggregation rules over the
per-ROW-GROUP blobs, and the end-to-end arrival of both statistics in
RelationStatistics.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.connectors.skene_io import skene_aggregate_row_group_statistics

# skene format.h StatFlag
K_STAT_MIN = 0x1
K_STAT_MAX = 0x2
K_STAT_NULL_COUNT = 0x4
K_STAT_NDV = 0x40
K_STAT_NDV_EXACT = 0x80
K_STAT_SKETCH = 0x100

TPCH_SKENE = os.path.join("testdata", "tpch_1_skene")
TPCDS_SKENE = os.path.join("testdata", "tpcds_1_skene")
JOB_SKENE = os.path.join("testdata", "job_skene")
V1_FIXTURE = os.path.join("skene", "tests", "fixtures", "v1", "v1_accel_none.skene")

# The big fixtures are not populated everywhere — same convention as
# tests/unit/optimizer/test_predicate_pushdown_across_barrier.py.
needs_tpch = pytest.mark.skipif(not os.path.isdir(TPCH_SKENE), reason=f"{TPCH_SKENE} not populated")
needs_tpcds = pytest.mark.skipif(
    not os.path.isdir(TPCDS_SKENE), reason=f"{TPCDS_SKENE} not populated"
)
needs_job = pytest.mark.skipif(not os.path.isdir(JOB_SKENE), reason=f"{JOB_SKENE} not populated")


def _blob(*, flags, lo=0, hi=0, nulls=0, ndv=None, ndv_exact=None, sketch=None):
    """One statistics blob in the shape skene.read_metadata() emits."""
    return {
        "flags": flags,
        "min_ordinal": lo,
        "max_ordinal": hi,
        "null_count": nulls,
        "sum": 0,
        "ndv": ndv,
        "ndv_exact": ndv_exact,
        "sketch": sketch,
    }


def _row_groups(*blobs):
    """One single-column row group per blob."""
    return [{"column_statistics": [b]} for b in blobs]


def _read_footer(path):
    """Footer of `path`, or of the first .skene file in it when it is a dataset
    directory."""
    from skene import read_metadata

    if os.path.isdir(path):
        files = sorted(f for f in os.listdir(path) if f.endswith(".skene"))
        assert files, f"no .skene files under {path}"
        path = os.path.join(path, files[0])
    with open(path, "rb") as handle:
        return read_metadata(memoryview(handle.read()))


# ── Task A: the native emitter ───────────────────────────────────────────────


def test_ndv_is_gated_on_its_flag_not_on_the_bytes():
    """`ndv` is a v2 growth field; kStatNdv is the only honest reader of it.

    A v1 blob is a 48-byte PREFIX of ColumnStatistics and the reader memcpy's
    only the bytes it was given, so the `ndv` slot on a v1 blob was never
    written. Reading it as a number would report 0 distinct values for every
    column of every v1 file — a fabricated statistic, not a missing one.
    """
    footer = _read_footer(V1_FIXTURE)
    assert footer["version"] == 1, "fixture is meant to be the golden v1 file"

    seen = 0
    for row_group in footer["row_groups"]:
        for statistics in row_group["column_statistics"]:
            if statistics is None:
                continue
            seen += 1
            assert statistics["flags"] & K_STAT_NDV == 0
            assert statistics["ndv"] is None
            assert statistics["ndv_exact"] is None
    assert seen > 0, "fixture carried no statistics blobs at all"


@needs_tpch
def test_exact_and_sketched_ndv_are_told_apart():
    """kStatNdvExact and kStatNdv alone are not the same kind of number.

    Exact means value ordering deduplicated the column, so the count is a
    BOUND. kStatNdv alone means the write-side KMV sketch estimated it (~+/-3%
    at K=1024). A consumer needing a bound must be able to tell, so the value
    and the flag that produced it travel together and stay consistent.
    """
    footer = _read_footer(os.path.join(TPCH_SKENE, "part"))
    assert footer["version"] == 2

    exact_seen = sketched_seen = 0
    for row_group in footer["row_groups"]:
        for statistics in row_group["column_statistics"]:
            if statistics is None:
                continue
            tracked = bool(statistics["flags"] & K_STAT_NDV)
            assert (statistics["ndv"] is not None) is tracked
            assert (statistics["ndv_exact"] is not None) is tracked
            if not tracked:
                # kStatNdvExact is never set without kStatNdv (format.h).
                assert statistics["flags"] & K_STAT_NDV_EXACT == 0
                continue
            assert statistics["ndv_exact"] is bool(statistics["flags"] & K_STAT_NDV_EXACT)
            exact_seen += statistics["ndv_exact"]
            sketched_seen += not statistics["ndv_exact"]

    assert exact_seen, "expected value-ordered columns in tpch part"
    assert sketched_seen, "expected sketch-estimated columns in tpch part"


@needs_tpch
def test_exact_ndv_is_the_true_distinct_count():
    """An exact NDV claims to be a bound, so check it against known truth.

    p_mfgr/p_brand/p_type/p_size/p_container are TPC-H generator constants at
    any scale factor, and `part` is a single-row-group fixture, so the file
    NDV is that row group's NDV with no merge in between.
    """
    footer = _read_footer(os.path.join(TPCH_SKENE, "part"))
    assert len(footer["row_groups"]) == 1

    by_name = dict(
        zip(
            [column["name"] for column in footer["columns"]],
            footer["row_groups"][0]["column_statistics"],
        )
    )
    for name, truth in (
        ("p_mfgr", 5),
        ("p_brand", 25),
        ("p_type", 150),
        ("p_size", 50),
        ("p_container", 40),
    ):
        assert by_name[name]["ndv_exact"] is True, name
        assert by_name[name]["ndv"] == truth, name


# ── Task B: row group -> file aggregation ────────────────────────────────────


def test_null_counts_sum_across_row_groups():
    """A row belongs to exactly one row group, so the file total is the sum."""
    flags = K_STAT_NULL_COUNT
    *_, nulls, _, _, _ = skene_aggregate_row_group_statistics(
        _row_groups(
            _blob(flags=flags, nulls=3),
            _blob(flags=flags, nulls=4),
            _blob(flags=flags, nulls=0),
        ),
        [0],
    )
    assert nulls == {0: 7}


def test_one_row_group_without_a_null_count_makes_the_file_unknown():
    """A partial sum UNDERSTATES nulls, and TopN manifest pruning reads a total
    of 0 as "provably no nulls" — an understated total is a wrong answer, not a
    worse estimate."""
    *_, nulls, _, _, _ = skene_aggregate_row_group_statistics(
        _row_groups(
            _blob(flags=K_STAT_NULL_COUNT, nulls=3),
            _blob(flags=0),
        ),
        [0],
    )
    assert nulls == {}


def test_ndv_sums_only_when_the_row_group_ranges_are_disjoint():
    """Disjoint ordinal ranges hold disjoint values, so the counts add — and a
    sum of exact counts over disjoint ranges is still exact."""
    flags = K_STAT_MIN | K_STAT_MAX | K_STAT_NDV | K_STAT_NDV_EXACT
    *_, distincts, _, _ = skene_aggregate_row_group_statistics(
        _row_groups(
            _blob(flags=flags, lo=0, hi=9, ndv=10, ndv_exact=True),
            _blob(flags=flags, lo=10, hi=19, ndv=10, ndv_exact=True),
        ),
        [0],
    )
    assert distincts == {0: (20, True)}


def test_ndv_takes_the_max_when_the_ranges_overlap():
    """Two row groups can hold the SAME value, so summing would overstate the
    column's cardinality. MAX is the safe floor — and a floor is an estimate
    however exact each contributor was."""
    flags = K_STAT_MIN | K_STAT_MAX | K_STAT_NDV | K_STAT_NDV_EXACT
    *_, distincts, _, _ = skene_aggregate_row_group_statistics(
        _row_groups(
            _blob(flags=flags, lo=0, hi=99, ndv=10, ndv_exact=True),
            _blob(flags=flags, lo=50, hi=149, ndv=14, ndv_exact=True),
        ),
        [0],
    )
    assert distincts == {0: (14, False)}


def test_a_sketched_contributor_taints_an_otherwise_exact_sum():
    flags = K_STAT_MIN | K_STAT_MAX | K_STAT_NDV
    *_, distincts, _, _ = skene_aggregate_row_group_statistics(
        _row_groups(
            _blob(flags=flags | K_STAT_NDV_EXACT, lo=0, hi=9, ndv=10, ndv_exact=True),
            _blob(flags=flags, lo=10, hi=19, ndv=9, ndv_exact=False),
        ),
        [0],
    )
    assert distincts == {0: (19, False)}


def test_ndv_without_bounds_cannot_prove_disjointness_and_takes_the_max():
    """No range means no disjointness proof; MAX is the only safe answer."""
    flags = K_STAT_NDV | K_STAT_NDV_EXACT
    *_, distincts, _, _ = skene_aggregate_row_group_statistics(
        _row_groups(
            _blob(flags=flags, ndv=10, ndv_exact=True),
            _blob(flags=flags, ndv=7, ndv_exact=True),
        ),
        [0],
    )
    assert distincts == {0: (10, False)}


def test_one_row_group_without_ndv_makes_the_file_unknown():
    flags = K_STAT_MIN | K_STAT_MAX | K_STAT_NDV | K_STAT_NDV_EXACT
    *_, distincts, _, _ = skene_aggregate_row_group_statistics(
        _row_groups(
            _blob(flags=flags, lo=0, hi=9, ndv=10, ndv_exact=True),
            _blob(flags=K_STAT_MIN | K_STAT_MAX, lo=10, hi=19),
        ),
        [0],
    )
    assert distincts == {}


def test_the_three_aggregations_are_independent():
    """A row group that bounds nothing still carries a usable null count. One
    missing statistic must not discard the other two."""
    lower, upper, nulls, distincts, _, _ = skene_aggregate_row_group_statistics(
        _row_groups(
            _blob(
                flags=K_STAT_MIN | K_STAT_MAX | K_STAT_NULL_COUNT | K_STAT_NDV,
                lo=0,
                hi=9,
                nulls=2,
                ndv=5,
                ndv_exact=False,
            ),
            # No bounds; null count and NDV still present.
            _blob(flags=K_STAT_NULL_COUNT | K_STAT_NDV, nulls=3, ndv=4, ndv_exact=False),
        ),
        [0],
    )
    assert lower == {} and upper == {}, "an un-bounded row group must void the bound"
    assert nulls == {0: 5}
    assert distincts == {0: (5, False)}


def test_array_child_slots_are_skipped():
    """Slots are depth first and include ARRAY children; an element's statistic
    is not the array's, and manifest keys are top-level schema positions."""
    blob = _blob(flags=K_STAT_NULL_COUNT | K_STAT_NDV, nulls=1, ndv=3, ndv_exact=False)
    row_groups = [{"column_statistics": [blob, blob, blob]}]
    _, _, nulls, distincts, _, _ = skene_aggregate_row_group_statistics(row_groups, [0, None, 1])
    assert sorted(nulls) == [0, 1]
    assert sorted(distincts) == [0, 1]


def test_a_row_group_tracking_nothing_voids_every_aggregation():
    lower, upper, nulls, distincts, _, _ = skene_aggregate_row_group_statistics(
        [
            {"column_statistics": [_blob(flags=0x7 | K_STAT_NDV, lo=0, hi=9, ndv=5, ndv_exact=True)]},
            {"column_statistics": [None]},
        ],
        [0],
    )
    assert (lower, upper, nulls, distincts) == ({}, {}, {}, {})


# ── Task B: end to end into RelationStatistics ───────────────────────────────


def _relation_statistics(dataset):
    from opteryx.connectors import connector_factory

    connector = connector_factory(dataset, telemetry=None)
    table = connector.table_engine(dataset)
    schema, manifest = table.get_dataset_metadata()
    stats = manifest._as_relation_statistics()
    by_name = {
        column.name: stats.columns.get(column.identity)
        for column in schema.columns
        if column.identity in stats.columns
    }
    return manifest, by_name


@needs_tpch
def test_skene_varchar_columns_now_have_a_distinct_count():
    """Before this wiring, distinct_count came only from the integer bounds-span
    heuristic, which is None for every VARCHAR (a prefix-packed string ordinal
    span says nothing about cardinality)."""
    _, columns = _relation_statistics(os.path.join(TPCH_SKENE, "part"))

    assert columns["p_comment"].distinct_count is not None
    # p_mfgr/p_brand/p_type/p_container are TPC-H generator constants.
    assert columns["p_mfgr"].distinct_count == 5
    assert columns["p_brand"].distinct_count == 25
    assert columns["p_type"].distinct_count == 150
    assert columns["p_container"].distinct_count == 40


@needs_tpch
def test_skene_columns_now_have_a_null_fraction():
    """`has_null_counts` was False for every skene relation, so null_fraction
    was None for every skene column regardless of what the footer stored."""
    manifest, columns = _relation_statistics(os.path.join(TPCH_SKENE, "part"))

    assert columns["p_comment"].null_fraction == 0.0
    assert columns["p_partkey"].null_fraction == 0.0
    # TopN manifest pruning gates on this; None meant it could never fire.
    assert manifest.get_total_null_count("p_comment") == 0


@needs_tpch
def test_multi_row_group_ndv_is_not_summed():
    """lineitem is 16 row groups over 2 files. l_returnflag has 3 distinct
    values in TPC-H at any scale; a per-row-group sum would report ~48 and a
    per-file sum ~6."""
    _, columns = _relation_statistics(os.path.join(TPCH_SKENE, "lineitem"))

    assert columns["l_returnflag"].distinct_count == 3
    assert columns["l_linestatus"].distinct_count == 2
    assert columns["l_shipmode"].distinct_count == 7
    assert columns["l_shipinstruct"].distinct_count == 4


@needs_tpch
def test_distinct_counts_never_exceed_the_row_count():
    """The KMV sketch can overshoot (c_phone estimates 150400 for a 150000-row
    file); the cap is what keeps a cardinality above the relation's own size out
    of the planner."""
    manifest, columns = _relation_statistics(os.path.join(TPCH_SKENE, "customer"))
    rows = manifest.get_record_count()

    for name, column in columns.items():
        if column.distinct_count is not None:
            assert 1 <= column.distinct_count <= rows, name


@needs_tpcds
def test_null_counts_match_the_data():
    """TPC-H has no NULLs anywhere, so the sum-across-row-groups rule only ever
    sees zeros there. TPC-DS does, including a column that is ALL null — which
    is also the case that proves the aggregations are independent, since an
    all-null column carries no bounds at all (ORDINAL_NULL would poison them)
    but must still report its null count.

    ⛔ The oracle MUST read data. `SELECT COUNT(col)` is answered straight out
    of the manifest by StatisticsOnlyResponseStrategy — from these very null
    counts — so asserting against it proves a tautology, not a fact. Hence the
    `WHERE col IS NULL` form and the explicit assertion that the
    statistics-only path did not fire.
    """
    import opteryx
    from opteryx.connectors import DiskConnector

    opteryx.register_workspace("testdata", DiskConnector)

    def count_nulls_by_reading(table, column):
        session = opteryx.session()
        counted = None
        try:
            for morsel in session.execute_to_morsels(
                f"SELECT COUNT(*) AS n FROM testdata.tpcds_1_skene.{table} "
                f"WHERE {column} IS NULL"
            ):
                counted = morsel.column(b"n").to_pylist()[0]
            telemetry = session.telemetry
        finally:
            session.close()
        assert not telemetry.get("optimization_statistics_only_response"), (
            f"{table}.{column}: the oracle was answered from the manifest, so it "
            "is checking the footer against itself"
        )
        return counted

    cases = [
        ("call_center", ["cc_rec_end_date", "cc_closed_date_sk"]),
        ("catalog_page", ["cp_start_date_sk", "cp_department", "cp_description"]),
        ("web_sales", ["ws_ship_customer_sk", "ws_promo_sk"]),  # 3 row groups
    ]
    for table, columns in cases:
        manifest, _ = _relation_statistics(os.path.join(TPCDS_SKENE, table))
        rows = manifest.get_record_count()
        for column in columns:
            nulls = count_nulls_by_reading(table, column)
            assert manifest.get_total_null_count(column) == nulls, f"{table}.{column}"
            assert manifest.estimate_null_fraction(column) == pytest.approx(nulls / rows)

    # cc_closed_date_sk is all-null: the bound is absent, the count is not.
    manifest, _ = _relation_statistics(os.path.join(TPCDS_SKENE, "call_center"))
    assert manifest.estimate_null_fraction("cc_closed_date_sk") == 1.0


@needs_job
def test_topn_manifest_pruning_can_now_fire_on_skene():
    """The null count is a CORRECTNESS precondition, not a cost input.

    `prune_files_for_topn` drops whole files and explicitly does not re-check
    nullability — `TopNManifestPruningStrategy` must have proved
    `get_total_null_count(col) == 0` first. That proof was impossible for skene
    (the count was always None), so the strategy could never fire on a skene
    relation whatever the data looked like. cast_info is 9 files with disjoint
    `id` ranges, so an `ORDER BY id LIMIT n` needs exactly one of them.
    """
    manifest, _ = _relation_statistics(os.path.join(JOB_SKENE, "cast_info"))
    assert manifest.get_file_count() > 1, "fixture is meant to be multi-file"

    # The precondition the strategy gates on — None before this wiring.
    assert manifest.get_total_null_count("id") == 0

    pruned = manifest.prune_files_for_topn("id", descending=False, limit=6)
    assert pruned is not manifest, "a real prune must hand back a NEW manifest"
    assert pruned.get_file_count() < manifest.get_file_count()

    # The survivors must still cover the rows the query will return: the file
    # holding the global minimum cannot have been dropped.
    position = manifest._resolve_field_id("id")
    lowest = min(f.lower_bounds[position] for f in manifest.files)
    assert min(f.lower_bounds[position] for f in pruned.files) == lowest


# ── The stored KMV sketch ────────────────────────────────────────────────────


def test_sketch_round_trips_and_is_exact_below_k():
    """A sketch that never filled holds EVERY distinct value, so its length is
    the exact answer — the regime every low-cardinality column lives in."""
    import skene
    from draken.draken_native import DrakenType
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel

    ints = vector_from_sequence([5, 3, 3, None, 9], DrakenType.INT64)
    strs = vector_from_sequence(["red", "blue", "red", "green", None], DrakenType.VARCHAR)
    buf = skene.write_morsel(
        Morsel.from_vectors(["a", "colour"], [ints, strs]), read_acceleration=True
    )
    metadata = skene.read_metadata(buf)

    for statistics in metadata["row_groups"][0]["column_statistics"]:
        assert statistics["sketch"] is not None
        # Nulls contribute no value, so 3 distinct out of 5 rows with one null.
        assert statistics["ndv"] == 3
        assert len(statistics["sketch"]) == 3
        assert statistics["sketch"] == sorted(statistics["sketch"])
        assert len(set(statistics["sketch"])) == 3


def test_pre_sketch_files_read_back_without_one():
    """The sketch is appended after the fixed struct in the same length-prefixed
    blob, so older files must still parse — losing an estimate, not erroring."""
    for path in (V1_FIXTURE, os.path.join(TPCH_SKENE, "nation")):
        if not os.path.exists(path):
            continue
        footer = _read_footer(path)
        for row_group in footer["row_groups"]:
            for statistics in row_group["column_statistics"]:
                if statistics is None:
                    continue
                assert statistics["sketch"] is None
                assert statistics["flags"] & 0x100 == 0  # kStatSketch


def test_sketches_union_across_row_groups():
    """The union of KMV sketches is the K smallest of the combined hashes —
    exact arithmetic, which is the whole reason the hashes are stored rather
    than the count they imply."""
    flags = K_STAT_NDV | K_STAT_SKETCH
    _, _, _, distincts, sketches, _ = skene_aggregate_row_group_statistics(
        _row_groups(
            _blob(flags=flags, ndv=3, ndv_exact=False, sketch=[10, 20, 30]),
            _blob(flags=flags, ndv=3, ndv_exact=False, sketch=[20, 30, 40]),
        ),
        [0],
    )
    # Four distinct hashes across the two, well under K, so the answer is exact.
    assert sketches == {0: [10, 20, 30, 40]}
    assert distincts[0] == (3, False)  # the SCALAR merge, kept as a floor source


def test_one_row_group_without_a_sketch_voids_the_union():
    """A union missing a row group's hashes undercounts, and nothing tells that
    apart from a genuinely smaller column."""
    flags = K_STAT_NDV | K_STAT_SKETCH
    _, _, _, _, sketches, _ = skene_aggregate_row_group_statistics(
        _row_groups(
            _blob(flags=flags, ndv=3, ndv_exact=False, sketch=[10, 20, 30]),
            _blob(flags=K_STAT_NDV, ndv=3, ndv_exact=False),
        ),
        [0],
    )
    assert sketches == {}


def test_exact_row_group_counts_are_kept_as_a_proven_floor():
    """A row group is a subset of the file, so its EXACT distinct count can never
    exceed the file's — a hard lower bound the K=32 estimator must not go under.
    It survives a MAX merge, which is exactly when the estimator needs it."""
    flags = K_STAT_MIN | K_STAT_MAX | K_STAT_NDV | K_STAT_NDV_EXACT
    _, _, _, distincts, _, floors = skene_aggregate_row_group_statistics(
        _row_groups(
            _blob(flags=flags, lo=0, hi=99, ndv=2526, ndv_exact=True),
            _blob(flags=flags, lo=50, hi=149, ndv=2400, ndv_exact=True),
        ),
        [0],
    )
    # Overlapping ranges, so the merged count is a floor and NOT exact...
    assert distincts[0] == (2526, False)
    # ...but the largest exact row group count is still proven.
    assert floors == {0: 2526}


def test_a_sketched_row_group_count_is_not_a_floor():
    """An estimate is not a bound, however close it looks."""
    flags = K_STAT_MIN | K_STAT_MAX | K_STAT_NDV
    _, _, _, _, _, floors = skene_aggregate_row_group_statistics(
        _row_groups(_blob(flags=flags, lo=0, hi=99, ndv=2526, ndv_exact=False)),
        [0],
    )
    assert floors == {}


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
