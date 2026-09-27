"""
ANALYZE … FOR COLUMNS / DROP STATISTICS — dataset manifest lifecycle.

ANALYZE computes per-file KMV sketches and writes them into the dataset's single
manifest (the shared Parquet manifest format — see opteryx.models.manifest_io);
DROP STATISTICS removes them. NDV estimates are advisory (planning only) so there
is no correctness risk — these tests assert the manifest lifecycle and that the
estimator lights up.
"""

import glob
import os
import sys
from opteryx.compiled.structures.expressions import Comparison
from opteryx.compiled.structures.expressions import Literal

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import connector_factory
from opteryx.expression import NodeType
from opteryx.models.manifest_io import DATASET_MANIFEST_NAME
from opteryx.types.logical_type import FLOAT64
from opteryx.types.logical_type import INT64
from opteryx.types.logical_type import VARCHAR
from opteryx.types.logical_type import LogicalCategory
from opteryx.compiled.structures.expressions import LogicalColumn
from opteryx.planner.plan_context import PlanContext

DATASET = "testdata.satellites"
_MANIFEST_GLOB = f"testdata/satellites/{DATASET_MANIFEST_NAME}"
# satellites has no NULLs at all; the null-count assertions need a dataset that
# does (astronauts: death_date/death_mission are mostly null).
NULLABLE_DATASET = "testdata.astronauts"
_NULLABLE_MANIFEST_GLOB = f"testdata/astronauts/{DATASET_MANIFEST_NAME}"


def _clean():
    for p in glob.glob(_MANIFEST_GLOB):
        os.remove(p)


def _run(sql):
    list(opteryx.session().execute_to_morsels(sql))


def _manifests():
    return glob.glob(_MANIFEST_GLOB)


def _manifest_rows(path=None):
    """The dataset manifest's rows AS WRITTEN - one {manifest column: value}
    dict per file - read with rugo directly, which is what the format is. The
    per-column lists are positional over the dataset's schema."""
    import rugo.parquet as rugo_parquet

    with open(path or _manifests()[0], "rb") as handle:
        data = handle.read()
    rows = []
    with rugo_parquet.read_parquet(data) as reader:
        for morsel in reader:
            columns = {
                name.decode("utf-8"): morsel.column(name).to_pylist()
                for name in morsel.column_names
            }
            for i in range(morsel.num_rows):
                rows.append({name: values[i] for name, values in columns.items()})
    return rows


def _nested(column):
    """{file_path: positional per-column list} of one nested statistic
    (min_k_hashes / histogram_counts / char_class_counts) as the manifest
    records it, row by row."""
    return {
        row["file_path"]: [list(values or []) for values in (row[column] or [])]
        for row in _manifest_rows()
    }


def _sketches():
    """{file_path: positional per-column sketch} from the dataset manifest."""
    return _nested("min_k_hashes")


def _analyzed_column_count(sketch) -> int:
    """How many columns of a per-file sketch actually carry hashes."""
    return sum(1 for col in sketch if col)


def _metadata():
    eng = connector_factory(DATASET, None).table_engine(DATASET, telemetry=None)
    return eng.get_dataset_metadata()


def test_analyze_for_columns_writes_scoped_manifest():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS planetId, gm")
        manifests = _manifests()
        assert len(manifests) == 1, manifests
        sketches = _sketches()
        assert len(sketches) == 1  # one data file in this dataset
        sketch = next(iter(sketches.values()))
        schema, _ = _metadata()
        # The sketch is positional across the FULL schema...
        assert len(sketch) == len(schema.columns)
        # ...but only the named columns are sketched.
        assert _analyzed_column_count(sketch) == 2
    finally:
        _clean()


def test_manifest_is_not_read_back_as_a_data_file():
    """The manifest is a .parquet living beside the data it describes — the scan
    must never mistake it for a data file (it would corrupt every result)."""
    _clean()
    try:
        before = list(opteryx.session().execute_to_morsels("SELECT * FROM testdata.satellites"))
        rows_before = sum(m.num_rows for m in before)

        _run("ANALYZE TABLE testdata.satellites")
        assert len(_manifests()) == 1  # manifest now sits in the dataset directory

        after = list(opteryx.session().execute_to_morsels("SELECT * FROM testdata.satellites"))
        rows_after = sum(m.num_rows for m in after)

        assert rows_after == rows_before, "manifest leaked into the dataset's data files"
    finally:
        _clean()


def test_estimate_cardinality_lights_up_exact_for_low_ndv():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS planetId")
        _, manifest = _metadata()
        # satellites.planetId has few distinct planets → KMV is exact (< K).
        assert manifest.estimate_cardinality("planetId") == 7
    finally:
        _clean()


def test_drop_statistics_for_columns_then_all():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS planetId, gm")
        _run("DROP STATISTICS ON testdata.satellites FOR COLUMNS planetId")
        sketch = next(iter(_sketches().values()))
        assert _analyzed_column_count(sketch) == 1  # gm survives

        _run("DROP STATISTICS ON testdata.satellites")
        assert _manifests() == []
    finally:
        _clean()


def test_drop_statistics_is_idempotent():
    _clean()
    try:
        # No manifest present — dropping is a success, not an error.
        _run("DROP STATISTICS ON testdata.satellites")
        assert _manifests() == []
    finally:
        _clean()


def test_bare_analyze_covers_all_columns():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites")
        schema, _ = _metadata()
        sketch = next(iter(_sketches().values()))
        assert _analyzed_column_count(sketch) == len(schema.columns)
    finally:
        _clean()


def test_drop_statistics_bad_syntax_fails_loud():
    from opteryx.exceptions import UnsupportedSyntaxError

    _clean()
    try:
        failed = False
        try:
            _run("DROP STATISTICS testdata.satellites")  # missing ON
        except UnsupportedSyntaxError:
            failed = True
        assert failed
    finally:
        _clean()


def _comparison(plan_context, column_name, op, value, column_type):
    """`column_name <op> value`, built in the arena of the query that prunes;
    the literal carries its type and a native value."""
    arena = plan_context.expressions
    identifier = LogicalColumn(node_type=NodeType.IDENTIFIER, source_column=column_name, arena=arena)
    literal = Literal(value=value, type=column_type, arena=arena)
    return Comparison(value=op, left=identifier, right=literal, arena=arena)


# satellites.id ranges [1, 177], gm ranges [0.0, 9887.834], name ranges
# ['Adrastea', 'Ymir'] — one data file (see test above), confirmed via
# SELECT MIN/MAX before writing these tests.


def test_prune_files_wired_from_analyze_manifest_int_column():
    """ANALYZE's min/max for an INT column now actually reaches
    Manifest.prune_files (previously discarded — see filesystem_connector.py's
    _read_dataset_manifest). INT64.ordinalize is an identity widen, so this
    also proves the wiring end-to-end without any lossiness in play."""
    plan_context = PlanContext()
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS id")
        _, manifest = _metadata()

        assert manifest.bounds_are_ordinal is True
        assert manifest.get_ordinal_bounds("id") is not None

        # id's real range is [1, 177] — 10000 is far outside it.
        manifest = manifest.prune_files(
            [_comparison(plan_context, "id", "Gt", 10000, INT64)], plan_context=plan_context
        )
        assert manifest.get_file_count() == 0
    finally:
        _clean()


def test_prune_files_wired_from_analyze_manifest_int_column_keeps_in_range():
    plan_context = PlanContext()
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS id")
        _, manifest = _metadata()

        manifest = manifest.prune_files(
            [_comparison(plan_context, "id", "Eq", 1, INT64)], plan_context=plan_context
        )
        assert manifest.get_file_count() == 1
    finally:
        _clean()


def test_prune_files_wired_from_analyze_manifest_float_column():
    """gm's ordinal bound is NOT the real float value (lossy bit-transform) —
    pruning must still be correct because the predicate literal is run
    through the same ColumnType.ordinalize transform before comparing."""
    plan_context = PlanContext()
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS gm")
        _, manifest = _metadata()

        # The stored bound is an ordinal key, not the real value. (gm's real
        # min happens to be exactly 0.0, whose ordinal key is also 0 — use
        # the max bound, where the transform is unambiguously visible.)
        _, stored_max = manifest.get_ordinal_bounds("gm")
        assert stored_max != 9887.834  # real max is 9887.834; ordinal key is not

        # gm's real range is [0.0, 9887.834] — 1e12 is far outside it.
        manifest = manifest.prune_files(
            [_comparison(plan_context, "gm", "Gt", 1e12, FLOAT64)], plan_context=plan_context
        )
        assert manifest.get_file_count() == 0
    finally:
        _clean()


def test_prune_files_wired_from_analyze_manifest_float_column_keeps_in_range():
    plan_context = PlanContext()
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS gm")
        _, manifest = _metadata()

        manifest = manifest.prune_files(
            [_comparison(plan_context, "gm", "Lt", 5000.0, FLOAT64)], plan_context=plan_context
        )
        assert manifest.get_file_count() == 1
    finally:
        _clean()


def test_prune_files_wired_from_analyze_manifest_varchar_column():
    """name's ordinal bound is a lossy 8-byte-prefix transform, not the
    string itself — pruning must still be correct via ordinalize(literal)."""
    plan_context = PlanContext()
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS name")
        _, manifest = _metadata()

        stored_min, _ = manifest.get_ordinal_bounds("name")
        assert stored_min != "Adrastea"
        assert type(stored_min) is int

        # name's real range is ['Adrastea', 'Ymir'] — "Zzz" sorts after both.
        manifest = manifest.prune_files(
            [_comparison(plan_context, "name", "Eq", b"Zzz", VARCHAR)], plan_context=plan_context
        )
        assert manifest.get_file_count() == 0
    finally:
        _clean()


def test_prune_files_wired_from_analyze_manifest_varchar_column_keeps_in_range():
    plan_context = PlanContext()
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS name")
        _, manifest = _metadata()

        manifest = manifest.prune_files(
            [_comparison(plan_context, "name", "Eq", b"Adrastea", VARCHAR)], plan_context=plan_context
        )
        assert manifest.get_file_count() == 1
    finally:
        _clean()


def test_prune_files_manifest_bounds_survive_the_metadata_cache():
    """get_dataset_metadata caches the native manifest across calls within a
    process (see filesystem_connector._MANIFEST_CACHE) — bounds_are_ordinal must
    be cached alongside the rows, not just computed on the first (cold) call."""
    plan_context = PlanContext()
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS id")

        # First call builds and caches; second call is a cache hit.
        _metadata()
        _, manifest = _metadata()

        assert manifest.bounds_are_ordinal is True
        manifest = manifest.prune_files(
            [_comparison(plan_context, "id", "Gt", 10000, INT64)], plan_context=plan_context
        )
        assert manifest.get_file_count() == 0
    finally:
        _clean()


def test_no_manifest_means_no_bounds_and_no_pruning():
    """Without an ANALYZE'd manifest, no lower_bounds/upper_bounds are
    available at all — prune_files must be a safe no-op, not a crash."""
    plan_context = PlanContext()
    _clean()
    try:
        _, manifest = _metadata()
        bounds = manifest.native.cell(0, manifest.position_of("id"))["bounds"]
        assert bounds["min"] is None and bounds["min_ordinal"] is None

        manifest = manifest.prune_files(
            [_comparison(plan_context, "id", "Gt", 10000, INT64)], plan_context=plan_context
        )
        # No bounds to prune with — the file is conservatively kept.
        assert manifest.get_file_count() == 1
    finally:
        _clean()


# ── Part A: full native statistics pass (record_count, null_counts,
# min/max, histogram, char-class, lengths) — not just the KMV sketch ────────


def test_record_count_is_real_not_hardcoded_zero():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites")
        rows = _manifest_rows()
        assert len(rows) == 1
        assert rows[0]["record_count"] == 177  # satellites has 177 rows
    finally:
        _clean()


def test_null_counts_populated_for_analyzed_columns():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS id, name")
        rows = _manifest_rows()
        schema, _ = _metadata()
        id_idx = next(i for i, c in enumerate(schema.columns) if c.name == "id")
        name_idx = next(i for i, c in enumerate(schema.columns) if c.name == "name")
        null_counts = rows[0]["null_counts"]
        assert null_counts[id_idx] == 0  # satellites has no nulls
        assert null_counts[name_idx] == 0
        # An un-analyzed column's slot stays None, not a fabricated 0.
        gm_idx = next(i for i, c in enumerate(schema.columns) if c.name == "gm")
        assert null_counts[gm_idx] is None
    finally:
        _clean()


def test_histogram_bins_populated_and_sum_to_record_count():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS gm")
        histograms = _nested("histogram_counts")
        schema, _ = _metadata()
        gm_idx = next(i for i, c in enumerate(schema.columns) if c.name == "gm")
        rows = _manifest_rows()
        bins = histograms[rows[0]["file_path"]][gm_idx]
        assert len(bins) == 32  # HISTOGRAM_BINS
        assert sum(bins) == 177  # every non-null row counted exactly once
    finally:
        _clean()


def test_min_max_lengths_populated_for_string_columns_only():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites")  # all columns
        row = _manifest_rows()[0]
        schema, _ = _metadata()
        name_idx = next(i for i, c in enumerate(schema.columns) if c.name == "name")
        gm_idx = next(i for i, c in enumerate(schema.columns) if c.name == "gm")
        # 'Adrastea'..'Ymir'-ish range — real string lengths, not None.
        assert row["min_lengths"][name_idx] is not None
        assert row["max_lengths"][name_idx] is not None
        assert row["min_lengths"][name_idx] <= row["max_lengths"][name_idx]
        # gm is FLOAT64 — no string lengths.
        assert row["min_lengths"][gm_idx] is None
        assert row["max_lengths"][gm_idx] is None
    finally:
        _clean()


def test_char_class_counts_populated_for_string_columns_only():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites")
        char_classes = _nested("char_class_counts")
        schema, _ = _metadata()
        name_idx = next(i for i, c in enumerate(schema.columns) if c.name == "name")
        gm_idx = next(i for i, c in enumerate(schema.columns) if c.name == "gm")
        row = char_classes[_manifest_rows()[0]["file_path"]]
        assert len(row[name_idx]) == 8
        assert sum(row[name_idx]) > 0
        assert row[gm_idx] == []  # non-string column, empty not fabricated
    finally:
        _clean()


def test_char_total_bytes_equals_sum_of_char_class_counts():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS name")
        char_classes = _nested("char_class_counts")
        written = _manifest_rows()[0]
        schema, _ = _metadata()
        name_idx = next(i for i, c in enumerate(schema.columns) if c.name == "name")
        row = char_classes[written["file_path"]]
        assert written["char_total_bytes"][name_idx] == sum(row[name_idx])
    finally:
        _clean()


def test_column_subset_analyze_preserves_full_stats_of_untouched_columns():
    """A second ANALYZE FOR COLUMNS on a different column must not clobber
    the first column's null_counts/min_values/histogram/char-class — only
    the sketch merge-preserve was covered before this session's Part A work."""
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS id")
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS name")

        row = _manifest_rows()[0]
        schema, _ = _metadata()
        id_idx = next(i for i, c in enumerate(schema.columns) if c.name == "id")
        name_idx = next(i for i, c in enumerate(schema.columns) if c.name == "name")

        # id's stats from the FIRST analyze must still be present.
        assert row["null_counts"][id_idx] == 0
        assert row["min_values"][id_idx] is not None
        # name's stats from the SECOND analyze must also be present.
        assert row["min_lengths"][name_idx] is not None
    finally:
        _clean()


def test_drop_statistics_for_columns_clears_all_new_stat_types():
    """DROP STATISTICS FOR COLUMNS must clear null_counts/min_values/
    max_values/lengths/char-class for the dropped column too, not just the
    KMV sketch — while leaving other columns' full stats intact."""
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS id, name")
        _run("DROP STATISTICS ON testdata.satellites FOR COLUMNS name")

        row = _manifest_rows()[0]
        schema, _ = _metadata()
        id_idx = next(i for i, c in enumerate(schema.columns) if c.name == "id")
        name_idx = next(i for i, c in enumerate(schema.columns) if c.name == "name")

        # A list no column of the file records a value in is written EMPTY
        # (the manifest's "not tracked"); otherwise it is positional with None
        # for the dropped column. Either way nothing survives for `name`.
        def _unrecorded(values, index):
            return not values or values[index] is None

        assert _unrecorded(row["null_counts"], name_idx)
        assert _unrecorded(row["min_lengths"], name_idx)
        assert _unrecorded(row["max_lengths"], name_idx)
        assert _unrecorded(row["min_values"], name_idx)

        # id survives untouched.
        assert row["null_counts"][id_idx] == 0
        assert row["min_values"][id_idx] is not None

        char_classes = _nested("char_class_counts")
        assert char_classes[row["file_path"]][name_idx] == []
    finally:
        _clean()


def test_char_class_stats_light_up_the_selectivity_estimator():
    """End-to-end: ANALYZE a real VARCHAR column and confirm
    Manifest.get_char_class_stats returns usable (proportions, avg_length),
    the closest in-engine reproduction of the offline experiment's own
    validation against real data."""
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS name")
        _, manifest = _metadata()
        result = manifest.get_char_class_stats("name")
        assert result is not None
        class_proportions, avg_length = result
        assert set(class_proportions.keys()) == {
            "upper", "lower", "digit", "whitespace", "punct_text",
            "semantic", "extended", "control",
        }
        assert abs(sum(class_proportions.values()) - 1.0) < 1e-9
        assert avg_length > 0
    finally:
        _clean()


def test_no_char_class_stats_for_non_string_column():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS gm")
        _, manifest = _metadata()
        assert manifest.get_char_class_stats("gm") is None
    finally:
        _clean()


def test_analyze_does_not_crash_on_array_columns():
    """Vector.ordinalize() explicitly does not support DRAKEN_ARRAY (or
    VECTOR_FP16/DECIMAL128) -- calling it unguarded would crash ANALYZE for
    the whole file the moment it reached one of these columns. testdata.
    astronauts has two real ARRAY<VARCHAR> columns (alma_mater, missions) --
    confirm the min/max/histogram pass degrades that one column to "no
    stats" instead of aborting every other column's analysis too."""
    manifest_glob = "testdata/astronauts/_opteryx_manifest.parquet"
    for p in glob.glob(manifest_glob):
        os.remove(p)
    try:
        _run("ANALYZE TABLE testdata.astronauts")
        eng = connector_factory("testdata.astronauts", None).table_engine(
            "testdata.astronauts", telemetry=None
        )
        schema, _ = eng.get_dataset_metadata()
        row = _manifest_rows(glob.glob(manifest_glob)[0])[0]

        alma_mater_idx = next(i for i, c in enumerate(schema.columns) if c.name == "alma_mater")
        name_idx = next(i for i, c in enumerate(schema.columns) if c.name == "name")

        # The ARRAY column has no ordinal min/max (unsupported type)...
        assert row["min_values"][alma_mater_idx] is None
        # ...but every OTHER column's stats still landed -- the ARRAY column
        # didn't abort the rest of the file's analysis.
        assert row["min_values"][name_idx] is not None
        assert row["null_counts"][alma_mater_idx] is not None  # null_count has no such gap
    finally:
        for p in glob.glob(manifest_glob):
            os.remove(p)


def test_analyze_unknown_column_fails_loud():
    from opteryx.exceptions import ColumnNotFoundError

    _clean()
    try:
        failed = False
        try:
            _run("ANALYZE TABLE testdata.satellites FOR COLUMNS nonexistent")
        except ColumnNotFoundError:
            failed = True
        assert failed
    finally:
        _clean()


# ======================================================================
# Statistics decoded from the manifest must reach the manifest rows the
# planner sees. Before this, _read_dataset_manifest returned only the
# sketches and the value bounds: every other per-column statistic ANALYZE
# had computed was decoded and then dropped on the floor.
# ======================================================================


def _fresh_metadata(dataset):
    """Dataset metadata read from disk, not from the connector's process-global
    manifest cache. These tests assert what the READ PATH produces; a cache
    entry another test in this process left behind would answer for it."""
    from opteryx.connectors.filesystem_connector import _MANIFEST_CACHE

    _MANIFEST_CACHE.clear()
    eng = connector_factory(dataset, None).table_engine(dataset, telemetry=None)
    return eng.get_dataset_metadata()


def _astronauts_metadata():
    return _fresh_metadata(NULLABLE_DATASET)


def _clean_nullable():
    for p in glob.glob(_NULLABLE_MANIFEST_GLOB):
        os.remove(p)


def test_length_bounds_reach_the_manifest_from_an_analyzed_dataset():
    """get_length_bounds returned None for EVERY filesystem dataset, however
    recently ANALYZE'd, because min_length_bounds/max_length_bounds were never
    carried from the manifest onto the planner's file rows."""
    _clean()
    try:
        _, manifest = _fresh_metadata(DATASET)
        # Nothing ANALYZE'd: no length statistics exist at all.
        assert manifest.get_length_bounds("name") is None

        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS name")
        _, manifest = _fresh_metadata(DATASET)
        bounds = manifest.get_length_bounds("name")
        assert bounds is not None, "ANALYZE'd string column still has no length bounds"
        min_length, max_length = bounds
        assert 0 < min_length <= max_length
        # Real satellite names, not a fabricated span.
        assert (min_length, max_length) == (2, 10), bounds
    finally:
        _clean()


def test_null_counts_reach_the_manifest_from_an_analyzed_dataset():
    """ANALYZE's per-column null counts land on the planner's file row, keyed
    by load-time position (the manifest's one key space), and are what every
    Manifest accessor reads."""
    _clean_nullable()
    try:
        _run(f"ANALYZE TABLE {NULLABLE_DATASET}")
        _, manifest = _astronauts_metadata()
        assert manifest.has_null_counts()
        # death_date is mostly null in this dataset — a real count, not zeros.
        position = manifest.position_of("death_date")
        null_count = manifest.native.cell(0, position)["null_count"]
        assert null_count is not None and null_count > 0
        # ...and it is the number the manifest had written for that column.
        assert null_count == _manifest_rows(glob.glob(_NULLABLE_MANIFEST_GLOB)[0])[0]["null_counts"][position]
        assert manifest.get_total_null_count("death_date") == null_count

        null_fraction = manifest.estimate_null_fraction("death_date")
        assert null_fraction is not None and 0.0 < null_fraction < 1.0
    finally:
        _clean_nullable()


def test_relation_statistics_carry_length_bounds_and_null_fraction():
    """End-to-end at the surface the planner actually reads: the
    RelationStatistics snapshot the selectivity estimators are handed."""
    _clean_nullable()
    try:
        _run(f"ANALYZE TABLE {NULLABLE_DATASET}")
        described, manifest = _astronauts_metadata()
        # Bound the way binder/dataset.py::visit_scan binds a scan: statistics are
        # keyed by BOUND column identity, so the manifest reads the bound schema.
        manifest.schema = PlanContext().columns.bind_relation(described, NULLABLE_DATASET)
        stats = manifest._as_relation_statistics()

        column = next(
            c for c in manifest.schema.columns if c.name == "death_mission"
        )
        column_stats = stats.columns[column.identity]
        assert column_stats.length_bounds is not None
        assert column_stats.null_fraction is not None and column_stats.null_fraction > 0
        # avg_length divides char_total_bytes by the NON-NULL row count; with
        # ~95% of this column null, the raw-record_count denominator produced a
        # value an order of magnitude too small.
        assert column_stats.avg_length is not None
        assert column_stats.avg_length >= column_stats.length_bounds[0]
    finally:
        _clean_nullable()


def test_histogram_bin_count_is_read_back_not_assumed():
    """The manifest records how many bins its histograms hold; the reader
    honours that number rather than assuming manifest_io.HISTOGRAM_BINS."""
    from opteryx.models.manifest_io import HISTOGRAM_BINS

    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS id")
        _, manifest = _metadata()
        assert manifest.native.file_row(0)["histogram_bins"] == HISTOGRAM_BINS
        # ... and the histogram still folds cleanly against it.
        assert manifest.get_distogram("id") is not None
    finally:
        _clean()


def test_stale_row_bin_count_does_not_block_the_fold():
    """The counts are the truth; the row-level `histogram_bins` scalar is not.

    Widths legitimately vary per column within one file (an exact two-bin
    boolean histogram beside 32-bin numerics), so a single row-level number
    cannot describe them all — the reader takes each column's own slice length
    and ignores the scalar rather than rejecting a well-formed histogram."""
    _clean()
    try:
        from tests.manifests import FileSpec
        from tests.manifests import build_manifest

        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS id")
        schema, manifest = _metadata()
        # A width the stored counts do not have, on a NEW manifest over the same
        # file, bounds (the histogram's span) and sketch vectors -
        # get_dataset_metadata caches its manifest, so the live one is never
        # touched.
        file_row = manifest.native.file_row(0)
        position = manifest.position_of("id")
        bounds = manifest.native.cell(0, position)["bounds"]
        probe = build_manifest(
            schema,
            [
                FileSpec(
                    file_path=file_row["file_path"],
                    record_count=file_row["record_count"],
                    file_size_in_bytes=file_row["file_size_in_bytes"],
                    histogram_bins=17,
                    lower_bounds={position: bounds["min_ordinal"]},
                    upper_bounds={position: bounds["max_ordinal"]},
                )
            ],
            bounds_are_ordinal=manifest.bounds_are_ordinal,
            sketches=manifest.native.sketches,
        )
        assert probe.native.file_row(0)["histogram_bins"] == 17
        assert probe.get_distogram("id") is not None
    finally:
        _clean()


def _written_bin_count(histograms, stored_bins=None):
    """The histogram_bins the manifest writer (the native encoder) records for
    one file whose per-column histogram_counts are `histograms` (None: no
    histogram vector at all) and whose row carries `stored_bins`."""
    from draken import draken_native as dn

    from opteryx.types.schema import ColumnDescriptor
    from opteryx.types.schema import RelationDescriptor
    from tests.manifests import FileSpec
    from tests.manifests import build_manifest

    schema = RelationDescriptor(
        name="t",
        columns=[ColumnDescriptor(name=f"c{i}", column_type=INT64) for i in range(2)],
    )
    sketches = {}
    if histograms is not None:
        sketches["histogram_counts"] = dn.vector_array_from_sequence(
            [histograms], element_type=dn.DrakenType.INT64.value, nesting_depth=2
        )
    manifest = build_manifest(
        schema,
        [FileSpec("f.parquet", record_count=1, file_size_in_bytes=1, histogram_bins=stored_bins)],
        bounds_are_ordinal=True,
        sketches=sketches,
    )
    manifest_bytes = manifest.native.to_parquet()
    rows = []
    import rugo.parquet as rugo_parquet

    with rugo_parquet.read_parquet(manifest_bytes) as reader:
        for morsel in reader:
            rows.extend(morsel.column(b"histogram_bins").to_pylist())
    assert len(rows) == 1
    return rows[0]


def test_manifest_writer_stamps_the_real_bin_count():
    assert _written_bin_count(None) == 0
    assert _written_bin_count([[], []]) == 0
    assert _written_bin_count([[0] * 8, []]) == 8

    # The width is derived from the counts in hand, never copied from a stored
    # scalar that producers stamp unconditionally.
    assert _written_bin_count([[0] * 8, []], stored_bins=8) == 8
    assert _written_bin_count([[0] * 8, []], stored_bins=32) == 8

    # Per-column widths in one file are legal — a boolean's exact two bins
    # beside 32-bin numerics. No single number describes them, so the row says
    # 0 ("no single width") and readers fall back to each column's own length.
    assert _written_bin_count([[0] * 2, [0] * 32]) == 0


def test_analyze_records_uncompressed_sizes():
    """ANALYZE computed no size statistics at all: the manifest's
    uncompressed_size_in_bytes / column_uncompressed_sizes_in_bytes columns were
    written empty for every filesystem dataset."""
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites")
        row = _manifest_rows()[0]
        sizes = row["column_uncompressed_sizes_in_bytes"]

        schema, manifest = _fresh_metadata(DATASET)
        assert sizes is not None
        assert len(sizes) == len(schema.columns)
        assert all(size > 0 for size in sizes)
        # The file total is the sum of its columns, not a separate measurement.
        assert row["uncompressed_size_in_bytes"] == sum(sizes)

        # ... and they are the SAME bytes the footer reports, positionally by
        # load-time position — a size list keyed one column out would be
        # silently wrong, never visibly so.
        for position, column in enumerate(schema.columns):
            assert sizes[position] == manifest.get_total_uncompressed_size(column.name), column.name
    finally:
        _clean()


def test_analyze_for_columns_still_sizes_every_column():
    """Sizes are facts about the file, not about the analyzed columns: a subset
    ANALYZE must not leave holes in a list that is read positionally."""
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS name")
        schema, _ = _fresh_metadata(DATASET)
        sizes = _manifest_rows()[0]["column_uncompressed_sizes_in_bytes"]
        assert sizes is not None and len(sizes) == len(schema.columns)
        assert all(size > 0 for size in sizes)
    finally:
        _clean()


def test_drop_statistics_for_columns_keeps_sizes():
    """DROP STATISTICS clears value statistics. A column's byte size on disk is
    not one of them, and the surviving list is still read positionally."""
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites")
        before = _manifest_rows()[0]

        _run("DROP STATISTICS ON testdata.satellites FOR COLUMNS name")
        after = _manifest_rows()[0]

        assert (
            after["column_uncompressed_sizes_in_bytes"]
            == before["column_uncompressed_sizes_in_bytes"]
        )
        assert after["uncompressed_size_in_bytes"] == before["uncompressed_size_in_bytes"]
    finally:
        _clean()


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
