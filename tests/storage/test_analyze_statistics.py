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

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import connector_factory
from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.models.manifest_io import DATASET_MANIFEST_NAME
from opteryx.models.manifest_io import read_manifest_char_classes
from opteryx.models.manifest_io import read_manifest_file_entries
from opteryx.models.manifest_io import read_manifest_histograms
from opteryx.models.manifest_io import read_manifest_sketches
from opteryx.types.logical_type import LogicalCategory

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


def _sketches():
    """{file_path: positional per-column sketch} from the dataset manifest."""
    with open(_manifests()[0], "rb") as handle:
        return read_manifest_sketches(handle.read())


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


def _comparison(column_name, op, value):
    identifier = Node(NodeType.IDENTIFIER, source_column=column_name)
    literal = Node(NodeType.LITERAL, value=value)
    return Node(NodeType.COMPARISON_OPERATOR, value=op, left=identifier, right=literal)


# satellites.id ranges [1, 177], gm ranges [0.0, 9887.834], name ranges
# ['Adrastea', 'Ymir'] — one data file (see test above), confirmed via
# SELECT MIN/MAX before writing these tests.


def test_prune_files_wired_from_analyze_manifest_int_column():
    """ANALYZE's min/max for an INT column now actually reaches
    Manifest.prune_files (previously discarded — see filesystem_connector.py's
    _read_dataset_manifest). INT64.ordinalize is an identity widen, so this
    also proves the wiring end-to-end without any lossiness in play."""
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS id")
        _, manifest = _metadata()

        assert manifest.bounds_are_ordinal is True
        assert manifest.files[0].lower_bounds is not None

        # id's real range is [1, 177] — 10000 is far outside it.
        manifest = manifest.prune_files([_comparison("id", "Gt", 10000)])
        assert manifest.files == []
    finally:
        _clean()


def test_prune_files_wired_from_analyze_manifest_int_column_keeps_in_range():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS id")
        _, manifest = _metadata()

        manifest = manifest.prune_files([_comparison("id", "Eq", 1)])
        assert len(manifest.files) == 1
    finally:
        _clean()


def test_prune_files_wired_from_analyze_manifest_float_column():
    """gm's ordinal bound is NOT the real float value (lossy bit-transform) —
    pruning must still be correct because the predicate literal is run
    through the same ColumnType.ordinalize transform before comparing."""
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS gm")
        _, manifest = _metadata()

        # The stored bound is an ordinal key, not the real value. (gm's real
        # min happens to be exactly 0.0, whose ordinal key is also 0 — use
        # the max bound, where the transform is unambiguously visible.)
        field_id = next(
            i for i, c in enumerate(manifest.schema.columns) if c.name == "gm"
        )
        stored_max = manifest.files[0].upper_bounds[field_id]
        assert stored_max != 9887.834  # real max is 9887.834; ordinal key is not

        # gm's real range is [0.0, 9887.834] — 1e12 is far outside it.
        manifest = manifest.prune_files([_comparison("gm", "Gt", 1e12)])
        assert manifest.files == []
    finally:
        _clean()


def test_prune_files_wired_from_analyze_manifest_float_column_keeps_in_range():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS gm")
        _, manifest = _metadata()

        manifest = manifest.prune_files([_comparison("gm", "Lt", 5000.0)])
        assert len(manifest.files) == 1
    finally:
        _clean()


def test_prune_files_wired_from_analyze_manifest_varchar_column():
    """name's ordinal bound is a lossy 8-byte-prefix transform, not the
    string itself — pruning must still be correct via ordinalize(literal)."""
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS name")
        _, manifest = _metadata()

        field_id = next(
            i for i, c in enumerate(manifest.schema.columns) if c.name == "name"
        )
        stored_min = manifest.files[0].lower_bounds[field_id]
        assert stored_min != "Adrastea"
        assert isinstance(stored_min, int)

        # name's real range is ['Adrastea', 'Ymir'] — "Zzz" sorts after both.
        manifest = manifest.prune_files([_comparison("name", "Eq", "Zzz")])
        assert manifest.files == []
    finally:
        _clean()


def test_prune_files_wired_from_analyze_manifest_varchar_column_keeps_in_range():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS name")
        _, manifest = _metadata()

        manifest = manifest.prune_files([_comparison("name", "Eq", "Adrastea")])
        assert len(manifest.files) == 1
    finally:
        _clean()


def test_prune_files_manifest_bounds_survive_the_metadata_cache():
    """get_dataset_metadata caches file_entries across calls within a process
    (see filesystem_connector._MANIFEST_CACHE) — bounds_are_ordinal must be
    cached alongside them, not just computed on the first (cold) call."""
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS id")

        # First call builds and caches; second call is a cache hit.
        _metadata()
        _, manifest = _metadata()

        assert manifest.bounds_are_ordinal is True
        manifest = manifest.prune_files([_comparison("id", "Gt", 10000)])
        assert manifest.files == []
    finally:
        _clean()


def test_no_manifest_means_no_bounds_and_no_pruning():
    """Without an ANALYZE'd manifest, no lower_bounds/upper_bounds are
    available at all — prune_files must be a safe no-op, not a crash."""
    _clean()
    try:
        _, manifest = _metadata()
        assert manifest.files[0].lower_bounds is None

        manifest = manifest.prune_files([_comparison("id", "Gt", 10000)])
        # No bounds to prune with — the file is conservatively kept.
        assert len(manifest.files) == 1
    finally:
        _clean()


# ── Part A: full native statistics pass (record_count, null_counts,
# min/max, histogram, char-class, lengths) — not just the KMV sketch ────────


def _entries():
    with open(_manifests()[0], "rb") as handle:
        return read_manifest_file_entries(handle.read())


def test_record_count_is_real_not_hardcoded_zero():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites")
        entries, _native = _entries()
        assert len(entries) == 1
        assert entries[0].record_count == 177  # satellites has 177 rows
    finally:
        _clean()


def test_null_counts_populated_for_analyzed_columns():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS id, name")
        entries, _native = _entries()
        schema, _ = _metadata()
        id_idx = next(i for i, c in enumerate(schema.columns) if c.name == "id")
        name_idx = next(i for i, c in enumerate(schema.columns) if c.name == "name")
        null_counts = entries[0].null_counts
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
        data = open(_manifests()[0], "rb").read()
        histograms = read_manifest_histograms(data)
        schema, _ = _metadata()
        gm_idx = next(i for i, c in enumerate(schema.columns) if c.name == "gm")
        entries, _native = _entries()
        bins = histograms[entries[0].file_path][gm_idx]
        assert len(bins) == 32  # HISTOGRAM_BINS
        assert sum(bins) == 177  # every non-null row counted exactly once
    finally:
        _clean()


def test_min_max_lengths_populated_for_string_columns_only():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites")  # all columns
        entries, _native = _entries()
        schema, _ = _metadata()
        name_idx = next(i for i, c in enumerate(schema.columns) if c.name == "name")
        gm_idx = next(i for i, c in enumerate(schema.columns) if c.name == "gm")
        # 'Adrastea'..'Ymir'-ish range — real string lengths, not None.
        assert entries[0].min_lengths[name_idx] is not None
        assert entries[0].max_lengths[name_idx] is not None
        assert entries[0].min_lengths[name_idx] <= entries[0].max_lengths[name_idx]
        # gm is FLOAT64 — no string lengths.
        assert entries[0].min_lengths[gm_idx] is None
        assert entries[0].max_lengths[gm_idx] is None
    finally:
        _clean()


def test_char_class_counts_populated_for_string_columns_only():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites")
        data = open(_manifests()[0], "rb").read()
        char_classes = read_manifest_char_classes(data)
        schema, _ = _metadata()
        name_idx = next(i for i, c in enumerate(schema.columns) if c.name == "name")
        gm_idx = next(i for i, c in enumerate(schema.columns) if c.name == "gm")
        entries, _native = _entries()
        row = char_classes[entries[0].file_path]
        assert len(row[name_idx]) == 8
        assert sum(row[name_idx]) > 0
        assert row[gm_idx] == []  # non-string column, empty not fabricated
    finally:
        _clean()


def test_char_total_bytes_equals_sum_of_char_class_counts():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS name")
        data = open(_manifests()[0], "rb").read()
        char_classes = read_manifest_char_classes(data)
        entries, _native = _entries()
        schema, _ = _metadata()
        name_idx = next(i for i, c in enumerate(schema.columns) if c.name == "name")
        row = char_classes[entries[0].file_path]
        assert entries[0].char_total_bytes[name_idx] == sum(row[name_idx])
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

        entries, _native = _entries()
        schema, _ = _metadata()
        id_idx = next(i for i, c in enumerate(schema.columns) if c.name == "id")
        name_idx = next(i for i, c in enumerate(schema.columns) if c.name == "name")

        # id's stats from the FIRST analyze must still be present.
        assert entries[0].null_counts[id_idx] == 0
        assert entries[0].min_values[id_idx] is not None
        # name's stats from the SECOND analyze must also be present.
        assert entries[0].min_lengths[name_idx] is not None
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

        entries, _native = _entries()
        schema, _ = _metadata()
        id_idx = next(i for i, c in enumerate(schema.columns) if c.name == "id")
        name_idx = next(i for i, c in enumerate(schema.columns) if c.name == "name")

        assert entries[0].null_counts[name_idx] is None
        assert entries[0].min_lengths[name_idx] is None
        assert entries[0].max_lengths[name_idx] is None
        assert entries[0].min_values[name_idx] is None

        # id survives untouched.
        assert entries[0].null_counts[id_idx] == 0
        assert entries[0].min_values[id_idx] is not None

        data = open(_manifests()[0], "rb").read()
        char_classes = read_manifest_char_classes(data)
        assert char_classes[entries[0].file_path][name_idx] == []
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
        with open(glob.glob(manifest_glob)[0], "rb") as handle:
            entries, _native = read_manifest_file_entries(handle.read())

        alma_mater_idx = next(i for i, c in enumerate(schema.columns) if c.name == "alma_mater")
        name_idx = next(i for i, c in enumerate(schema.columns) if c.name == "name")

        # The ARRAY column has no ordinal min/max (unsupported type)...
        assert entries[0].min_values[alma_mater_idx] is None
        # ...but every OTHER column's stats still landed -- the ARRAY column
        # didn't abort the rest of the file's analysis.
        assert entries[0].min_values[name_idx] is not None
        assert entries[0].null_counts[alma_mater_idx] is not None  # null_count has no such gap
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
# Statistics decoded from the manifest must reach the FileEntry the
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
    carried from the manifest onto the FileEntry."""
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
    """ANALYZE's per-column null counts land on the FileEntry in BOTH forms —
    the positional list (SHOW MANIFEST, the char-class avg_length denominator)
    and the field_id-keyed dict every Manifest accessor reads."""
    _clean_nullable()
    try:
        _run(f"ANALYZE TABLE {NULLABLE_DATASET}")
        _, manifest = _astronauts_metadata()
        file_entry = manifest.files[0]
        assert file_entry.null_counts is not None
        assert file_entry.null_value_counts is not None
        # death_date is mostly null in this dataset — a real count, not zeros.
        field_id = manifest._resolve_field_id("death_date")
        assert file_entry.null_value_counts[field_id] > 0
        assert file_entry.null_counts[field_id] == file_entry.null_value_counts[field_id]

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
        _, manifest = _astronauts_metadata()
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
        assert manifest.files[0].histogram_bins == HISTOGRAM_BINS
        # ... and the histogram still folds cleanly against it.
        assert manifest.get_distogram("id") is not None
    finally:
        _clean()


def test_histogram_bin_count_mismatch_fails_loud():
    """A stored bin count that disagrees with the counts actually present is a
    mis-binned histogram — every boundary in the wrong place. It must raise,
    never be silently coerced to the default width."""
    _clean()
    try:
        import dataclasses

        from opteryx.models.manifest import Manifest

        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS id")
        schema, manifest = _metadata()
        # Claim a width the stored counts do not have. A COPY of the file entry,
        # not the live one: get_dataset_metadata caches the FileEntry objects
        # themselves, so mutating one would poison every later reader.
        patched = dataclasses.replace(manifest.files[0], histogram_bins=17)
        probe = Manifest(
            [patched],
            schema,
            min_k_vector=manifest._min_k_vector,
            histogram_vector=manifest._histogram_vector,
            char_class_vector=manifest._char_class_vector,
            bounds_are_ordinal=manifest.bounds_are_ordinal,
        )
        failed = False
        try:
            probe.get_distogram("id")
        except ValueError:
            failed = True
        assert failed, "a mis-binned histogram was read as if it were well-formed"
    finally:
        _clean()


def test_manifest_writer_stamps_the_real_bin_count():
    from opteryx.models.manifest_io import _histogram_bins_of
    from opteryx.models.file_entry import FileEntry

    entry = FileEntry(file_path="f.parquet", file_format="PARQUET", record_count=1, file_size_in_bytes=1)
    assert _histogram_bins_of(entry, None) == 0
    assert _histogram_bins_of(entry, [[], []]) == 0
    assert _histogram_bins_of(entry, [[0] * 8, []]) == 8

    entry.histogram_bins = 8
    assert _histogram_bins_of(entry, [[0] * 8]) == 8

    entry.histogram_bins = 32
    failed = False
    try:
        _histogram_bins_of(entry, [[0] * 8])
    except ValueError:
        failed = True
    assert failed, "a bin count that contradicts the counts was written anyway"

    entry.histogram_bins = None
    failed = False
    try:
        _histogram_bins_of(entry, [[0] * 8, [0] * 16])
    except ValueError:
        failed = True
    assert failed, "two histogram widths cannot share one manifest row"


def test_analyze_records_uncompressed_sizes():
    """ANALYZE computed no size statistics at all: the manifest's
    uncompressed_size_in_bytes / column_uncompressed_sizes_in_bytes columns were
    written empty for every filesystem dataset."""
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites")
        with open(_manifests()[0], "rb") as handle:
            entries, _native = read_manifest_file_entries(handle.read())
        entry = entries[0]

        schema, manifest = _fresh_metadata(DATASET)
        assert entry.column_uncompressed_sizes_in_bytes is not None
        assert len(entry.column_uncompressed_sizes_in_bytes) == len(schema.columns)
        assert all(size > 0 for size in entry.column_uncompressed_sizes_in_bytes)
        # The file total is the sum of its columns, not a separate measurement.
        assert entry.uncompressed_size_in_bytes == sum(
            entry.column_uncompressed_sizes_in_bytes
        )

        # ... and they are the SAME bytes the footer reports, positionally by
        # field_id — a size list keyed one column out would be silently wrong,
        # never visibly so.
        for position, column in enumerate(schema.columns):
            assert entry.column_uncompressed_sizes_in_bytes[
                position
            ] == manifest.get_total_uncompressed_size(column.name), column.name
    finally:
        _clean()


def test_analyze_for_columns_still_sizes_every_column():
    """Sizes are facts about the file, not about the analyzed columns: a subset
    ANALYZE must not leave holes in a list that is read positionally."""
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS name")
        with open(_manifests()[0], "rb") as handle:
            entries, _native = read_manifest_file_entries(handle.read())
        schema, _ = _fresh_metadata(DATASET)
        sizes = entries[0].column_uncompressed_sizes_in_bytes
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
        with open(_manifests()[0], "rb") as handle:
            before, _native = read_manifest_file_entries(handle.read())

        _run("DROP STATISTICS ON testdata.satellites FOR COLUMNS name")
        with open(_manifests()[0], "rb") as handle:
            after, _native = read_manifest_file_entries(handle.read())

        assert (
            after[0].column_uncompressed_sizes_in_bytes
            == before[0].column_uncompressed_sizes_in_bytes
        )
        assert after[0].uncompressed_size_in_bytes == before[0].uncompressed_size_in_bytes
    finally:
        _clean()


def _scalar(sql):
    """First column of the first row of `sql`."""
    session = opteryx.session()
    rows = None
    for morsel in session.execute_to_morsels(sql):
        rows = morsel.to_arrow().to_pylist()
    return rows[0][session.column_names[0]]


def test_dense_size_is_the_values_not_the_encoded_pages():
    """get_total_uncompressed_size reports the parquet footer's
    total_uncompressed_size — the DECOMPRESSED size of the ENCODED pages, i.e.
    the dictionary page plus its index values. Billing charges dense-equivalent
    bytes, which for a low-cardinality column is very much larger. The two
    numbers are asserted apart in BOTH directions on purpose: a
    low-cardinality column is dense-LARGER (dictionary encoding pays off), and
    a fully-distinct one is dense-SMALLER (the footer figure carries offset
    framing the dense figure excludes). Neither bounds the other, so neither
    may be quietly substituted for the other."""
    _clean_nullable()
    try:
        _run(f"ANALYZE TABLE {NULLABLE_DATASET}")
        _, manifest = _astronauts_metadata()

        # gender: 2 distinct values over 357 rows — the case dictionary
        # encoding is built for, and the case billing under-charged worst.
        assert manifest.get_total_dense_size("gender") > (
            10 * manifest.get_total_uncompressed_size("gender")
        )
        # name: distinct per row, nothing to dictionary-encode.
        assert (
            manifest.get_total_dense_size("name")
            < manifest.get_total_uncompressed_size("name")
        )
    finally:
        _clean_nullable()


def test_dense_size_matches_the_columns_actual_value_bytes():
    """The string arm is ANALYZE's char_total_bytes, and it must equal what
    summing the values themselves says — values only, with no per-row offset or
    validity framing added, and NULL rows contributing nothing."""
    _clean_nullable()
    try:
        _run(f"ANALYZE TABLE {NULLABLE_DATASET}")
        schema, manifest = _astronauts_metadata()

        string_columns = [
            c.name
            for c in schema.columns
            if c.column_type.category
            in (LogicalCategory.VARCHAR, LogicalCategory.NVARCHAR, LogicalCategory.VARBINARY)
        ]
        assert string_columns, "fixture no longer has string columns to check"
        for name in string_columns:
            measured = _scalar(f'SELECT SUM(LENGTH("{name}")) FROM {NULLABLE_DATASET}')
            assert manifest.get_total_dense_size(name) == measured, name
    finally:
        _clean_nullable()


def test_dense_size_fixed_width_arm_needs_no_analyze():
    """Deriving rather than storing buys this: record_count alone answers a
    fixed-width column, so the fixed-width arm works on a manifest no stats
    pass has ever touched. Only the string arm needs ANALYZE to have run."""
    _clean_nullable()
    try:
        _, manifest = _astronauts_metadata()
        rows = manifest.get_record_count()
        assert rows > 0

        assert manifest.get_total_dense_size("year") == rows * 8  # INT64
        assert manifest.get_total_dense_size("group") == rows * 8  # FLOAT64
        assert manifest.get_total_dense_size("birth_date") == rows * 4  # DATE32
        # No ANALYZE — the string arm has no input, and says so.
        assert manifest.get_total_dense_size("gender") is None
    finally:
        _clean_nullable()


def test_dense_size_charges_null_rows_their_fixed_width_slot():
    """A dense vector allocates a slot whether or not it holds a value, so a
    mostly-null fixed-width column costs the same as a full one. death_date is
    ~95% null in this fixture and must still bill every row."""
    _clean_nullable()
    try:
        _run(f"ANALYZE TABLE {NULLABLE_DATASET}")
        _, manifest = _astronauts_metadata()
        rows = manifest.get_record_count()
        assert manifest.estimate_null_fraction("death_date") > 0.5
        assert manifest.get_total_dense_size("death_date") == rows * 4
    finally:
        _clean_nullable()


def test_dense_size_is_none_never_zero_for_unmeasured_types():
    """ARRAY (and BOOL, VARIANT, VECTOR_FP16, NULL) have no dense measure
    recorded anywhere yet. None means UNKNOWN. A consumer that reads it as zero
    bills a column nothing because nobody measured it — a silent revenue error,
    which is why this must never be softened to 0."""
    _clean_nullable()
    try:
        _run(f"ANALYZE TABLE {NULLABLE_DATASET}")
        schema, manifest = _astronauts_metadata()
        arrays = [
            c.name for c in schema.columns if c.column_type.category == LogicalCategory.ARRAY
        ]
        assert arrays, "fixture no longer has an ARRAY column to check"
        for name in arrays:
            assert manifest.get_total_dense_size(name) is None, name
            # ... and the footer still answers for it, so the two accessors are
            # genuinely independent rather than one wrapping the other.
            assert manifest.get_total_uncompressed_size(name) > 0, name
    finally:
        _clean_nullable()


def test_dense_size_and_footer_size_both_survive():
    """The encoded-size accessor is what the PLANNER wants (an I/O and decode
    cost proxy) and the dense one is what BILLING wants. Two different numbers,
    both live — this test fails the moment one replaces the other."""
    _clean_nullable()
    try:
        _run(f"ANALYZE TABLE {NULLABLE_DATASET}")
        _, manifest = _astronauts_metadata()
        for name in ("gender", "year", "name"):
            assert manifest.get_total_uncompressed_size(name) is not None, name
            assert manifest.get_total_dense_size(name) is not None, name
            assert manifest.get_total_dense_size(name) != manifest.get_total_uncompressed_size(
                name
            ), name
    finally:
        _clean_nullable()


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
