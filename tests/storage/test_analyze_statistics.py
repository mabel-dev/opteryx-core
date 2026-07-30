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

DATASET = "testdata.satellites"
_MANIFEST_GLOB = f"testdata/satellites/{DATASET_MANIFEST_NAME}"


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
        manifest.prune_files([_comparison("id", "Gt", 10000)])
        assert manifest.files == []
    finally:
        _clean()


def test_prune_files_wired_from_analyze_manifest_int_column_keeps_in_range():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS id")
        _, manifest = _metadata()

        manifest.prune_files([_comparison("id", "Eq", 1)])
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
        manifest.prune_files([_comparison("gm", "Gt", 1e12)])
        assert manifest.files == []
    finally:
        _clean()


def test_prune_files_wired_from_analyze_manifest_float_column_keeps_in_range():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS gm")
        _, manifest = _metadata()

        manifest.prune_files([_comparison("gm", "Lt", 5000.0)])
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
        manifest.prune_files([_comparison("name", "Eq", "Zzz")])
        assert manifest.files == []
    finally:
        _clean()


def test_prune_files_wired_from_analyze_manifest_varchar_column_keeps_in_range():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS name")
        _, manifest = _metadata()

        manifest.prune_files([_comparison("name", "Eq", "Adrastea")])
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
        manifest.prune_files([_comparison("id", "Gt", 10000)])
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

        manifest.prune_files([_comparison("id", "Gt", 10000)])
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


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
