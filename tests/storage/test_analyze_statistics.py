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
from opteryx.models.manifest_io import DATASET_MANIFEST_NAME
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
