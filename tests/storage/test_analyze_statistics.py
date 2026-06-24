"""
ANALYZE … FOR COLUMNS / DROP STATISTICS — statistics sidecar lifecycle.

ANALYZE computes per-file KMV sketches and writes the `.stats.json` sidecar the
scan loads; DROP STATISTICS removes them. NDV estimates are advisory (planning
only) so there is no correctness risk — these tests assert the sidecar lifecycle
and that the estimator lights up.
"""

import glob
import json
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import connector_factory

DATASET = "testdata.satellites"
_SIDECAR_GLOB = "testdata/satellites/*.stats.json"


def _clean():
    for p in glob.glob(_SIDECAR_GLOB):
        os.remove(p)


def _run(sql):
    list(opteryx.session().execute_to_morsels(sql))


def _sidecars():
    return glob.glob(_SIDECAR_GLOB)


def _metadata():
    eng = connector_factory(DATASET, None).table_engine(DATASET, telemetry=None)
    return eng.get_dataset_metadata()


def test_analyze_for_columns_writes_scoped_sidecar():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites FOR COLUMNS planetId, gm")
        sidecars = _sidecars()
        assert len(sidecars) == 1, sidecars
        payload = json.load(open(sidecars[0]))
        # field_ids must cover the FULL schema (loader staleness check)...
        schema, _ = _metadata()
        assert len(payload["field_ids"]) == len(schema.columns)
        # ...but only the named columns are sketched.
        assert len(payload["min_k_hashes"]) == 2
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
        payload = json.load(open(_sidecars()[0]))
        assert len(payload["min_k_hashes"]) == 1  # gm survives

        _run("DROP STATISTICS ON testdata.satellites")
        assert _sidecars() == []
    finally:
        _clean()


def test_drop_statistics_is_idempotent():
    _clean()
    try:
        # No sidecars present — dropping is a success, not an error.
        _run("DROP STATISTICS ON testdata.satellites")
        assert _sidecars() == []
    finally:
        _clean()


def test_bare_analyze_covers_all_columns():
    _clean()
    try:
        _run("ANALYZE TABLE testdata.satellites")
        payload = json.load(open(_sidecars()[0]))
        assert len(payload["min_k_hashes"]) == len(payload["field_ids"])
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
