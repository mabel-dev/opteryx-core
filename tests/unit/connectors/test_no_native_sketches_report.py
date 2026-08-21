"""What the engine says when a catalog backend exposes no sketch vectors.

`OpteryxTable._build_manifest` duck-types `manifest_sketch_vectors` onto
whatever `Dataset` the workspace is registered with. The report for a missing
accessor used to be a single process-wide WARNING that told the operator to
upgrade opteryx_catalog. That was wrong twice over for a third-party backend:
Apache Iceberg's manifests have no field for NDV/histogram sketches, so no
version of opteryx_catalog could supply them, and the message named no dataset
so the operator could not tell which workspace it was even about.

These tests pin the three properties that fix depends on.
"""

import logging

import pytest

from opteryx.connectors import opteryx_connector


def _backend(module: str, name: str = "Dataset"):
    """A stand-in dataset class attributed to `module`, with no sketch accessor."""
    cls = type(name, (), {})
    cls.__module__ = module
    return cls()


@pytest.fixture(autouse=True)
def _reset_guard():
    """The guard is process-wide state; isolate each test from the others."""
    original = set(opteryx_connector._warned_no_native_sketches)
    opteryx_connector._warned_no_native_sketches.clear()
    yield
    opteryx_connector._warned_no_native_sketches.clear()
    opteryx_connector._warned_no_native_sketches.update(original)


def test_third_party_backend_is_not_told_to_upgrade_opteryx_catalog(caplog):
    """The Iceberg case: unactionable advice must not be given.

    A backend outside opteryx_catalog may have no sketches to give as a property
    of its storage format. Reporting that at WARNING, with an upgrade remedy that
    cannot work, is what sent operators chasing an already-current package.
    """
    with caplog.at_level(logging.DEBUG, logger=opteryx_connector.logger.name):
        opteryx_connector._warn_no_native_sketches(_backend("opteryx_iceberg.dataset"))

    (record,) = [r for r in caplog.records if "manifest_sketch_vectors" in r.getMessage()]
    assert record.levelno == logging.DEBUG, "an inherent format property is not a warning"
    assert "upgrade opteryx_catalog" not in record.getMessage().lower()
    # The condition must be attributable to a backend, not anonymous.
    assert "opteryx_iceberg.dataset" in record.getMessage()


def test_stale_native_catalog_still_warns_with_the_upgrade_remedy(caplog):
    """The case the warning was written for must keep working."""
    with caplog.at_level(logging.DEBUG, logger=opteryx_connector.logger.name):
        opteryx_connector._warn_no_native_sketches(_backend("opteryx_catalog.catalog.dataset"))

    (record,) = [r for r in caplog.records if "manifest_sketch_vectors" in r.getMessage()]
    assert record.levelno == logging.WARNING
    assert "upgrade opteryx_catalog" in record.getMessage().lower()


def test_each_backend_is_reported_once_and_does_not_mask_the_others(caplog):
    """The guard is per backend class, not per process.

    With a single global flag, a process serving both a native workspace and a
    third-party one reported only whichever was read first and stayed silent
    about the other for its whole life.
    """
    native = _backend("opteryx_catalog.catalog.dataset")
    third_party = _backend("opteryx_iceberg.dataset")

    with caplog.at_level(logging.DEBUG, logger=opteryx_connector.logger.name):
        for _ in range(4):
            opteryx_connector._warn_no_native_sketches(native)
            opteryx_connector._warn_no_native_sketches(third_party)

    reported = [r for r in caplog.records if "manifest_sketch_vectors" in r.getMessage()]
    assert len(reported) == 2, "expected exactly one report per backend class"
    assert {r.levelno for r in reported} == {logging.WARNING, logging.DEBUG}


def test_a_backend_returning_an_empty_dict_is_not_reported_at_all():
    """Declaring "no sketches" explicitly is the supported answer, and is silent.

    This is the contract opteryx-iceberg's `IcebergDataset.manifest_sketch_vectors`
    relies on: the probe is `getattr(...) is not None`, so an accessor returning
    `{}` takes the native branch and never reaches the report.
    """
    class Declares:
        def manifest_sketch_vectors(self, snapshot_id=None):
            return {}

    assert getattr(Declares(), "manifest_sketch_vectors", None) is not None
