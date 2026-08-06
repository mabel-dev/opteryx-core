"""GCS parquet via the CATALOG — the engine's production read path.

Storage architecture: **local = ad-hoc, GCS = catalog-driven**. Ad-hoc local
(`FileSystemConnector`) is covered heavily by the `make q` battery, and ad-hoc GCS by
`test_documentation.py::test_readme_4` — but the catalog path, which is what real traffic
actually runs on, had NO end-to-end test at all: `test_opteryx_connector_catalog_factory.py`
only exercises factory wiring, and `tests/integration/worker/test_worker.py` is a script
(no test functions) whose only SQL is `SELECT 1`. Nothing proved the engine could read a
row from GCS through the catalog.

Two DIFFERENT engine paths are covered here, and conflating them is the trap:

* **the scan** — reads parquet out of GCS. Proven only by asserting on decoded VALUES.
* **the manifest short-circuit** — a bare `SELECT COUNT(*)` is rewritten to a literal from
  the catalog manifest's row counts by `StatisticsOnlyResponseStrategy`, reading no parquet
  at all (measured: zero object-storage requests). Worth testing in its own right — a wrong
  manifest count is a silently wrong answer — but it proves NOTHING about reading.

So a bare `COUNT(*)` must never be used as evidence the scan works: it would pass green with
the GCS read completely broken. The COUNT(*) tests below therefore assert the served PATH
(via telemetry), not just the number, and one test pins the manifest count against a real
read so the two cannot drift apart unnoticed.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

# The catalog is a sibling repo, not a package dependency (same as the worker script does).
_CATALOG_REPO = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "opteryx-catalog")
)
if os.path.isdir(_CATALOG_REPO) and _CATALOG_REPO not in sys.path:
    sys.path.insert(1, _CATALOG_REPO)

# Importing opteryx also loads .env via dotenv — which is where the GCP credentials
# (GOOGLE_APPLICATION_CREDENTIALS) and the catalog identifiers below come from. Import it
# before reading os.environ.
import opteryx  # noqa: E402
from opteryx.connectors import OpteryxConnector  # noqa: E402

_REQUIRED_ENV = ("GCP_PROJECT_ID", "FIRESTORE_DATABASE", "GCS_BUCKET")

# `opteryx.<collection>.<dataset>` — the catalog resolves workspace.collection.dataset.
WORKSPACE = "opteryx"
PLANETS = f"{WORKSPACE}.test.planets"
MISSIONS = f"{WORKSPACE}.test.space_missions"


def _why_unavailable():
    try:
        import opteryx_catalog  # noqa: F401
    except ImportError:
        return f"opteryx_catalog not importable (expected sibling repo at {_CATALOG_REPO})"
    missing = [k for k in _REQUIRED_ENV if not os.environ.get(k)]
    if missing:
        return f"catalog env not configured: {', '.join(missing)}"
    return None


_UNAVAILABLE = _why_unavailable()

# Skips only when the catalog/credentials genuinely aren't reachable (e.g. a contributor
# without GCP access). The reason is explicit so a silent skip can't be mistaken for a pass.
pytestmark = pytest.mark.skipif(_UNAVAILABLE is not None, reason=str(_UNAVAILABLE))


@pytest.fixture(scope="module")
def catalog_connector():
    """Point the DEFAULT connector at the catalog, restoring global state afterwards.

    `set_default_connector` is process-global and catches every dataset whose name matches
    no registered prefix — including the `testdata.*` datasets the rest of the suite reads
    from local disk. Left set, it would silently reroute those to the catalog. The
    `_storage_prefixes` save/restore matters too: `test_documentation.py::test_readme_4`
    registers the prefix `"opteryx"` for ad-hoc GCS, which — if it ran first in the same
    session — would capture this module's `opteryx.test.*` datasets and route them down the
    ad-hoc path instead of the catalog, testing the wrong thing entirely.
    """
    from opteryx_catalog import OpteryxCatalog
    import opteryx.connectors as connectors

    saved_default = connectors._default_connector
    saved_prefixes = dict(connectors._storage_prefixes)
    saved_cache = dict(connectors._connector_cache)

    connectors._storage_prefixes.pop(WORKSPACE, None)
    connectors._connector_cache.clear()

    opteryx.set_default_connector(
        OpteryxConnector,
        catalog=OpteryxCatalog,
        firestore_project=os.environ["GCP_PROJECT_ID"],
        firestore_database=os.environ["FIRESTORE_DATABASE"],
        gcs_bucket=os.environ["GCS_BUCKET"],
    )
    try:
        yield
    finally:
        connectors._default_connector = saved_default
        connectors._storage_prefixes.clear()
        connectors._storage_prefixes.update(saved_prefixes)
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved_cache)


@pytest.fixture(scope="module")
def catalog_session(catalog_connector):
    """A shared session for value assertions. Tests that read TELEMETRY must not use this —
    telemetry accumulates across a session's queries, so those build their own."""
    return opteryx.session()


def _rows(session, sql):
    out = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(morsel[i])
    return out


def test_catalog_scan_decodes_gcs_parquet_values(catalog_session):
    # The core proof: real values decoded out of a parquet file in GCS, via the catalog.
    names = [r[0] for r in _rows(catalog_session, f"SELECT name FROM {PLANETS}")]
    assert names == [
        "Mercury",
        "Venus",
        "Earth",
        "Mars",
        "Jupiter",
        "Saturn",
        "Uranus",
        "Neptune",
        "Pluto",
    ], names


def test_catalog_scan_applies_a_pushed_predicate(catalog_session):
    # Exercises the pushed-predicate path (row-group pruning + residual filter) against
    # real GCS data, not just a full read.
    names = [r[0] for r in _rows(catalog_session, f"SELECT name FROM {PLANETS} WHERE id > 5")]
    assert names == ["Saturn", "Uranus", "Neptune", "Pluto"], names


def test_catalog_scan_projects_a_subset_of_columns(catalog_session):
    # Projection pushdown: only the named columns are decoded, and they must line up.
    rows = _rows(catalog_session, f"SELECT name, id FROM {PLANETS} WHERE id = 3")
    assert rows == [("Earth", 3)], rows


def test_catalog_scan_reads_a_wider_dataset(catalog_session):
    # A second, wider/multi-column dataset guards against a planets-shaped fluke.
    rows = _rows(catalog_session, f"SELECT Mission FROM {MISSIONS} LIMIT 5")
    assert len(rows) == 5, rows
    assert all(isinstance(r[0], str) and r[0] for r in rows), rows


def test_catalog_scan_aggregates_over_gcs_data(catalog_session):
    # An aggregate whose answer can only come from decoded rows — unlike a bare COUNT(*),
    # which the optimizer answers from manifest statistics without reading any parquet.
    rows = _rows(catalog_session, f"SELECT COUNT(*) FROM {PLANETS} WHERE id > 5")
    assert rows == [(4,)], rows


# ── COUNT(*) — the manifest-statistics path ────────────────────────────────────────────
# A bare COUNT(*) is NOT a scan: the optimizer (StatisticsOnlyResponseStrategy) rewrites it
# to a literal from the catalog manifest's row counts, reading no parquet at all. That is a
# real engine path in its own right and is tested here — but it is a DIFFERENT path from the
# scans above, and must never be mistaken for proof that reading works.


@pytest.mark.parametrize(
    "dataset, expected",
    [
        ("planets", 9),
        ("satellites", 177),
        ("astronauts", 357),
        ("space_missions", 4630),
    ],
)
def test_count_star_returns_the_correct_count(catalog_session, dataset, expected):
    rows = _rows(catalog_session, f"SELECT COUNT(*) FROM {WORKSPACE}.test.{dataset}")
    assert rows == [(expected,)], rows


def test_count_star_is_served_from_the_manifest_without_reading_gcs(catalog_connector):
    # Pins the optimisation itself, not just the answer: a bare COUNT(*) must issue ZERO
    # object-storage requests. If this ever starts scanning, the count stays correct while
    # the query silently gets far more expensive — invisible without asserting the path.
    # Needs its own session: telemetry accumulates across a session's queries.
    session = opteryx.session()
    rows = _rows(session, f"SELECT COUNT(*) FROM {MISSIONS}")
    assert rows == [(4630,)], rows

    telemetry = dict(session.telemetry)
    assert telemetry.get("io_http_request_count", 0) == 0, telemetry


def test_predicated_count_does_read_gcs(catalog_connector):
    # The counterpart: adding a predicate defeats the manifest short-circuit and forces a
    # real read. Proves the zero above is a genuine property of the bare form, not simply
    # that this telemetry counter never gets populated.
    session = opteryx.session()
    rows = _rows(session, f"SELECT COUNT(*) FROM {MISSIONS} WHERE Company = 'RVSN USSR'")
    assert rows and rows[0][0] > 0, rows

    telemetry = dict(session.telemetry)
    assert telemetry.get("io_http_request_count", 0) > 0, telemetry


def test_manifest_count_agrees_with_the_rows_actually_stored(catalog_session):
    # The failure this catches is the nastiest kind: manifest statistics drifting away from
    # the data they describe. COUNT(*) would keep answering confidently from the manifest
    # while disagreeing with what a real read returns — a silently wrong answer, no error.
    counted = _rows(catalog_session, f"SELECT COUNT(*) FROM {PLANETS}")[0][0]
    actually_read = len(_rows(catalog_session, f"SELECT name FROM {PLANETS}"))
    assert counted == actually_read == 9, (counted, actually_read)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
