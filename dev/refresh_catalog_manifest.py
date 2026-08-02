# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Refresh a catalog-backed dataset's manifest statistics directly against
``opteryx_catalog`` — no SQL, no planner, no operator, no opteryx query
session at all. This is the same call ``ANALYZE TABLE`` makes under the hood
(see ``opteryx/operators/table_management/_analyze_catalog.py`` ->
``SimpleDataset.refresh_manifest``), invoked standalone so a long-running
refresh can be run from a VM instead of a container with a request timeout.

Usage:

    python dev/refresh_catalog_manifest.py opteryx.prod.github_events

Requires the same environment ``ANALYZE`` needs in production: GCP
credentials (``GOOGLE_APPLICATION_CREDENTIALS``) plus ``GCP_PROJECT_ID``,
``FIRESTORE_DATABASE``, and ``GCS_BUCKET`` — either exported in the shell or
in a ``.env`` file (``import opteryx`` loads one via dotenv, same as
production and ``tests/integration/test_catalog_gcs_scan.py``).
"""

from __future__ import annotations

import argparse
import getpass
import logging
import os
import sys
import time

sys.path.insert(1, os.path.join(os.path.dirname(__file__), ".."))

# The catalog is a sibling repo, not a package dependency, in dev checkouts
# (same fallback tests/integration/test_catalog_gcs_scan.py uses). In a VM
# where opteryx_catalog is pip-installed this is a no-op.
_CATALOG_REPO = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "pyiceberg-firestore-gcs")
)
if os.path.isdir(_CATALOG_REPO) and _CATALOG_REPO not in sys.path:
    sys.path.insert(1, _CATALOG_REPO)

# Importing opteryx loads .env via dotenv, which is where GCP credentials and
# the catalog identifiers below typically come from. Import before reading
# os.environ. Nothing else about opteryx (session, planner, connector
# registry) is used below - the catalog is driven directly.
import opteryx  # noqa: E402,F401
from opteryx_catalog import OpteryxCatalog  # noqa: E402

_REQUIRED_ENV = ("GCP_PROJECT_ID", "FIRESTORE_DATABASE", "GCS_BUCKET")

log = logging.getLogger("refresh_catalog_manifest")


def _parse_identifier(name: str) -> tuple:
    """Split ``workspace.namespace.dataset`` into (workspace, relative_id),
    matching OpteryxConnector._parse_identifier: split on the FIRST dot."""
    parts = name.split(".", 1)
    if len(parts) != 2:
        raise ValueError(
            f"'{name}' is not a fully qualified dataset name "
            "(expected workspace.namespace.dataset)"
        )
    return parts[0], parts[1]


def refresh(dataset: str, agent: str, author: str) -> None:
    missing = [k for k in _REQUIRED_ENV if not os.environ.get(k)]
    if missing:
        raise RuntimeError(f"missing required environment variable(s): {', '.join(missing)}")

    workspace, relative_id = _parse_identifier(dataset)

    catalog = OpteryxCatalog(
        workspace=workspace,
        firestore_project=os.environ["GCP_PROJECT_ID"],
        firestore_database=os.environ["FIRESTORE_DATABASE"],
        gcs_bucket=os.environ["GCS_BUCKET"],
    )
    table = catalog.load_dataset(relative_id)

    log.info("[%s] refresh_manifest starting (agent=%r, author=%r)", dataset, agent, author)
    started = time.monotonic()
    table.refresh_manifest(agent=agent, author=author)
    elapsed = time.monotonic() - started
    log.info("[%s] refresh_manifest committed in %.1fs", dataset, elapsed)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "dataset",
        nargs="+",
        help="Fully qualified dataset name(s), e.g. opteryx.prod.github_events",
    )
    parser.add_argument(
        "--agent",
        default="opteryx-analyze-manual",
        help="Recorded in the catalog audit log as who/what performed the refresh "
        "(default: opteryx-analyze-manual, distinct from SQL ANALYZE's 'opteryx-analyze').",
    )
    parser.add_argument(
        "--author",
        default=None,
        help="Snapshot author. Defaults to the current OS user; pass an explicit "
        "value or an empty string to leave it unattributed.",
    )
    parser.add_argument(
        "--log-level",
        default="DEBUG",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Root log level. DEBUG (the default) also turns on "
        "opteryx_catalog.catalog.manifest's per-file progress line "
        "(file path, running files-read count, per-file duration) - that "
        "module logs at DEBUG and is otherwise silent; refresh_manifest "
        "itself logs nothing per-file regardless of level.",
    )
    args = parser.parse_args()
    author = getpass.getuser() if args.author is None else (args.author or None)

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        stream=sys.stdout,
        force=True,
    )

    # Fail fast: per _analyze_catalog.py's docstring, refresh_manifest commits
    # a single new snapshot at the end (not verified here - opteryx_catalog is
    # a sibling repo, not vendored in this tree) - so on failure there is
    # nothing to catch or clean up. Let it fail loud and rerun.
    for dataset in args.dataset:
        refresh(dataset, agent=args.agent, author=author)


if __name__ == "__main__":
    main()
