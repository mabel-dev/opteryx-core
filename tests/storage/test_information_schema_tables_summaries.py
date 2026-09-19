# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`information_schema.tables` for a workspace whose data is NOT in our catalog.

Every metadata column on that table - sort order, snapshot id and sequence,
update time, file/byte/record counts - was derived from a dataset document's
current snapshot. A workspace bound to a connector with no metastore (a
PostgreSQL server) has no dataset document and no snapshot, so all seven read
NULL for relations that queried perfectly: a metadata gap by construction, not
by oversight.

What is pinned here is the second path that closes it - the `Summarisable`
capability on the workspace's DATA connector - and, as much, its edges:

* it is BATCHED. One call per collection, whatever the collection holds. The
  per-relation `load_dataset` that the native path makes is exactly the cost
  that must not be reproduced against a remote server.
* it replaces the catalog round trips rather than adding to them.
* it only answers what a remote relation can honestly answer. `snapshot_id`,
  `snapshot_sequence_id` and `table_file_count` describe a snapshot store and
  stay NULL, as they always have for a view.
* a relation the source said nothing about, and a source that could not be
  reached at all, read exactly as they read before this existed. Listing a
  workspace must not fail because the workspace's server is down - and, in
  particular, must not fail on the credential decrypt that resolving the data
  binding performs, which is the conflation the settings/data resolver split
  exists to keep out.
* a native workspace is untouched: no data connector is asked to summarise
  anything it has not declared it can.
"""

import datetime
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx import connectors
from opteryx.connectors import Resolution
from opteryx.connectors import set_workspace_resolver
from opteryx.connectors import set_workspace_settings_resolver
from opteryx.connectors.capabilities import RelationSummary
from opteryx.connectors.capabilities import Summarisable
from opteryx.connectors.opteryx_connector import OpteryxConnector

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]

_UPDATED = datetime.datetime(2026, 3, 1, 12, 0, tzinfo=datetime.timezone.utc)


def _morsels_to_rows(morsels):
    rows = []
    for morsel in morsels:
        if morsel is None:
            continue
        pydict = morsel.to_arrow().to_pydict()
        count = len(next(iter(pydict.values()))) if pydict else 0
        for index in range(count):
            row = {}
            for key, values in pydict.items():
                value = values[index]
                if isinstance(value, bytes):
                    value = value.decode()
                row[key] = value
            rows.append(row)
    return rows


class _SnapshotlessDataset:
    """What a bound workspace's dataset document is: a projected name with no
    snapshot behind it. Every metadata column derived from `snapshot()` is
    NULL for it, which is the gap the summariser closes."""

    def snapshot(self):
        return None


class _StubCatalog:
    """The SETTINGS catalog for a bound workspace: it knows the NAMES (they
    are projected into it for listing) and has nothing behind them. Loads are
    recorded, because "the catalog was not consulted per relation" is half of
    what the summariser path is for."""

    loads = []

    def __init__(self, workspace=None, **kwargs):
        pass

    def list_collections(self):
        return ["probe"]

    def list_datasets(self, collection):
        return ["results", "runs", "unmeasured"]

    def list_views(self, collection):
        return []

    def load_dataset(self, identifier):
        _StubCatalog.loads.append(identifier)
        return _SnapshotlessDataset()

    def dataset_exists(self, identifier):
        return True

    def get_relation(self, identifier):
        return (None, None)


class _RemoteConnector(Summarisable):
    """A data connector that can describe its own relations, like the real
    PostgreSQL gateway. Records every call so the batching can be asserted."""

    calls = []

    def __init__(self, telemetry=None, **kwargs):
        self.telemetry = telemetry

    def relation_summaries(self, schema_name, relation_names):
        _RemoteConnector.calls.append((schema_name, tuple(relation_names)))
        return {
            "results": RelationSummary(
                record_count=14820,
                record_count_is_estimate=True,
                byte_count=2129920,
                sort_order="checked_at DESC",
                updated_at=_UPDATED,
            ),
            "runs": RelationSummary(record_count=None, byte_count=8192),
            # 'unmeasured' is deliberately absent - a relation the source had
            # nothing to say about.
        }


class _PlainConnector:
    """A data connector with no summarising capability - the native shape."""

    def __init__(self, telemetry=None, **kwargs):
        self.telemetry = telemetry

    def relation_summaries(self, schema_name, relation_names):
        raise AssertionError("a connector that never declared the capability was asked")


class _CredentialFailure(Exception):
    """Stands in for the KMS decrypt that resolving a data binding performs."""


@pytest.fixture
def bound_workspace():
    """A workspace whose SETTINGS resolve to the stub catalog and whose DATA
    resolve wherever the test points `data_connector` - the production shape,
    where the two questions have separate resolvers."""
    saved = (
        connectors._workspace_resolver,
        connectors._workspace_settings_resolver,
        dict(connectors._connector_cache),
        dict(connectors._connector_versions),
    )
    connectors._connector_cache.clear()
    connectors._connector_versions.clear()
    _RemoteConnector.calls = []
    _StubCatalog.loads = []

    state = {"data_connector": _RemoteConnector, "data_error": None}

    def settings_resolver(workspace):
        if workspace != "health":
            return None
        return Resolution(OpteryxConnector, {"catalog": _StubCatalog})

    def data_resolver(workspace):
        if workspace != "health":
            return None
        if state["data_error"] is not None:
            raise state["data_error"]
        return Resolution(state["data_connector"], {})

    set_workspace_settings_resolver(settings_resolver)
    set_workspace_resolver(data_resolver)
    try:
        yield state
    finally:
        set_workspace_resolver(saved[0])
        set_workspace_settings_resolver(saved[1])
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved[2])
        connectors._connector_versions.clear()
        connectors._connector_versions.update(saved[3])


def _read(where=""):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    return {
        row["table_name"]: row
        for row in _morsels_to_rows(
            session.execute_to_morsels(
                f"SELECT * FROM health.information_schema.tables{where}"
            )
        )
    }


def test_remote_metadata_reaches_the_columns(bound_workspace):
    rows = _read()

    results = rows["results"]
    assert results["table_record_count"] == 14820
    assert results["table_bytes"] == 2129920
    assert results["table_sort_order"] == "checked_at DESC"
    assert results["table_updated_at"] == _UPDATED.replace(tzinfo=None)


def test_snapshot_columns_stay_null_for_a_remote_relation(bound_workspace):
    """They describe a snapshot store. A relation living behind a socket has no
    snapshot and no files of ours, so there is nothing truthful to put here -
    the same reason a view has reported NULL for them all along."""
    results = _read()["results"]

    assert results["snapshot_id"] is None
    assert results["snapshot_sequence_id"] is None
    assert results["table_file_count"] is None


def test_a_field_the_source_did_not_answer_is_null(bound_workspace):
    runs = _read()["runs"]

    assert runs["table_bytes"] == 8192
    assert runs["table_record_count"] is None
    assert runs["table_sort_order"] is None
    assert runs["table_updated_at"] is None


def test_a_relation_the_source_did_not_mention_is_still_listed(bound_workspace):
    """Absent from the summary mapping, present in the listing: the catalog
    knows the name, and a missing measurement is not a missing table."""
    rows = _read()

    assert "unmeasured" in rows
    assert rows["unmeasured"]["table_type"] == "BASE TABLE"
    assert rows["unmeasured"]["table_record_count"] is None
    assert rows["unmeasured"]["table_bytes"] is None


def test_the_source_is_asked_once_for_the_whole_collection(bound_workspace):
    """One round trip for three relations - and the per-relation catalog loads
    the native path makes are REPLACED, not joined."""
    _read()

    assert _RemoteConnector.calls == [("probe", ("results", "runs", "unmeasured"))]
    assert _StubCatalog.loads == []


def test_a_pushed_predicate_narrows_what_the_source_is_asked_about(bound_workspace):
    """The names are settled - by predicate and by permission - BEFORE the
    batch call, so a predicate that prunes the listing prunes the remote work
    too rather than measuring rows that will be discarded."""
    rows = _read(" WHERE table_name = 'results'")

    assert set(rows) == {"results"}
    assert _RemoteConnector.calls == [("probe", ("results",))]


def test_an_unreachable_binding_leaves_the_columns_null(bound_workspace):
    """A data binding that will not resolve - a credential gone bad is the
    case this is really about - must not take the LISTING down with it. The
    workspace still has to be browsable, and repairable through SQL."""
    bound_workspace["data_error"] = _CredentialFailure("decrypt failed")

    rows = _read()

    assert set(rows) == {"results", "runs", "unmeasured"}
    assert all(row["table_record_count"] is None for row in rows.values())
    assert all(row["table_bytes"] is None for row in rows.values())
    # Fell back to the catalog, which is what it read before this path existed.
    assert _StubCatalog.loads == ["probe.results", "probe.runs", "probe.unmeasured"]


def test_a_connector_without_the_capability_is_never_asked(bound_workspace):
    """The capability is the whole gate: no isinstance test on a particular
    connector class, and nothing is asked of a connector that has not declared
    it can answer."""
    bound_workspace["data_connector"] = _PlainConnector

    rows = _read()

    assert _StubCatalog.loads == ["probe.results", "probe.runs", "probe.unmeasured"]
    assert all(row["table_sort_order"] is None for row in rows.values())
