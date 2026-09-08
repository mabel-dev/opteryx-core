# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The provenance receipt a write hands the catalog.

opteryx-catalog PROVENANCE_DESIGN.md S3: every catalog read settles its
snapshot during binding, so the receipt - which relation, which snapshot, how
it was chosen - is a by-product of the bound plan. The binder walks the Scan
nodes and the insert/merge sinks pass what it found to the connector beside
`author`.

The connector here is catalog-shaped on the READ side (a real `OpteryxTable`
over a fake catalog, so `_resolve_snapshot` runs for real) and in-memory on
the WRITE side, recording exactly what `insert`/`replace_relation` were
handed. What is under test is the receipt, not the rows.
"""

import datetime
from types import SimpleNamespace

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import Writable
from opteryx.connectors.opteryx_connector import OpteryxTable
from opteryx.models.file_entry import FileEntry

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]

_COLUMNS = [{"name": "id", "type": "INTEGER", "id": 1}]

_HEAD = 7284091337
_OLDER = 7283449002
_T_OLDER = 1786421691019
_T_HEAD = 1786587302881


def _snapshot(snapshot_id, timestamp_ms, parent=None):
    return SimpleNamespace(
        snapshot_id=snapshot_id,
        timestamp_ms=timestamp_ms,
        author="seed",
        user_created=True,
        sequence_number=None,
        manifest_list=f"metadata/manifest-{snapshot_id}.parquet",
        operation_type="append",
        parent_snapshot_id=parent,
        schema_id="sch-0001",
        commit_message=None,
        summary={},
    )


class _Dataset:
    """A catalog dataset with the history given and nothing to scan - the
    receipt is about which snapshot was RESOLVED, and an empty scan resolves
    one exactly as a full one does."""

    bounds_are_ordinal = True

    def __init__(self, identifier, history):
        self.identifier = identifier
        self._history = history
        self.metadata = SimpleNamespace(
            current_snapshot_id=_HEAD if history else None,
            current_schema_id="sch-0001",
            location=f"gs://bucket/ws/{identifier.replace('.', '/')}",
            snapshots=list(history),
        )

    def snapshot(self, snapshot_id=None, user_only=False):
        if not self._history:
            return None
        if snapshot_id is None:
            return next(s for s in self._history if s.snapshot_id == _HEAD)
        return next((s for s in self._history if s.snapshot_id == snapshot_id), None)

    def snapshots(self):
        return list(self._history)

    def previous_user_snapshot(self):
        return next((s for s in self._history if s.snapshot_id == _OLDER), None)

    def scan(self, snapshot_id=None):
        return []

    def schema(self, schema_id=None):
        return SimpleNamespace(columns=list(_COLUMNS), name=self.identifier)


_HISTORY = [_snapshot(_OLDER, _T_OLDER), _snapshot(_HEAD, _T_HEAD, parent=_OLDER)]


class _Catalog:
    """`ops.src` has two commits; `ops.fresh` has a schema and none."""

    def __init__(self, workspace=None, **kwargs):
        self.workspace = workspace or "cat"

    def load_dataset(self, identifier, load_history=False):
        from opteryx_catalog.exceptions import DatasetNotFound

        if identifier == "ops.src":
            return _Dataset(identifier, _HISTORY)
        if identifier in ("ops.fresh", "ops.sink"):
            return _Dataset(identifier, [])
        raise DatasetNotFound(identifier)

    def resolve_tag(self, identifier, tag):
        from opteryx_catalog.exceptions import TagNotFound

        if identifier == "ops.src" and tag == "month_end":
            return _OLDER
        raise TagNotFound(tag)


class _Connector(BaseConnector, Writable):
    """Reads through a real OpteryxTable; writes record what they were handed."""

    commits = []
    # The binder hands `version`/`version_tag`/`at_date` to `table_engine` only
    # for a connector that says it can travel in time.
    supports_diachronic = True
    supports_version_travel = True

    def __init__(self, **kwargs):
        self.telemetry = kwargs.get("telemetry")

    def relation_exists(self, relation_name):
        return relation_name in ("cat.ops.src", "cat.ops.fresh", "cat.ops.sink")

    def relation_column_names(self, relation_name):
        return [column["name"] for column in _COLUMNS]

    def table_engine(self, name, **kwargs):
        _, relative = name.split(".", 1)
        return OpteryxTable(
            dataset=relative,
            catalog=_Catalog(),
            workspace="cat",
            telemetry=kwargs.get("telemetry"),
            **{
                key: kwargs[key]
                for key in ("version", "version_tag", "at_date", "start_date", "end_date")
                if key in kwargs
            },
        )

    def write_morsel(self, relation_name, morsel):
        return FileEntry(
            file_path=f"memory://{relation_name}/{len(_Connector.commits)}",
            file_format="PARQUET",
            record_count=len(morsel),
            file_size_in_bytes=1,
        )

    def create_relation(self, relation_name, schema, author=None):
        pass

    def insert(self, relation_name, file_entries, author=None, commit_message=None, read_sources=None, produced_by=None):
        _Connector.commits.append(("insert", relation_name, read_sources, produced_by))

    def replace_relation(self, relation_name, schema, file_entries, author=None, commit_message=None, read_sources=None, produced_by=None):
        _Connector.commits.append(("replace", relation_name, read_sources, produced_by))


@pytest.fixture
def workspace():
    _Connector.commits = []
    register_workspace("cat", _Connector)
    return _Connector


def _run(statement):
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels(statement))


def _receipt():
    assert len(_Connector.commits) == 1, _Connector.commits
    return _Connector.commits[0]


# --- what the receipt says


def test_insert_values_reads_no_catalog_relation(workspace):
    """`[]`, not None: the statement read nothing, and the binder knows it."""
    _run("INSERT INTO cat.ops.sink VALUES (1), (2)")

    _, _, read_sources, produced_by = _receipt()
    assert read_sources == []
    assert produced_by is None


def test_a_select_over_a_virtual_dataset_reads_no_catalog_relation(workspace):
    _run("INSERT INTO cat.ops.sink SELECT id FROM $planets")

    assert _receipt()[2] == []


def test_a_read_of_the_head_is_recorded_as_current(workspace):
    _run("INSERT INTO cat.ops.sink SELECT id FROM cat.ops.src")

    assert _receipt()[2] == [
        {"dataset": "cat.ops.src", "snapshot-id": _HEAD, "resolved-by": "current"}
    ]


def test_version_as_of_is_recorded_as_version(workspace):
    _run(f"INSERT INTO cat.ops.sink SELECT id FROM cat.ops.src VERSION AS OF {_OLDER}")

    assert _receipt()[2] == [
        {"dataset": "cat.ops.src", "snapshot-id": _OLDER, "resolved-by": "version"}
    ]


def test_version_as_of_previous_is_recorded_as_previous(workspace):
    _run("INSERT INTO cat.ops.sink SELECT id FROM cat.ops.src VERSION AS OF PREVIOUS")

    assert _receipt()[2] == [
        {"dataset": "cat.ops.src", "snapshot-id": _OLDER, "resolved-by": "previous"}
    ]


def test_a_tag_is_recorded_as_tag(workspace):
    _run("INSERT INTO cat.ops.sink SELECT id FROM cat.ops.src VERSION AS OF month_end")

    assert _receipt()[2] == [
        {"dataset": "cat.ops.src", "snapshot-id": _OLDER, "resolved-by": "tag"}
    ]


def test_the_virtual_current_tag_is_recorded_as_current(workspace):
    _run("INSERT INTO cat.ops.sink SELECT id FROM cat.ops.src VERSION AS OF current")

    assert _receipt()[2][0]["resolved-by"] == "current"


def test_a_point_in_time_read_is_recorded_as_date(workspace):
    # Six hours after the older commit and forty before the head: inside the
    # window whichever zone the literal is read in, and past second rounding.
    at = datetime.datetime.fromtimestamp((_T_OLDER + 6 * 3_600_000) / 1000, tz=datetime.timezone.utc)
    _run(
        "INSERT INTO cat.ops.sink SELECT id FROM cat.ops.src "
        f"TIMESTAMP AS OF '{at.strftime('%Y-%m-%d %H:%M:%S')}'"
    )

    assert _receipt()[2] == [
        {"dataset": "cat.ops.src", "snapshot-id": _OLDER, "resolved-by": "date"}
    ]


def test_a_relation_with_nothing_committed_is_recorded_with_no_version(workspace):
    """It was read. It had nothing. Both are facts about this commit."""
    _run("INSERT INTO cat.ops.sink SELECT id FROM cat.ops.fresh")

    assert _receipt()[2] == [
        {"dataset": "cat.ops.fresh", "snapshot-id": None, "resolved-by": "current"}
    ]


def test_one_relation_read_twice_at_one_version_is_one_entry(workspace):
    _run("INSERT INTO cat.ops.sink SELECT a.id FROM cat.ops.src a JOIN cat.ops.src b ON a.id = b.id")

    assert len(_receipt()[2]) == 1


def test_one_relation_read_at_two_versions_is_two_entries(workspace):
    _run(
        "INSERT INTO cat.ops.sink SELECT a.id FROM cat.ops.src a "
        f"JOIN cat.ops.src VERSION AS OF {_OLDER} b ON a.id = b.id"
    )

    assert sorted(e["snapshot-id"] for e in _receipt()[2]) == [_OLDER, _HEAD]


def test_a_relation_read_only_inside_a_subquery_is_in_the_receipt(workspace):
    _run(
        "INSERT INTO cat.ops.sink SELECT id FROM $planets "
        "WHERE id IN (SELECT id FROM cat.ops.src)"
    )

    assert [e["dataset"] for e in _receipt()[2]] == ["cat.ops.src"]


def test_a_replace_carries_the_receipt_too(workspace):
    _run("CREATE OR REPLACE TABLE cat.ops.sink AS SELECT id FROM cat.ops.src")

    kind, _, read_sources, _ = _receipt()
    assert kind == "replace"
    assert read_sources[0]["dataset"] == "cat.ops.src"


# --- the producer


def test_a_hand_run_statement_has_no_producer(workspace):
    _run("INSERT INTO cat.ops.sink SELECT id FROM cat.ops.src")

    assert _receipt()[3] is None


def test_the_binder_names_the_task_an_execute_expanded():
    from opteryx.planner.binder.relation import _produced_by

    assert _produced_by(SimpleNamespace(executing_task="ops.t", relation_name="ops.sink")) == "task:ops.t"


def test_the_binder_names_the_view_a_refresh_populates():
    from opteryx.planner.binder.relation import _produced_by

    node = SimpleNamespace(is_refresh=True, relation_name="cat.mart.v")
    assert _produced_by(node) == "view:cat.mart.v"
    node = SimpleNamespace(is_materialized_view=True, relation_name="cat.mart.v")
    assert _produced_by(node) == "view:cat.mart.v"


def test_a_task_outranks_the_view_flags_as_producer():
    """A task whose statement is REFRESH lands as the task: it is the thing a
    person can point at, and its `reads` is what the receipt is checked against."""
    from opteryx.planner.binder.relation import _produced_by

    node = SimpleNamespace(executing_task="ops.t", is_refresh=True, relation_name="cat.mart.v")
    assert _produced_by(node) == "task:ops.t"


# --- version skew against the catalog


class _OldDataset:
    def add_files(self, files, author=None, commit_message=None):
        self.called = {"files": files, "author": author}
        return None


class _NewDataset:
    def add_files(self, files, author=None, commit_message=None, read_sources=None, produced_by=None):
        self.called = {"read_sources": read_sources, "produced_by": produced_by}
        return None


def _connector_over(dataset):
    from opteryx.connectors.opteryx_connector import OpteryxConnector

    connector = OpteryxConnector.__new__(OpteryxConnector)
    # The real parser splits `workspace.collection.name`; names reach the
    # connector already carrying the workspace `connector_factory` dispatched on.
    connector._parse_identifier = lambda name: tuple(name.split(".", 1))
    return connector


def test_the_receipt_is_dropped_for_a_catalog_that_cannot_take_it():
    """A `TypeError` from a commit is a write that reported failure after its
    files landed. An older catalog gets no receipt, and reports the gap itself
    once it is upgraded."""
    dataset = _OldDataset()
    connector = _connector_over(dataset)

    kwargs = connector._provenance_kwargs(dataset.add_files, [("cat.ops.src", 1)], "task:ops.t")

    assert kwargs == {}


def test_the_receipt_is_passed_to_a_catalog_that_takes_it():
    dataset = _NewDataset()
    connector = _connector_over(dataset)

    kwargs = connector._provenance_kwargs(dataset.add_files, [{"dataset": "cat.ops.src"}], "task:cat.ops.t")

    assert kwargs == {"read_sources": [{"dataset": "cat.ops.src"}], "produced_by": "task:cat.ops.t"}


def test_an_unreported_receipt_stays_none_rather_than_becoming_empty():
    """`[]` is an assertion only the binder may make."""
    dataset = _NewDataset()
    connector = _connector_over(dataset)

    kwargs = connector._provenance_kwargs(dataset.add_files, None, None)

    assert kwargs == {"read_sources": None, "produced_by": None}


def test_the_producer_is_qualified_by_the_connector():
    connector = _connector_over(_NewDataset())

    assert connector._qualified_producer("task:cat.ops.t") == "task:cat.ops.t"
    assert connector._qualified_producer("view:cat.mart.v") == "view:cat.mart.v"
    assert connector._qualified_producer(None) is None
