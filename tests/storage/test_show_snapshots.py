# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""SHOW SNAPSHOTS FOR <table> - a relation's commit history.

Driven through a fake catalog rather than a live one: what is under test is
the shape the engine produces from a history, and the fact that it reads the
history at all (with `load_history=True` - the loader returns only the current
snapshot without it, which would silently report a one-entry history for every
relation).
"""

import datetime
from types import SimpleNamespace

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx.exceptions import UnsupportedSyntaxError

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]

# 2026-08-10T04:14:51.019Z and two later commits, deliberately NOT in
# chronological order in the catalog's own list - the connector sorts.
_T0 = 1786421691019
_T1 = 1786500897203
_T2 = 1786587302881


def _snapshot(
    snapshot_id,
    timestamp_ms,
    parent=None,
    operation="append",
    author="mv-refresh@opteryx",
    user_created=False,
    sequence_number=None,
    commit_message=None,
    summary=None,
):
    return SimpleNamespace(
        snapshot_id=snapshot_id,
        timestamp_ms=timestamp_ms,
        author=author,
        user_created=user_created,
        sequence_number=sequence_number,
        manifest_list=f"metadata/manifest-{snapshot_id}.parquet",
        operation_type=operation,
        parent_snapshot_id=parent,
        schema_id="sch-0004",
        commit_message=commit_message,
        summary={} if summary is None else summary,
    )


_HISTORY = [
    _snapshot(
        7283449002,
        _T0,
        parent=None,
        operation="overwrite",
        author="justin.joyce@joocer.com",
        user_created=True,
        sequence_number=4468,
        commit_message="backfill 2024 partitions",
        summary={
            "added-records": 1607774,
            "added-data-files": 7,
            "added-files-size": 132656086,
            "deleted-records": 0,
            "deleted-data-files": 0,
            "deleted-files-size": 0,
            "total-records": 183611884,
            "total-data-files": 1377,
            "total-files-size": 43220881004,
        },
    ),
    # Newest, listed in the middle: the catalog does not promise an order.
    _snapshot(
        7284091337,
        _T2,
        parent=7283774102,
        sequence_number=4471,
        summary={"added-records": 316273, "total-records": 184220391},
    ),
    _snapshot(
        7283774102,
        _T1,
        parent=7283449002,
        sequence_number=4470,
        summary={"added-records": 292234, "total-records": 183904118},
    ),
]


class _FakeDataset:
    """One catalog dataset. `snapshots()` is empty unless the loader was asked
    for history, mirroring the real loader - a connector that forgets
    `load_history=True` must not quietly see a truncated history."""

    def __init__(self, history, with_history, previous_id=None):
        self._history = history
        self._with_history = with_history
        self._previous_id = previous_id
        self.metadata = SimpleNamespace(
            current_snapshot_id=7284091337 if history else None
        )

    def previous_user_snapshot(self):
        """Stubbed to a configured id, NOT reimplemented here.

        The walk that skips commits which changed no rows lives in the catalog
        and is tested there. A second copy of it in this fake would be a second
        place for it to drift, and these tests are about what SHOW SNAPSHOTS
        renders once the answer is known - not about how the catalog finds it.
        """
        if not self._history or self._previous_id is None:
            return None
        return next(s for s in self._history if s.snapshot_id == self._previous_id)

    def snapshot(self, snapshot_id=None):
        if not self._history:
            return None
        if snapshot_id is None:
            return max(self._history, key=lambda s: s.timestamp_ms)
        return next(s for s in self._history if s.snapshot_id == snapshot_id)

    def snapshots(self):
        return list(self._history) if self._with_history else []

    def schema(self, schema_id=None):
        return SimpleNamespace(
            columns=[{"name": "id", "type": "INTEGER", "id": 1}], name="src"
        )


class _FakeCatalog:
    """`coll1.src` has a three-commit history; `coll1.empty` has none."""

    loads = []
    history = _HISTORY
    # One tag, on the MIDDLE snapshot: a tag on the current snapshot would pass
    # a grouping that ignored `snapshot-id` and put every tag on row one.
    tags = [{"name": "month_end", "snapshot-id": 7283774102}]
    # The previous VERSION OF THE DATA - the oldest commit here, which is the
    # only `user_created` one. The two maintenance commits above it changed no
    # rows, so `previous` naming either of them would answer a time-travel read
    # with the data an unqualified read already returns.
    previous_user_snapshot_id = 7283449002

    def __init__(self, workspace=None, **kwargs):
        pass

    def load_dataset(self, identifier, load_history=False):
        _FakeCatalog.loads.append((identifier, load_history))
        history = _FakeCatalog.history if identifier == "coll1.src" else []
        return _FakeDataset(
            history,
            with_history=load_history,
            previous_id=_FakeCatalog.previous_user_snapshot_id,
        )

    def list_tags(self, identifier):
        """The dataset's tags, as the plain dicts the connector groups on.

        SHOW SNAPSHOTS reads these for every relation it lists - a catalog
        object without this method takes the whole statement down, which is
        exactly what a fake missing it did here.
        """
        return _FakeCatalog.tags if identifier == "coll1.src" else []

    def dataset_exists(self, identifier):
        return True

    def get_relation(self, identifier):
        # (None, None) = "not resolved here", which sends binding down the
        # ordinary load_dataset path rather than handing it a prefetched
        # dataset. That is the path SHOW SNAPSHOTS takes against a real
        # catalog too, since the prefetched dataset carries no history.
        return (None, None)


@pytest.fixture
def catalog_workspace():
    _FakeCatalog.loads = []
    _FakeCatalog.history = _HISTORY
    _FakeCatalog.tags = [{"name": "month_end", "snapshot-id": 7283774102}]
    _FakeCatalog.previous_user_snapshot_id = 7283449002
    register_workspace("cat", OpteryxConnector, catalog=_FakeCatalog)
    return _FakeCatalog


def _rows(statement, user="olive"):
    session = opteryx.session(user=user, access_policies=_OWNER_POLICY)
    collected = []
    for morsel in session.execute_to_morsels(statement):
        if morsel is None:
            continue
        pydict = morsel.to_arrow().to_pydict()
        length = len(next(iter(pydict.values()))) if pydict else 0
        for index in range(length):
            collected.append({key: values[index] for key, values in pydict.items()})
    return collected


# --- the shape


def test_show_snapshots_returns_one_row_per_snapshot_newest_first(catalog_workspace):
    rows = _rows("SHOW SNAPSHOTS FOR cat.coll1.src")

    assert [row["snapshot_id"] for row in rows] == [7284091337, 7283774102, 7283449002]


def test_show_snapshots_returns_the_whole_column_set(catalog_workspace):
    rows = _rows("SHOW SNAPSHOTS FOR cat.coll1.src")

    assert list(rows[0].keys()) == [
        "snapshot_id",
        "committed_at",
        "is_current",
        "tags",
        "operation_type",
        "author",
        "user_created",
        "sequence_number",
        "parent_snapshot_id",
        "schema_id",
        "commit_message",
        "added_records",
        "added_data_files",
        "added_files_size_in_bytes",
        "deleted_records",
        "deleted_data_files",
        "deleted_files_size_in_bytes",
        "total_records",
        "total_data_files",
        "total_files_size_in_bytes",
    ]


def test_show_snapshots_marks_only_the_current_snapshot(catalog_workspace):
    rows = _rows("SHOW SNAPSHOTS FOR cat.coll1.src")

    assert [row["is_current"] for row in rows] == [True, False, False]


def test_the_current_snapshot_carries_the_virtual_current_tag(catalog_workspace):
    """`current` is a name the reader can write, so it is shown like any other.

    It is not in the tags subcollection and it pins nothing - it names whichever
    snapshot the head points at today.
    """
    rows = _rows("SHOW SNAPSHOTS FOR cat.coll1.src")

    assert "current" in rows[0]["tags"]
    assert all("current" not in row["tags"] for row in rows[1:])


def test_the_previous_version_of_the_data_carries_the_virtual_previous_tag(
    catalog_workspace,
):
    """`previous` is a name the reader can write, so it is shown like any other.

    It names the previous version of the DATA, which is why it lands on the
    oldest row here and not on the middle one: the two commits above it are
    maintenance commits that changed no rows.
    """
    rows = _rows("SHOW SNAPSHOTS FOR cat.coll1.src")

    assert "previous" in rows[2]["tags"]
    assert all("previous" not in row["tags"] for row in rows[:2])


def test_previous_is_absent_when_there_is_no_earlier_version_of_the_data(
    catalog_workspace,
):
    """Absent, not blank: a dataset at its earliest version has no previous
    version, and no row may claim to be one."""
    catalog_workspace.previous_user_snapshot_id = None
    rows = _rows("SHOW SNAPSHOTS FOR cat.coll1.src")

    assert all("previous" not in row["tags"] for row in rows)
    assert "current" in rows[0]["tags"]


def test_show_snapshots_reports_the_commit_timestamp(catalog_workspace):
    rows = _rows("SHOW SNAPSHOTS FOR cat.coll1.src")

    assert rows[0]["committed_at"] == datetime.datetime.fromtimestamp(
        _T2 / 1000, tz=datetime.timezone.utc
    ).replace(tzinfo=None)


def test_show_snapshots_unpacks_the_summary_counters(catalog_workspace):
    rows = _rows("SHOW SNAPSHOTS FOR cat.coll1.src")
    oldest = rows[-1]

    assert oldest["added_records"] == 1607774
    assert oldest["added_data_files"] == 7
    assert oldest["added_files_size_in_bytes"] == 132656086
    assert oldest["total_records"] == 183611884
    assert oldest["total_data_files"] == 1377
    assert oldest["total_files_size_in_bytes"] == 43220881004


def test_a_counter_the_catalog_never_recorded_is_null_not_zero(catalog_workspace):
    """Zero would claim the commit deleted nothing; null says we do not know.
    The newest snapshot's summary carries only two of the nine keys."""
    rows = _rows("SHOW SNAPSHOTS FOR cat.coll1.src")

    assert rows[0]["added_records"] == 316273
    assert rows[0]["deleted_records"] is None
    assert rows[0]["total_data_files"] is None


def test_show_snapshots_carries_provenance(catalog_workspace):
    rows = _rows("SHOW SNAPSHOTS FOR cat.coll1.src")
    oldest = rows[-1]

    assert oldest["operation_type"] == "overwrite"
    assert oldest["author"] == "justin.joyce@joocer.com"
    assert oldest["user_created"] is True
    assert oldest["sequence_number"] == 4468
    assert oldest["schema_id"] == "sch-0004"
    assert oldest["commit_message"] == "backfill 2024 partitions"


def test_a_tag_is_reported_against_the_snapshot_it_names(catalog_workspace):
    """A tag pins its snapshot's storage indefinitely, and that storage is
    charged - so which snapshot a tag holds is the point of the column, not
    just that the names appear somewhere."""
    rows = _rows("SHOW SNAPSHOTS FOR cat.coll1.src")

    assert [row["tags"] for row in rows] == [["current"], ["month_end"], ["previous"]]


def test_an_untagged_history_reports_empty_lists_not_nulls(catalog_workspace):
    """Empty says 'no tags on this snapshot'; null would say 'unknown', and
    nothing about an untagged snapshot is unknown.

    The head still carries the virtual `current`, which is a name rather than a
    pin - every dataset with a head has it and no dataset can be missing it. So
    does `previous`, on the previous version of the data.
    """
    catalog_workspace.tags = []
    rows = _rows("SHOW SNAPSHOTS FOR cat.coll1.src")

    assert [row["tags"] for row in rows] == [["current"], [], ["previous"]]


def test_several_tags_on_one_snapshot_are_listed_by_name(catalog_workspace):
    catalog_workspace.tags = [
        {"name": "quarter_end", "snapshot-id": 7283774102},
        {"name": "month_end", "snapshot-id": 7283774102},
    ]
    rows = _rows("SHOW SNAPSHOTS FOR cat.coll1.src")

    assert rows[1]["tags"] == ["month_end", "quarter_end"]


def test_the_root_snapshot_has_a_null_parent(catalog_workspace):
    rows = _rows("SHOW SNAPSHOTS FOR cat.coll1.src")

    assert [row["parent_snapshot_id"] for row in rows] == [7283774102, 7283449002, None]


# --- reading the history at all


def test_show_snapshots_loads_the_dataset_with_history(catalog_workspace):
    """Without load_history=True the catalog returns only the current snapshot,
    so a three-commit relation would report a one-row history."""
    _rows("SHOW SNAPSHOTS FOR cat.coll1.src")

    assert ("coll1.src", True) in catalog_workspace.loads


def test_a_relation_with_nothing_committed_has_no_rows(catalog_workspace):
    catalog_workspace.history = []

    assert _rows("SHOW SNAPSHOTS FOR cat.coll1.src") == []


def test_a_connector_with_no_commit_log_says_so(tmp_path):
    """Not the same answer as an empty history: this store keeps no history to
    report, and reporting zero rows would read as 'never written to'."""
    register_workspace("nolog", LocalStoreConnector, store_root=str(tmp_path))
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("CREATE TABLE nolog.dst AS SELECT 1 AS a"))

    with pytest.raises(UnsupportedSyntaxError, match="no snapshot history"):
        list(session.execute_to_morsels("SHOW SNAPSHOTS FOR nolog.dst"))


# --- the grammar


def test_bare_show_snapshots_is_rejected(catalog_workspace):
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="SHOW SNAPSHOTS FOR"):
        list(session.execute_to_morsels("SHOW SNAPSHOTS"))


def test_show_snapshots_from_is_not_the_spelling(catalog_workspace):
    """FOR is the keyword. FROM parses identically through the SHOW catch-all,
    so it has to be refused here or it would reach the planner as a table name."""
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError):
        list(session.execute_to_morsels("SHOW SNAPSHOTS FROM cat.coll1.src"))
