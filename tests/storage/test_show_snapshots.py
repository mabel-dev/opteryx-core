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
    expired_at_ms=None,
):
    return SimpleNamespace(
        snapshot_id=snapshot_id,
        expired_at_ms=expired_at_ms,
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


# Retired by expiration, tombstoned but not yet purged: OLDER than every live
# commit above, so its position also proves the two lists are merged and sorted
# together rather than appended. `expired_at_ms` is what the tombstone carries.
_EXPIRED = [
    _snapshot(
        7283001155,
        _T0 - 86_400_000,
        parent=None,
        operation="append",
        sequence_number=4467,
        commit_message="hourly load",
        expired_at_ms=_T2,
    )
]


class _FakeDataset:
    """One catalog dataset. `snapshots()` is empty unless the loader was asked
    for history, mirroring the real loader - a connector that forgets
    `load_history=True` must not quietly see a truncated history."""

    def __init__(self, history, with_history, previous_id=None, expired=()):
        self._history = history
        self._with_history = with_history
        self._previous_id = previous_id
        self._expired = list(expired)
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

    def expired_snapshots(self):
        """Tombstones, and only when the loader was asked for them - the real
        loader leaves this empty on every other path, so a connector that
        forgot to ask must see nothing rather than a short history."""
        return list(self._expired)

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
    expired = _EXPIRED
    # The previous VERSION OF THE DATA - the oldest commit here, which is the
    # only `user_created` one. The two maintenance commits above it changed no
    # rows, so `previous` naming either of them would answer a time-travel read
    # with the data an unqualified read already returns.
    previous_user_snapshot_id = 7283449002

    def __init__(self, workspace=None, **kwargs):
        pass

    def load_dataset(self, identifier, load_history=False, include_expired=False):
        _FakeCatalog.loads.append((identifier, load_history))
        history = _FakeCatalog.history if identifier == "coll1.src" else []
        expired = (
            _FakeCatalog.expired
            if include_expired and load_history and identifier == "coll1.src"
            else []
        )
        return _FakeDataset(
            history,
            with_history=load_history,
            previous_id=_FakeCatalog.previous_user_snapshot_id,
            expired=expired,
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
    _FakeCatalog.expired = _EXPIRED
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


# --- SHOW ALL SNAPSHOTS FOR: the live history plus the tombstones


def test_show_all_snapshots_lists_expired_snapshots_too(catalog_workspace):
    """The whole point of the form: an expired snapshot is invisible to
    `SHOW SNAPSHOTS FOR` (the catalog's loader keeps tombstones out of the
    history), and this is the one statement that reports it."""
    live = [row["snapshot_id"] for row in _rows("SHOW SNAPSHOTS FOR cat.coll1.src")]
    every = [row["snapshot_id"] for row in _rows("SHOW ALL SNAPSHOTS FOR cat.coll1.src")]

    assert 7283001155 not in live
    # Merged into one timeline and sorted with the rest, not appended after it.
    assert every == [7284091337, 7283774102, 7283449002, 7283001155]


def test_show_all_snapshots_adds_the_two_expiry_columns(catalog_workspace):
    """Both statements' shapes are stated here, together: the plain form must
    NOT grow a column that would be null on every row it can return."""
    rows = _rows("SHOW ALL SNAPSHOTS FOR cat.coll1.src")
    plain = _rows("SHOW SNAPSHOTS FOR cat.coll1.src")

    assert list(rows[0].keys())[-2:] == ["expired_at", "is_queryable"]
    assert "expired_at" not in plain[0]
    assert "is_queryable" not in plain[0]


def test_expired_at_is_set_only_on_the_tombstone(catalog_workspace):
    rows = _rows("SHOW ALL SNAPSHOTS FOR cat.coll1.src")

    assert [row["expired_at"] for row in rows[:3]] == [None, None, None]
    assert rows[3]["expired_at"] == datetime.datetime.fromtimestamp(
        _T2 / 1000, tz=datetime.timezone.utc
    ).replace(tzinfo=None)


def test_is_queryable_says_which_rows_can_still_be_read(catalog_workspace):
    """A timestamp alone leaves the reader to know that `VERSION AS OF` refuses
    an expired id. The column says it."""
    rows = _rows("SHOW ALL SNAPSHOTS FOR cat.coll1.src")

    assert [row["is_queryable"] for row in rows] == [True, True, True, False]


def test_an_expired_snapshot_is_never_current(catalog_workspace):
    rows = _rows("SHOW ALL SNAPSHOTS FOR cat.coll1.src")

    assert rows[3]["is_current"] is False
    assert [row["is_current"] for row in rows] == [True, False, False, False]


def test_show_all_snapshots_asks_the_catalog_for_tombstones(catalog_workspace):
    """The plain form must not: tombstones are a second read the statement that
    cannot show them has no use for."""
    seen = {}

    original = _FakeCatalog.load_dataset

    def _record(self, identifier, load_history=False, include_expired=False):
        seen[identifier] = include_expired
        return original(self, identifier, load_history, include_expired)

    _FakeCatalog.load_dataset = _record
    try:
        _rows("SHOW SNAPSHOTS FOR cat.coll1.src")
        assert seen["coll1.src"] is False
        _rows("SHOW ALL SNAPSHOTS FOR cat.coll1.src")
        assert seen["coll1.src"] is True
    finally:
        _FakeCatalog.load_dataset = original


def test_a_relation_with_nothing_committed_has_no_rows_in_the_all_form(catalog_workspace):
    catalog_workspace.history = []
    catalog_workspace.expired = []

    assert _rows("SHOW ALL SNAPSHOTS FOR cat.coll1.src") == []


# The owner-tier gate on this form is pinned in
# tests/storage/test_permissions_capability.py, beside the MANIFEST gate it
# borrows and with the scripted capability those tests install: an
# access_policies session here answers through the intrinsic permissive
# capability, which is not a gate at all.


# --- the grammar


def test_bare_show_all_snapshots_is_rejected(catalog_workspace):
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="SHOW ALL SNAPSHOTS FOR"):
        list(session.execute_to_morsels("SHOW ALL SNAPSHOTS"))


def test_show_all_is_only_for_snapshots(catalog_workspace):
    """LINEAGE is per-commit receipts and SOURCES is a field on the dataset;
    neither has an expired half for ALL to mean anything about."""
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="only supported for snapshots"):
        list(session.execute_to_morsels("SHOW ALL LINEAGE FOR cat.coll1.src"))


def test_a_catalog_that_cannot_read_tombstones_says_so(catalog_workspace):
    """Mid-upgrade: the engine is new enough to have the statement and the
    catalog is not. It must refuse rather than answer with the live history,
    which is a different answer wearing this statement's name."""

    class _OldCatalog(_FakeCatalog):
        def load_dataset(self, identifier, load_history=False):
            return _FakeCatalog.load_dataset(self, identifier, load_history)

    register_workspace("old", OpteryxConnector, catalog=_OldCatalog)
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    # The live history still answers.
    list(session.execute_to_morsels("SHOW SNAPSHOTS FOR old.coll1.src"))

    with pytest.raises(UnsupportedSyntaxError, match="cannot"):
        list(session.execute_to_morsels("SHOW ALL SNAPSHOTS FOR old.coll1.src"))
