# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`information_schema.forks`: the fork relationships this workspace is one
end of.

TWO SIDES, ONE TABLE. A dataset here that IS a fork contributes a row from its
own `fork_state`; a dataset here that HAS forks contributes one row per
registration in `list_forks`. The far end is named, never read.

A DROPPED DATASET IS NOT A FAILURE. One listed and then gone before the load
has no row. Every other catalog failure raises - a listing that silently loses
rows reads as "no forks", which is a lie about state.
"""

import pytest
from opteryx_catalog.exceptions import DatasetNotFound

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]

_NOW_MS = 1754000000000  # 2025-07-31T21:33:20Z


class _Dataset:
    def __init__(self, state):
        self._state = state

    def fork_state(self):
        return self._state


# `sales.orders` is a fork of another workspace's dataset; `sales.lineitem`
# has been forked twice (one pinned, one not); `sales.parts` is neither.
_FORK_STATES = {
    "sales.orders": {
        "upstream": "prod.sales.orders",
        "base_snapshot": 42,
        "revisions_behind": 3,
        "revisions_ahead": 0,
        "last_sync_ms": _NOW_MS,
    },
    "sales.lineitem": None,
    "sales.parts": None,
}

_REGISTRATIONS = {
    "sales.lineitem": [
        {"fork": "personal.justin.lineitem", "pinned-snapshot": 7, "created-at-ms": _NOW_MS},
        {"fork": "staging.sales.lineitem", "pinned-snapshot": None, "created-at-ms": _NOW_MS},
    ],
}


class _FakeCatalog:
    datasets = ["orders", "lineitem", "parts"]
    load_failures = {}
    list_forks_failures = {}

    def __init__(self, workspace=None, **kwargs):
        pass

    def list_collections(self):
        return ["sales"]

    def list_datasets(self, collection):
        return list(_FakeCatalog.datasets)

    def load_dataset(self, identifier, load_history=False):
        if identifier in _FakeCatalog.load_failures:
            raise _FakeCatalog.load_failures[identifier]
        return _Dataset(_FORK_STATES[identifier])

    def list_forks(self, identifier):
        if identifier in _FakeCatalog.list_forks_failures:
            raise _FakeCatalog.list_forks_failures[identifier]
        return [dict(row) for row in _REGISTRATIONS.get(identifier, [])]

    def dataset_exists(self, identifier):
        return True

    def get_relation(self, identifier):
        return (None, None)


class _CatalogWithoutForks(_FakeCatalog):
    """An installed catalog wheel that predates the forks registry: the fork
    side still answers from the dataset document, the upstream side is empty."""

    list_forks = None


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


@pytest.fixture
def catalog_workspace():
    _FakeCatalog.datasets = ["orders", "lineitem", "parts"]
    _FakeCatalog.load_failures = {}
    _FakeCatalog.list_forks_failures = {}
    register_workspace("cat", OpteryxConnector, catalog=_FakeCatalog)
    return _FakeCatalog


def _read(user="alice"):
    session = opteryx.session(user=user, access_policies=_OWNER_POLICY)
    return _morsels_to_rows(session.execute_to_morsels("SELECT * FROM cat.information_schema.forks"))


def test_a_fork_reports_its_upstream_and_state(catalog_workspace):
    rows = [row for row in _read() if row["fork"] == "cat.sales.orders"]

    assert len(rows) == 1
    row = rows[0]
    assert row["upstream"] == "prod.sales.orders"
    assert row["base_snapshot"] == "42"
    assert row["revisions_behind"] == 3
    assert row["revisions_ahead"] == 0
    assert row["last_sync"] is not None


def test_an_upstream_reports_one_row_per_registered_fork(catalog_workspace):
    rows = {row["fork"]: row for row in _read() if row["upstream"] == "cat.sales.lineitem"}

    assert set(rows) == {"personal.justin.lineitem", "staging.sales.lineitem"}
    assert rows["personal.justin.lineitem"]["base_snapshot"] == "7"
    assert rows["staging.sales.lineitem"]["base_snapshot"] is None
    # The far end is not read, so its revision counts are unknown - not zero.
    for row in rows.values():
        assert row["revisions_behind"] is None
        assert row["revisions_ahead"] is None
        assert row["last_sync"] is not None


def test_a_dataset_that_is_neither_end_has_no_row(catalog_workspace):
    rows = _read()

    assert len(rows) == 3
    assert all("parts" not in (row["fork"] + row["upstream"]) for row in rows)


def test_a_dataset_dropped_mid_listing_has_no_fork_row(catalog_workspace):
    """Listed, then gone before the load: not an error, just not a fork."""
    catalog_workspace.load_failures = {"sales.orders": DatasetNotFound("sales.orders")}

    rows = _read()

    assert [row["fork"] for row in rows if row["fork"] == "cat.sales.orders"] == []
    assert len(rows) == 2


def test_a_catalog_failure_on_load_raises(catalog_workspace):
    """Anything other than 'not found' must not be read as 'not a fork'."""
    catalog_workspace.load_failures = {"sales.orders": RuntimeError("metastore unavailable")}

    with pytest.raises(RuntimeError, match="metastore unavailable"):
        _read()


def test_a_catalog_failure_on_list_forks_raises(catalog_workspace):
    """A registry that cannot be read must not be read as 'no forks'."""
    catalog_workspace.list_forks_failures = {"sales.lineitem": RuntimeError("registry unavailable")}

    with pytest.raises(RuntimeError, match="registry unavailable"):
        _read()


def test_a_catalog_without_forks_registry_reports_only_the_fork_side(catalog_workspace):
    register_workspace("cat", OpteryxConnector, catalog=_CatalogWithoutForks)

    rows = _read()

    assert [row["fork"] for row in rows] == ["cat.sales.orders"]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
