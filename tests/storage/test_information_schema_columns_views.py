# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`information_schema.columns` lists a view's columns.

A view was listed in `information_schema.tables` as a VIEW but had no rows in
`columns`. CREATE/ALTER VIEW bind the definition and store its typed output
schema with the view's statement; `columns` reads that schema - it never binds.
A view with no stored schema raises rather than being silently left out.
"""

from types import SimpleNamespace

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx.exceptions import DatasetReadError

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]

_TABLE_COLUMNS = [
    {"name": "id", "type": "INTEGER", "id": 1},
    {"name": "name", "type": "VARCHAR", "id": 2},
]
_VIEW_COLUMNS = [
    {"name": "name", "type": "VARCHAR", "id": 1},
    {"name": "total", "type": "FLOAT64", "id": 2},
    {"name": "seen_at", "type": "TIMESTAMP", "id": 3},
]


class _Dataset:
    def snapshot(self):
        return None

    def schema(self, schema_id=None):
        return SimpleNamespace(columns=list(_TABLE_COLUMNS), name="ops.base")


class _Catalog:
    view_schema = SimpleNamespace(columns=list(_VIEW_COLUMNS), name="ops.summary")

    def __init__(self, workspace=None, **kwargs):
        pass

    def list_collections(self):
        return ["ops"]

    def list_datasets(self, collection):
        return ["base"]

    def list_views(self, collection):
        return ["summary"]

    def load_dataset(self, identifier):
        return _Dataset()

    def load_view(self, identifier):
        return SimpleNamespace(name="summary", metadata=SimpleNamespace(schema=type(self).view_schema))


class _LegacyCatalog(_Catalog):
    view_schema = None


def _run(statement):
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    rows = []
    for morsel in session.execute_to_morsels(statement):
        rows.extend(tuple(row) for row in morsel)
    return rows


_QUERY = (
    "SELECT table_name, column_name, ordinal_position, data_type "
    "FROM {ws}.information_schema.columns ORDER BY table_name, ordinal_position"
)


def test_columns_lists_view_columns_with_types():
    register_workspace("isviews", OpteryxConnector, catalog=_Catalog)

    rows = _run(_QUERY.format(ws="isviews"))

    assert rows == [
        ("base", "id", 1, "INT64"),
        ("base", "name", 2, "VARCHAR"),
        ("summary", "name", 1, "VARCHAR"),
        ("summary", "total", 2, "FLOAT64"),
        ("summary", "seen_at", 3, "TIMESTAMP[us]"),
    ]


def test_table_name_predicate_selects_only_the_view():
    register_workspace("isviews2", OpteryxConnector, catalog=_Catalog)

    rows = _run(
        "SELECT column_name FROM isviews2.information_schema.columns "
        "WHERE table_name = 'summary' ORDER BY ordinal_position"
    )

    assert rows == [("name",), ("total",), ("seen_at",)]


def test_view_without_stored_schema_raises():
    register_workspace("isviews3", OpteryxConnector, catalog=_LegacyCatalog)

    with pytest.raises(DatasetReadError, match="no stored schema"):
        _run(_QUERY.format(ws="isviews3"))
