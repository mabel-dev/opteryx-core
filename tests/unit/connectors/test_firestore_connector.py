"""
FirestoreConnector: a Firestore database as a workspace's data source.

No network: `FirestoreConnector.list_page` - the one method that talks to
Firestore - is replaced by a fake collection served in pages, so these tests
cover value decoding, relation naming, the page walk and LIMIT, and a real
query through the engine against the four-column shape.
"""

import datetime
import json
import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], "..", "..", ".."))

import pytest

import opteryx
from opteryx import connectors
from opteryx.connectors import register_workspace
from opteryx.connectors.firestore_connector import FirestoreConnector
from opteryx.connectors.firestore_connector import _decode_value
from opteryx.connectors.firestore_connector import _parse_rfc3339
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import UnsupportedSyntaxError

ROOT = "projects/p/databases/(default)/documents/"


def _doc(collection, doc_id, **fields):
    return {
        "name": f"{ROOT}{collection}/{doc_id}",
        "fields": fields,
        "createTime": "2026-01-02T03:04:05.123456789Z",
        "updateTime": "2026-02-03T04:05:06Z",
    }


def _collection(size):
    return [
        _doc(
            "Orders",
            f"o{i:04d}",
            status={"stringValue": "open" if i % 2 else "closed"},
            amount={"integerValue": str(i)},
        )
        for i in range(size)
    ]


class FakeFirestore:
    """Serves collections page by page and records each page request."""

    def __init__(self, collections):
        self.collections = collections
        self.requests = []

    def list_page(self, collection, page_size, page_token):
        self.requests.append((collection, page_size, page_token))
        documents = self.collections.get(collection, [])
        start = int(page_token or 0)
        page = {"documents": documents[start : start + page_size]}
        if start + page_size < len(documents):
            page["nextPageToken"] = str(start + page_size)
        if not page["documents"]:
            del page["documents"]
        return page


@pytest.fixture
def firestore(monkeypatch):
    fake = FakeFirestore({"Orders": _collection(420), "orders": _collection(3)})
    monkeypatch.setattr(
        FirestoreConnector,
        "list_page",
        lambda self, collection, page_size, page_token: fake.list_page(
            collection, page_size, page_token
        ),
    )
    saved_prefixes = dict(connectors._storage_prefixes)
    saved_cache = dict(connectors._connector_cache)
    saved_versions = dict(connectors._connector_versions)
    try:
        yield fake
    finally:
        connectors._storage_prefixes.clear()
        connectors._storage_prefixes.update(saved_prefixes)
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved_cache)
        connectors._connector_versions.clear()
        connectors._connector_versions.update(saved_versions)


def _rows(sql):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        rows.extend(morsel.to_arrow().to_pylist())
    return rows


# ------------------------------------------------------------------ decoding


def test_scalar_values_decode_to_plain_json():
    assert _decode_value({"stringValue": "a"}, ROOT) == "a"
    assert _decode_value({"integerValue": "9007199254740993"}, ROOT) == 9007199254740993
    assert _decode_value({"doubleValue": 1.5}, ROOT) == 1.5
    assert _decode_value({"doubleValue": "NaN"}, ROOT) == "NaN"
    assert _decode_value({"booleanValue": False}, ROOT) is False
    assert _decode_value({"nullValue": None}, ROOT) is None
    assert _decode_value({"timestampValue": "2026-01-01T00:00:00Z"}, ROOT) == "2026-01-01T00:00:00Z"
    assert _decode_value({"bytesValue": "AAE="}, ROOT) == "AAE="


def test_nested_and_special_values_decode():
    value = {
        "mapValue": {
            "fields": {
                "tags": {"arrayValue": {"values": [{"stringValue": "x"}, {"integerValue": "2"}]}},
                "empty_list": {"arrayValue": {}},
                "empty_map": {"mapValue": {}},
                "owner": {"referenceValue": f"{ROOT}users/abc"},
                "where": {"geoPointValue": {"latitude": 51.5}},
            }
        }
    }
    assert _decode_value(value, ROOT) == {
        "tags": ["x", 2],
        "empty_list": [],
        "empty_map": {},
        "owner": "users/abc",
        "where": {"latitude": 51.5, "longitude": 0.0},
    }


def test_rfc3339_nanoseconds_truncate_to_microseconds():
    assert _parse_rfc3339("2026-01-02T03:04:05.123456789Z") == datetime.datetime(
        2026, 1, 2, 3, 4, 5, 123456, tzinfo=datetime.timezone.utc
    )
    assert _parse_rfc3339("2026-01-02T03:04:05Z").microsecond == 0
    assert _parse_rfc3339(None) is None


# ------------------------------------------------------------------- naming


def test_collection_name_strips_workspace_and_refuses_subcollections():
    gateway = FirestoreConnector(project="p", prefix="fs")
    assert gateway.collection_for("fs.orders.documents") == "orders"
    assert gateway.collection_for("fs.orders") == "orders"
    assert gateway.collection_for("orders.documents") == "orders"
    assert gateway.collection_for("orders") == "orders"
    with pytest.raises(UnsupportedSyntaxError, match="subcollections"):
        gateway.collection_for("fs.users.orders")
    with pytest.raises(UnsupportedSyntaxError):
        gateway.collection_for("fs.a.b.documents")


def test_collection_ids_are_paged_and_filtered_by_the_allowlist(monkeypatch):
    pages = iter([
        {"collectionIds": ["jobs", "billing_accounts"], "nextPageToken": "t"},
        {"collectionIds": ["clients"]},
    ])
    calls = []

    def fake_request(self, url, body=None):
        calls.append((url, body))
        return next(pages)

    monkeypatch.setattr(FirestoreConnector, "_request", fake_request)
    gateway = FirestoreConnector(project="p", collections=["jobs", "clients"])
    assert gateway.list_collection_ids() == ["clients", "jobs"]
    assert calls[0][0].endswith("/projects/p/databases/(default)/documents:listCollectionIds")
    assert calls[1][1] == {"pageSize": 300, "pageToken": "t"}


def test_preserve_sql_case_uses_the_name_as_typed():
    gateway = FirestoreConnector(project="p", prefix="fs", preserve_sql_case=True)
    assert gateway.collection_for("fs.orders", "fs.Orders") == "Orders"


def test_unknown_config_and_bad_credentials_are_refused():
    with pytest.raises(ValueError, match="unknown configuration keys"):
        FirestoreConnector(project="p", host="x")
    with pytest.raises(ValueError, match="service-account key"):
        FirestoreConnector(project="p", credentials={"type": "service_account"})


def test_repr_never_shows_the_credential():
    gateway = FirestoreConnector(project="p", credentials='{"private_key": "SECRET"}')
    assert "SECRET" not in repr(gateway)


def test_collections_allowlist_hides_everything_else(firestore):
    gateway = FirestoreConnector(project="p", prefix="fs", collections=["orders"])
    with pytest.raises(DatasetNotFoundError):
        gateway.table_engine("fs.Orders", telemetry=None).get_dataset_schema()
    assert gateway.locate_object("fs.Orders") == (None, None)
    # Refused without asking Firestore.
    assert firestore.requests == []
    assert gateway.table_engine("fs.orders", telemetry=None).get_dataset_schema() is not None
    with pytest.raises(ValueError, match="list of collection ids"):
        FirestoreConnector(project="p", collections="orders")


# ---------------------------------------------------------------- page walk


def test_full_read_walks_every_page_with_growing_page_sizes(firestore):
    gateway = FirestoreConnector(project="p", prefix="fs", preserve_sql_case=True)
    table = gateway.table_engine("fs.orders", telemetry=None, original_relation="fs.Orders")
    total = sum(morsel.num_rows for morsel in table.read_dataset())
    assert total == 420
    assert [size for _, size, _ in firestore.requests] == [50, 100, 200, 300]


def test_missing_collection_is_dataset_not_found(firestore):
    gateway = FirestoreConnector(project="p", prefix="fs")
    table = gateway.table_engine("fs.nothing_here", telemetry=None)
    with pytest.raises(DatasetNotFoundError):
        table.get_dataset_schema()
    assert gateway.locate_object("fs.nothing_here") == (None, None)


# ------------------------------------------------------------- through SQL


def test_query_reads_documents_as_json(firestore):
    register_workspace("fs", FirestoreConnector, project="p")
    rows = _rows("SELECT id, doc, created_at FROM fs.orders.documents ORDER BY id")
    assert [row["id"] for row in rows] == ["o0000", "o0001", "o0002"]
    assert json.loads(rows[1]["doc"]) == {"status": "open", "amount": 1}
    assert rows[0]["created_at"].replace(tzinfo=None) == datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)


def test_json_operators_filter_in_the_engine(firestore):
    register_workspace("fs", FirestoreConnector, project="p")
    rows = _rows("SELECT id FROM fs.orders WHERE doc->>'status' = 'open'")
    assert [row["id"] for row in rows] == ["o0001"]


def test_limit_stops_the_page_walk(firestore):
    register_workspace("fs", FirestoreConnector, project="p", preserve_sql_case=True)
    rows = _rows("SELECT id FROM fs.Orders LIMIT 10")
    assert len(rows) == 10
    # Binding reads one document to prove the collection exists; the scan
    # then stops after the first (50-document) page.
    assert [size for _, size, _ in firestore.requests] == [1, 50]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
