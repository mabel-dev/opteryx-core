# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Firestore connector — a Firestore database as a workspace's data source.

A CONNECTOR, not a catalog, for the same reason `PostgresConnector` is one:
Firestore holds the data and serves the read, there are no files for the
engine to read itself, so the resolution chain binds a workspace straight to
this gateway (`Resolution(FirestoreConnector, config={...})`) with no
`catalog=` factory.

Deliberately minimal - this is expected to be rarely used:

  * Naming: the workspace is the DATABASE, each top-level Firestore
    collection is an Opteryx collection, and its documents are the dataset
    `documents` inside it - `jobs.clients.documents` reads `/clients/*`. The
    catalog lists datasets as `<workspace>.<collection>.<dataset>`, so this is
    the shape a bound workspace's listing can carry. `<workspace>.<collection>`
    is accepted as shorthand for the same relation. Other dataset names are
    reserved for subcollections (`jobs.jobs.runs` -> `/jobs/*/runs/*`), which
    are not read yet and are refused.
  * The whole document is ONE column. Firestore has no schema, and a schema
    guessed from a sample would change under the query that relied on it, so
    every relation has the same four columns and fields are reached with the
    JSON operators (`doc->>'status'`, `doc->'address'->>'city'`):

        id          VARCHAR    the document id (last segment of its name)
        doc         VARCHAR    the document's fields as JSON text
        created_at  TIMESTAMP  Firestore's createTime
        updated_at  TIMESTAMP  Firestore's updateTime

    `doc` is VARCHAR rather than VARIANT only because Draken has no Python
    constructor for a VARIANT vector; the JSON operators accept either.
  * No predicate pushdown. Every read is a full `listDocuments` scan, filtered
    by the engine. LIMIT is honoured by stopping the page walk (see
    `_PAGE_SIZES`) - Firestore bills per document returned, so a LIMIT that
    still fetched the whole collection would cost the customer real money.
  * Executed in Python over Firestore's REST API (no client library, no
    native Source): the generic "Reader" node drives `read_dataset`, the same
    route `information_schema` takes.

Value mapping - Firestore's typed REST values become plain JSON:

    integerValue    number (Firestore sends int64 as a string; parsed back)
    doubleValue     number; NaN / Infinity / -Infinity stay the STRINGS
                    Firestore sends, since JSON has no spelling for them
    timestampValue  RFC 3339 string, as sent
    bytesValue      base64 string, as sent
    referenceValue  document path relative to the database (`users/abc`)
    geoPointValue   {"latitude": .., "longitude": ..}
    arrayValue      array;  mapValue  object (a vector is a map, as stored)

Scope: a binding points at a whole database, which often holds far more than
the workspace should expose. `collections`, when set, is the complete list of
collections the workspace can read; any other name is not found, without a
call to Firestore.

Auth: `credentials` absent means the engine's own identity (ADC) - the
binding's "ambient" mode, where the customer grants the worker's service
account read access to their database. A "stored" binding injects a
service-account key (the JSON text) as `credentials`.
"""

import datetime
import json
import math
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel

from opteryx.connectors import TableType
from opteryx.connectors.base.base_connector import BaseConnector, BaseTable
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import QueryTelemetry
from opteryx.types import logical_type as _lt
from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity

FIRESTORE_ENDPOINT = "https://firestore.googleapis.com/v1"
_DATASTORE_SCOPE = "https://www.googleapis.com/auth/datastore"

# Page sizes for the listDocuments walk, in order; the last repeats. Small
# first pages keep `LIMIT n` cheap - the reader stops pulling pages as soon as
# the LIMIT is met, so `LIMIT 10` costs at most 50 billed reads, not 300 - and
# a full scan still reaches Firestore's 300-per-page ceiling quickly.
_PAGE_SIZES = (50, 100, 200, 300)

# Transient statuses retried with backoff before the read is failed.
_RETRY_STATUSES = (429, 500, 502, 503, 504)
_MAX_ATTEMPTS = 4

# The dataset that holds a Firestore collection's own documents.
DOCUMENTS_DATASET = "documents"

_COLUMNS = (
    ("id", _lt.VARCHAR, DrakenType.VARCHAR),
    ("doc", _lt.VARCHAR, DrakenType.VARCHAR),
    ("created_at", _lt.TIMESTAMP(), DrakenType.TIMESTAMP64),
    ("updated_at", _lt.TIMESTAMP(), DrakenType.TIMESTAMP64),
)


def _parse_rfc3339(text: Optional[str]) -> Optional[datetime.datetime]:
    """Firestore's `2024-01-02T03:04:05.123456789Z` as a UTC datetime.

    Python keeps microseconds, so nanoseconds beyond the sixth digit are
    truncated - the same precision the TIMESTAMP column holds."""
    if not text:
        return None
    body = text.rstrip("Z")
    if "." in body:
        head, fraction = body.split(".", 1)
        body = f"{head}.{fraction[:6].ljust(6, '0')}"
    else:
        body = f"{body}.000000"
    return datetime.datetime.strptime(body, "%Y-%m-%dT%H:%M:%S.%f").replace(
        tzinfo=datetime.timezone.utc
    )


def _decode_value(value: Dict[str, Any], documents_root: str) -> Any:
    """One Firestore REST `Value` as a plain JSON-able Python value."""
    if "stringValue" in value:
        return value["stringValue"]
    if "integerValue" in value:
        return int(value["integerValue"])
    if "doubleValue" in value:
        number = value["doubleValue"]
        # REST sends non-finite doubles as the strings "NaN"/"Infinity"; keep
        # them as those strings, since json.dumps would otherwise emit bare
        # NaN, which is not JSON.
        if isinstance(number, str):
            return number
        return float(number) if math.isfinite(number) else str(number)
    if "booleanValue" in value:
        return value["booleanValue"]
    if "nullValue" in value:
        return None
    if "timestampValue" in value:
        return value["timestampValue"]
    if "mapValue" in value:
        fields = value["mapValue"].get("fields", {})
        return {key: _decode_value(item, documents_root) for key, item in fields.items()}
    if "arrayValue" in value:
        items = value["arrayValue"].get("values", [])
        return [_decode_value(item, documents_root) for item in items]
    if "referenceValue" in value:
        reference = value["referenceValue"]
        if reference.startswith(documents_root):
            return reference[len(documents_root) :]
        return reference
    if "geoPointValue" in value:
        point = value["geoPointValue"]
        # Firestore omits a coordinate that is exactly 0.
        return {"latitude": point.get("latitude", 0.0), "longitude": point.get("longitude", 0.0)}
    if "bytesValue" in value:
        return value["bytesValue"]
    # A value type this module does not know yet: keep it rather than drop it.
    return value


class FirestoreConnector(BaseConnector):
    """Long-lived gateway for one Firestore database.

    Cached by the resolution chain per workspace; creates a transient
    `FirestoreTable` per query via `table_engine()`. The credential (and its
    cached access token) lives here, so a token is refreshed once per expiry
    for the workspace, not once per query.
    """

    __mode__ = "Document"
    __type__ = "FIRESTORE"

    # table_engine() needs the relation name as typed: Firestore collection
    # names are case-sensitive and commonly camelCase.
    requires_original_case = True

    def __init__(
        self,
        *,
        project: str,
        database: str = "(default)",
        credentials: Optional[str] = None,
        collections: Optional[List[str]] = None,
        timeout_s: int = 30,
        preserve_sql_case: bool = False,
        telemetry: Optional[QueryTelemetry] = None,
        prefix: Optional[str] = None,
        **kwargs,
    ) -> None:
        if kwargs:
            raise ValueError(
                f"FirestoreConnector: unknown configuration keys {sorted(kwargs)}; "
                "expected project, database, credentials, collections, timeout_s, preserve_sql_case"
            )
        for label, value in (("project", project), ("database", database)):
            if not isinstance(value, str) or not value:
                raise ValueError(f"FirestoreConnector: '{label}' must be a non-empty string")
        if credentials is not None and not isinstance(credentials, str):
            raise ValueError(
                "FirestoreConnector: 'credentials' must be a service-account key as JSON text"
            )
        if collections is not None and (
            not isinstance(collections, (list, tuple))
            or not all(isinstance(name, str) and name for name in collections)
        ):
            raise ValueError("FirestoreConnector: 'collections' must be a list of collection ids")
        self.project = project
        # None exposes every top-level collection; a list exposes only those.
        self.collections = None if collections is None else frozenset(collections)
        self.database = database
        self.timeout_s = int(timeout_s)
        self.preserve_sql_case = bool(preserve_sql_case)
        self.telemetry = telemetry
        # connector_factory overwrites this with the resolved workspace/prefix
        # after construction; a direct construction keeps what it was given.
        self._matched_prefix = prefix
        self._service_account_json = credentials
        self._credentials = None
        self._token_lock = threading.Lock()

    def __repr__(self) -> str:  # never the credential
        return f"FirestoreConnector(project={self.project!r}, database={self.database!r})"

    @property
    def documents_root(self) -> str:
        """The resource-name prefix every document in this database carries."""
        return f"projects/{self.project}/databases/{self.database}/documents/"

    # ------------------------------------------------------------------ auth

    def _bearer(self) -> str:
        """A valid access token, refreshing the credential when it has expired.

        Imported lazily: opteryx-core carries no hard dependency on google-auth,
        and only a process that actually serves a Firestore binding needs it."""
        with self._token_lock:
            if self._credentials is None:
                if self._service_account_json:
                    from google.oauth2 import service_account

                    try:
                        info = json.loads(self._service_account_json)
                    except ValueError as err:
                        # Never echo the text: it is the credential.
                        raise DatasetReadError(
                            "The Firestore binding's stored credential is not a "
                            "service-account key (JSON)"
                        ) from err
                    self._credentials = service_account.Credentials.from_service_account_info(
                        info, scopes=[_DATASTORE_SCOPE]
                    )
                else:
                    import google.auth

                    self._credentials, _ = google.auth.default(scopes=[_DATASTORE_SCOPE])
            if not self._credentials.valid:
                from google.auth.transport.requests import Request

                self._credentials.refresh(Request())
            return f"Bearer {self._credentials.token}"

    # ------------------------------------------------------------------ http

    def _request(self, url: str, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """GET `url` (or POST `body` to it) and return the decoded JSON body.

        A collection that does not exist is not an error here - it lists no
        documents. A 404 means the DATABASE (or project) is missing, and is
        raised; transient statuses are retried with backoff."""
        data = None if body is None else json.dumps(body).encode()
        delay = 0.5
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            headers = {"Authorization": self._bearer(), "Accept": "application/json"}
            if data is not None:
                headers["Content-Type"] = "application/json"
            request = urllib.request.Request(url, data=data, headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=self.timeout_s) as response:
                    return json.loads(response.read())
            except urllib.error.HTTPError as err:
                status = err.code
                detail = _error_message(err)
                if status in _RETRY_STATUSES and attempt < _MAX_ATTEMPTS:
                    time.sleep(delay)
                    delay *= 2
                    continue
                if status == 404:
                    raise DatasetReadError(
                        f"Firestore database '{self.database}' in project '{self.project}' "
                        f"was not found ({detail})"
                    ) from err
                if status in (401, 403):
                    raise DatasetReadError(
                        f"Firestore refused the read of project '{self.project}' database "
                        f"'{self.database}' ({detail}). The workspace's identity needs "
                        "read access (roles/datastore.viewer) on that project."
                    ) from err
                raise DatasetReadError(f"Firestore read failed with HTTP {status} ({detail})") from err
            except urllib.error.URLError as err:
                if attempt < _MAX_ATTEMPTS:
                    time.sleep(delay)
                    delay *= 2
                    continue
                raise DatasetReadError(f"Firestore could not be reached ({err.reason})") from err
        raise DatasetReadError("Firestore read failed")  # pragma: no cover - loop always returns/raises

    def list_page(self, collection: str, page_size: int, page_token: Optional[str]) -> Dict[str, Any]:
        """One page of `listDocuments` for a top-level collection."""
        params: Dict[str, Any] = {"pageSize": page_size}
        if page_token:
            params["pageToken"] = page_token
        query = urllib.parse.urlencode(params)
        return self._request(
            f"{FIRESTORE_ENDPOINT}/{self.documents_root}{urllib.parse.quote(collection)}?{query}"
        )

    def list_collection_ids(self) -> List[str]:
        """Every top-level collection id in the database, one page at a time.

        `listCollectionIds` names collections without reading any document in
        them. The binding's `collections` allowlist is applied here, so a
        listing never names a collection the workspace cannot read."""
        root = self.documents_root.rstrip("/")
        ids: List[str] = []
        token = None
        while True:
            body: Dict[str, Any] = {"pageSize": 300}
            if token:
                body["pageToken"] = token
            page = self._request(f"{FIRESTORE_ENDPOINT}/{root}:listCollectionIds", body)
            ids.extend(page.get("collectionIds") or [])
            token = page.get("nextPageToken")
            if not token:
                break
        return sorted(name for name in ids if self.exposes(name))

    # ------------------------------------------------------------- relations

    def collection_for(self, name: str, original: Optional[str] = None) -> str:
        """`<workspace>.<collection>.documents` -> the Firestore collection id.

        `<workspace>.<collection>` (and either without the workspace) is the
        same relation. Any other dataset name would be a subcollection, which
        is refused rather than guessed at.

        Lower-cased by the binder unless the binding preserves case, in which
        case the name as typed is used - Firestore collection ids are
        case-sensitive."""
        source = original if (self.preserve_sql_case and original) else name
        parts = source.split(".")
        prefix = self._matched_prefix
        if prefix and len(parts) > 1 and parts[0].lower() == prefix.lower():
            parts = parts[1:]
        if len(parts) == 2 and parts[1].lower() == DOCUMENTS_DATASET:
            parts = parts[:1]
        if len(parts) != 1 or not parts[0]:
            raise UnsupportedSyntaxError(
                f"'{name}' is not a Firestore relation; expected "
                f"<workspace>.<collection>.{DOCUMENTS_DATASET} (subcollections are not "
                "supported yet)"
            )
        return parts[0]

    def exposes(self, collection: str) -> bool:
        """Whether the binding lets this workspace read `collection` at all."""
        return self.collections is None or collection in self.collections

    def locate_object(self, name: str) -> Tuple[Optional[TableType], Any]:
        """A collection exists in Firestore exactly when it holds a document."""
        collection = self.collection_for(name)
        if not self.exposes(collection):
            return None, None
        page = self.list_page(collection, 1, None)
        if not page.get("documents"):
            return None, None
        return TableType.Table, {"collection": collection}

    def table_engine(self, name: str, **kwargs):
        return FirestoreTable(dataset=name, gateway=self, **kwargs)


def _error_message(err: urllib.error.HTTPError) -> str:
    """Firestore's own error message from a failed response, if it sent one."""
    try:
        body = json.loads(err.read())
        return body.get("error", {}).get("message") or err.reason
    except Exception:
        return str(err.reason)


class FirestoreTable(BaseTable):
    """Transient reader for one Firestore collection."""

    __mode__ = "Document"
    # Routes through the generic Python "Reader" physical node (see
    # physical_planner._build_scan_node), like information_schema.
    interal_only = True
    # The Reader stops iterating once the LIMIT is met, and read_dataset only
    # fetches a page when the Reader asks for more - so a pushed LIMIT stops
    # the billed page walk. Nothing is ever filtered here, so there is no
    # pushed predicate for a LIMIT to be counted against.
    supports_limit_pushdown = True

    def __init__(
        self,
        *,
        dataset: str,
        gateway: FirestoreConnector,
        telemetry: QueryTelemetry,
        original_relation: Optional[str] = None,
        **kwargs,
    ) -> None:
        BaseTable.__init__(self, dataset=dataset, telemetry=telemetry)
        self.gateway = gateway
        self.collection = gateway.collection_for(dataset, original_relation)

    def get_dataset_schema(self) -> RelationSchema:
        if not self.gateway.exposes(self.collection):
            raise DatasetNotFoundError(connector=self.gateway.__type__, dataset=self.dataset)
        # One billed read to tell an existing collection from a typo: Firestore
        # has no collection objects, so "not found" means "no documents".
        if not self.gateway.list_page(self.collection, 1, None).get("documents"):
            raise DatasetNotFoundError(connector=self.gateway.__type__, dataset=self.dataset)
        self.schema = RelationSchema(
            name=self.dataset,
            columns=[
                SchemaColumn(
                    name=name,
                    column_type=column_type,
                    identity=mint_column_identity(self.dataset, name),
                )
                for name, column_type, _ in _COLUMNS
            ],
        )
        return self.schema

    def _documents(self) -> Iterator[List[Dict[str, Any]]]:
        """Pages of raw documents, fetched only as they are consumed."""
        token = None
        page_number = 0
        while True:
            page_size = _PAGE_SIZES[min(page_number, len(_PAGE_SIZES) - 1)]
            page = self.gateway.list_page(self.collection, page_size, token)
            page_number += 1
            documents = page.get("documents") or []
            if documents:
                yield documents
            token = page.get("nextPageToken")
            if not token:
                return

    def read_dataset(self, **kwargs) -> Iterable[Morsel]:
        root = self.gateway.documents_root
        for documents in self._documents():
            ids, docs, created, updated = [], [], [], []
            for document in documents:
                ids.append(document["name"].rsplit("/", 1)[-1])
                fields = document.get("fields", {})
                decoded = {key: _decode_value(value, root) for key, value in fields.items()}
                docs.append(json.dumps(decoded, ensure_ascii=False, separators=(",", ":")))
                created.append(_parse_rfc3339(document.get("createTime")))
                updated.append(_parse_rfc3339(document.get("updateTime")))
            vectors = [
                vector_from_sequence(values, dtype=draken_type)
                for values, (_, _, draken_type) in zip((ids, docs, created, updated), _COLUMNS)
            ]
            yield Morsel.from_vectors([name for name, _, _ in _COLUMNS], vectors)
