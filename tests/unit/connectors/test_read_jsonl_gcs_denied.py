"""READ_JSONL must never sign a request or use this process's own ambient/platform
GCS service-account credentials for a user-supplied `gs://` path.

READ_JSONL is a bare dataset function (opteryx.planner.binder.dataset.
visit_function_dataset): unlike catalog-backed table scans (visit_scan), which are
gated by `can_perform_action` before any connector is even opened, READ_JSONL takes a
path (or, since Stage 4, a glob pattern) straight out of the SQL text with no
per-query authorization check at all.

OpteryxGcsFileSystem authenticates with the PROCESS's own ambient service-account
credentials (get_storage_credentials -> google.auth.default()), not anything scoped
to the requesting user. If READ_JSONL used that filesystem, any SQL text could read
or list ANY bucket that service account can reach, regardless of whether the
requesting user is authorized to see it -- an IDOR (the "object" being an
attacker-controlled bucket/path, with no ownership/authorization check tying it to
the caller).

This is NOT a blanket "refuse all gs://" rule. Instead, READ_JSONL's gs:// support
(opteryx.connectors.io_systems.anonymous_gcs_filesystem) always does a plain,
unauthenticated HTTPS GET of `https://storage.googleapis.com/<bucket>/<object>` --
never a signed URL, never the ambient bearer token. GCS's own object-level IAM
decides the outcome: a public object is read; a private one 403s from GCS itself,
not from any allow/deny logic in Opteryx. A glob LISTS the prefix, and that is tried
ANONYMOUSLY too (no Authorization header, ever); a bucket that does not allow it fails the
query saying so, and is never retried with a credential
(docs/ANONYMOUS_REMOTE_READS_DESIGN.md).

These tests pin that: the platform credential path is never touched (proven both by
the fake HTTP client's `.get(url)` signature, which cannot carry an Authorization
header, and by monkeypatching `get_storage_credentials` to blow up if it's ever
called), a public object is readable, a private one fails loud, a glob is listed
anonymously and a denied listing fails loud, local paths/globs are unaffected, and other GCS read paths
(OpteryxGcsFileSystem itself) are untouched by any of this.
"""

import json
import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import InvalidFunctionParameterError
from opteryx.exceptions import NotSupportedError


def _run(sql):
    session = opteryx.session()
    return list(session.execute_to_morsels(sql))


def _credentials_must_not_be_touched(*args, **kwargs):
    raise AssertionError(
        "get_storage_credentials() was called -- READ_JSONL must never touch platform "
        "GCS credentials for a user-supplied gs:// path."
    )


class _FakeAnonymousHttpClient:
    """Stands in for opteryx.compiled.http_client.HttpClient inside OpteryxHttpFileSystem.

    Its `.get(url)` signature takes no headers/Authorization argument at all -- the same
    shape the real OpteryxHttpFileSystem.open_input_stream calls it with -- so using this
    fake proves no credential could have been attached, not merely that this particular
    call site omitted one. Objects not in `objects` raise, mirroring a real GCS 403 on a
    private object fetched anonymously.
    """

    def __init__(self, *args, **kwargs):
        pass

    objects = {}
    listings = {}          # listing-URL prefix -> body
    denied_listings = ()   # listing-URL prefixes answered 403
    requested = []

    def get(self, url):
        type(self).requested.append(url)
        for prefix in type(self).denied_listings:
            if url.startswith(prefix):
                from opteryx.compiled.http_client import HttpStatusError

                raise HttpStatusError("HTTP 403", 403)
        for prefix, body in type(self).listings.items():
            if url.startswith(prefix):
                return body
        if url in type(self).objects:
            return type(self).objects[url]
        raise RuntimeError("HTTP 403: access denied")


@pytest.fixture(autouse=True)
def _reset_fake_http_client():
    _FakeAnonymousHttpClient.objects = {}
    _FakeAnonymousHttpClient.listings = {}
    _FakeAnonymousHttpClient.denied_listings = ()
    _FakeAnonymousHttpClient.requested = []
    yield


@pytest.fixture
def anonymous_http(monkeypatch):
    monkeypatch.setattr(
        "opteryx.compiled.http_client.HttpClient", _FakeAnonymousHttpClient
    )
    monkeypatch.setattr(
        "opteryx.connectors.io_systems.gcs_filesystem.get_storage_credentials",
        _credentials_must_not_be_touched,
    )
    return _FakeAnonymousHttpClient


class _PlanCaptured(Exception):
    """Stops a query once its native JSONL scan plan is built, before execution."""


def test_read_jsonl_public_gcs_object_is_read_anonymously(anonymous_http, monkeypatch):
    # _AnonymousGcsFileSystem quotes the object path the same way GcsFile does
    # (urllib.parse.quote(..., safe="")) -- '/' inside the object name becomes '%2F'.
    url = "https://storage.googleapis.com/opteryx/rugo_examples%2Fspace_missions.jsonl"
    anonymous_http.objects[url] = b'{"a": 1}\n{"a": 2}\n'

    # Bind time reads the object through the (faked) anonymous Python client. The
    # execution-time read is the native Source's own GET (NativeJsonlScanSource), so
    # it is pinned at the plan instead: the URL the Source will fetch must be the
    # public object URL, which the native contract fetches with no auth header
    # (AnonymousGcsFileSystem.native_auth_header is None).
    import opteryx.operators._operators as operators

    captured = {}

    def capture(parse_context, files, urls, *args):
        captured["files"] = files
        captured["urls"] = urls
        raise _PlanCaptured()

    monkeypatch.setattr(operators, "JsonlScanPlan", capture)
    with pytest.raises(_PlanCaptured):
        _run("SELECT * FROM READ_JSONL('gs://opteryx/rugo_examples/space_missions.jsonl')")

    assert captured["files"] == ["gs://opteryx/rugo_examples/space_missions.jsonl"]
    assert captured["urls"] == [url]
    assert anonymous_http.requested == [url]

    from opteryx.connectors.io_systems.anonymous_gcs_filesystem import anonymous_gcs_filesystem

    assert anonymous_gcs_filesystem().native_auth_header() is None


def test_read_jsonl_private_gcs_object_fails_loud_not_silent(anonymous_http):
    # Nothing is registered for this URL -- the fake 403s, exactly like a real private
    # object fetched with no Authorization header. Opteryx makes no allow/deny call of
    # its own; GCS's response is what decides this.
    with pytest.raises(DatasetReadError):
        _run(
            "SELECT * FROM READ_JSONL("
            "'gs://opteryx_data/mitre/attack/attack_pattern/data/data-1767139228503.jsonl')"
        )


_LIST = "https://storage.googleapis.com/storage/v1/b/opteryx/o?"


def test_read_jsonl_gcs_glob_is_listed_anonymously(anonymous_http, monkeypatch):
    # The glob LISTS the prefix through the faked client, whose `.get(url)` cannot carry a
    # header; the folder placeholder object is not a file; the matches are read.
    entries = [
        {"name": "rugo_examples/", "size": "0", "updated": "2026-10-25T21:02:08.697Z"},
        {"name": "rugo_examples/a.jsonl", "size": "9", "updated": "2026-10-25T21:02:08.697Z"},
        {"name": "rugo_examples/b.jsonl", "size": "9", "updated": "2026-10-25T21:02:08.697Z"},
        {"name": "rugo_examples/skip.txt", "size": "9", "updated": "2026-10-25T21:02:08.697Z"},
    ]
    anonymous_http.listings[_LIST] = json.dumps({"items": entries}).encode()
    anonymous_http.objects["https://storage.googleapis.com/opteryx/rugo_examples%2Fa.jsonl"] = b'{"a": 1}\n'
    anonymous_http.objects["https://storage.googleapis.com/opteryx/rugo_examples%2Fb.jsonl"] = b'{"a": 2}\n'

    import opteryx.operators._operators as operators

    captured = {}

    def capture(parse_context, files, urls, *args):
        captured["files"] = files
        raise _PlanCaptured()

    monkeypatch.setattr(operators, "JsonlScanPlan", capture)
    with pytest.raises(_PlanCaptured):
        _run("SELECT * FROM READ_JSONL('gs://opteryx/rugo_examples/*.jsonl')")

    assert captured["files"] == ["gs://opteryx/rugo_examples/a.jsonl", "gs://opteryx/rugo_examples/b.jsonl"]
    assert anonymous_http.requested[0].startswith(_LIST)


def test_read_jsonl_gcs_denied_listing_fails_loud_with_one_request(anonymous_http):
    # A bucket that does not allow anonymous listing: the query fails SAYING SO, after a single
    # request. There is no second, authenticated attempt (get_storage_credentials is rigged to blow
    # up by the fixture, and the fake client could not carry a header anyway).
    anonymous_http.denied_listings = (_LIST,)
    with pytest.raises(DatasetReadError, match="does not allow anonymous listing"):
        _run("SELECT * FROM READ_JSONL('gs://opteryx/rugo_examples/*.jsonl')")
    assert len(anonymous_http.requested) == 1


def test_read_jsonl_gcs_scheme_is_rejected_not_treated_as_gs_alias(anonymous_http):
    # "gcs://" is deliberately NOT accepted as an alias for "gs://": the native
    # Parquet scan gate (a separate READ_PARQUET concern) only recognizes the
    # literal "gs://" prefix as remote, so admitting "gcs://" here would be a trap
    # for that sibling feature rather than a convenience. Rejected outright, before
    # any network call.
    with pytest.raises(InvalidFunctionParameterError):
        _run("SELECT * FROM READ_JSONL('gcs://opteryx/rugo_examples/space_missions.jsonl')")
    assert anonymous_http.requested == []


def test_read_jsonl_local_paths_are_unaffected(tmp_path):
    # The gs:// handling must be scoped to remote protocols only -- a plain local path
    # (Stages 1-3's existing behavior, and Stage 4's non-glob case) must still work.
    jsonl_file = tmp_path / "ok.jsonl"
    jsonl_file.write_text('{"a": 1}\n{"a": 2}\n')

    morsels = _run(f"SELECT * FROM READ_JSONL('{jsonl_file}')")
    assert sum(m.num_rows for m in morsels) == 2


def test_read_jsonl_local_glob_is_unaffected(tmp_path):
    # Stage 4's glob support for local paths must not be caught by the gs:// handling.
    (tmp_path / "a.jsonl").write_text('{"a": 1}\n')
    (tmp_path / "b.jsonl").write_text('{"a": 2}\n')

    morsels = _run(f"SELECT * FROM READ_JSONL('{tmp_path}/*.jsonl')")
    assert sum(m.num_rows for m in morsels) == 2


class _FakeGcsListHttpClient:
    """Serves a canned GCS JSON-API list response; no credentials or network needed."""

    def __init__(self, items):
        self._items = items
        self.requested = []

    def get(self, url, headers=None):
        self.requested.append((url, headers))
        return json.dumps({"items": self._items}).encode("utf-8")


def test_other_gcs_read_paths_are_not_gated_by_the_read_jsonl_check():
    # The restriction lives entirely in READ_JSONL's own binder branch
    # (visit_function_dataset) and JsonlReadNode.native_file_locations. OpteryxGcsFileSystem
    # itself -- used by catalog-backed / ad-hoc-registered-workspace GCS scans -- must
    # remain fully able to authenticate and list/open gs:// paths; nothing about this
    # fix touches that class.
    from opteryx.connectors.io_systems.gcs_filesystem import OpteryxGcsFileSystem

    filesystem = OpteryxGcsFileSystem.__new__(OpteryxGcsFileSystem)
    filesystem.bucket = None
    filesystem.http_client = _FakeGcsListHttpClient(
        [{"name": "space_missions/a.parquet", "size": "10", "updated": "2026-09-26T01:02:03Z"}]
    )
    filesystem.client_credentials = type("C", (), {"valid": True, "token": "fake-token"})()

    assert filesystem.list_files("opteryx/space_missions") == [
        "gs://opteryx/space_missions/a.parquet"
    ]


def test_read_jsonl_rejects_column_rename_alias(tmp_path):
    # AS alias(col1, col2, ...) -- renaming columns via the alias's own column
    # list -- is rejected outright rather than attempted: node.columns holds
    # pre-bind plain strings that are never replaced with bound LogicalColumn
    # objects downstream, so this shape used to crash later with an opaque
    # AttributeError instead of failing loud at the actual problem.
    jsonl_file = tmp_path / "ok.jsonl"
    jsonl_file.write_text('{"a": 1, "b": 2}\n')

    with pytest.raises(NotSupportedError):
        _run(f"SELECT * FROM READ_JSONL('{jsonl_file}') AS m(x, y)")


def test_read_jsonl_plain_alias_still_works(tmp_path):
    # A plain relation rename (no column list) is unaffected by the rejection above.
    jsonl_file = tmp_path / "ok.jsonl"
    jsonl_file.write_text('{"a": 1, "b": 2}\n')

    morsels = _run(f"SELECT * FROM READ_JSONL('{jsonl_file}') AS m")
    assert sum(m.num_rows for m in morsels) == 1


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
