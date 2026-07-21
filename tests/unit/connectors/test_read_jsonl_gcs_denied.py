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
not from any allow/deny logic in Opteryx. Bucket LISTING is a separate IAM
permission from object GET and is not assumed granted anonymously, so glob patterns
are rejected outright for gs:// rather than silently escalating to an authenticated
listing call.

These tests pin that: the platform credential path is never touched (proven both by
the fake HTTP client's `.get(url)` signature, which cannot carry an Authorization
header, and by monkeypatching `get_storage_credentials` to blow up if it's ever
called), a public object is readable, a private one fails loud, globs are rejected
before any network call, local paths/globs are unaffected, and other GCS read paths
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
    requested = []

    def get(self, url):
        type(self).requested.append(url)
        if url in type(self).objects:
            return type(self).objects[url]
        raise RuntimeError("HTTP 403: access denied")


@pytest.fixture(autouse=True)
def _reset_fake_http_client():
    _FakeAnonymousHttpClient.objects = {}
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


def test_read_jsonl_public_gcs_object_is_read_anonymously(anonymous_http):
    # _AnonymousGcsFileSystem quotes the object path the same way GcsFile does
    # (urllib.parse.quote(..., safe="")) -- '/' inside the object name becomes '%2F'.
    url = "https://storage.googleapis.com/opteryx/rugo_examples%2Fspace_missions.jsonl"
    anonymous_http.objects[url] = b'{"a": 1}\n{"a": 2}\n'

    morsels = _run("SELECT * FROM READ_JSONL('gs://opteryx/rugo_examples/space_missions.jsonl')")

    assert sum(m.num_rows for m in morsels) == 2
    # Fetched twice: once at bind time (schema resolution) and once at execution time
    # (the real chunk decode) -- the same two-read shape READ_JSONL already has for
    # local files; both reads must be anonymous, which is what this asserts.
    assert anonymous_http.requested == [url, url]


def test_read_jsonl_private_gcs_object_fails_loud_not_silent(anonymous_http):
    # Nothing is registered for this URL -- the fake 403s, exactly like a real private
    # object fetched with no Authorization header. Opteryx makes no allow/deny call of
    # its own; GCS's response is what decides this.
    with pytest.raises(DatasetReadError):
        _run(
            "SELECT * FROM READ_JSONL("
            "'gs://opteryx_data/mitre/attack/attack_pattern/data/data-1767139228503.jsonl')"
        )


def test_read_jsonl_gcs_glob_is_rejected_before_any_network_call(anonymous_http):
    # Bucket LISTING is a separate IAM permission from object GET -- anonymous listing
    # isn't assumed available, so a glob over gs:// must fail loud with no request made,
    # rather than silently escalating to an authenticated (platform-credentialed) listing.
    with pytest.raises(NotSupportedError):
        _run("SELECT * FROM READ_JSONL('gs://opteryx/rugo_examples/*.jsonl')")

    assert anonymous_http.requested == []


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
    # (visit_function_dataset) and JsonlReadNode._ensure_filesystem. OpteryxGcsFileSystem
    # itself -- used by catalog-backed / ad-hoc-registered-workspace GCS scans -- must
    # remain fully able to authenticate and list/open gs:// paths; nothing about this
    # fix touches that class.
    from opteryx.connectors.io_systems.gcs_filesystem import OpteryxGcsFileSystem

    filesystem = OpteryxGcsFileSystem.__new__(OpteryxGcsFileSystem)
    filesystem.bucket = None
    filesystem.http_client = _FakeGcsListHttpClient([{"name": "space_missions/a.parquet"}])
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
