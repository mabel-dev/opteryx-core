# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Object-store listing, one implementation (docs/ANONYMOUS_REMOTE_READS_DESIGN.md §2.1, §3).

The shared functions are driven with a fake `get`; the two anonymous filesystems are driven against a
local object store that RECORDS every request, so "no credential on the wire" is asserted, not hoped."""

import json
import os
import sys
import urllib.parse

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)))

from opteryx.compiled.http_client import HttpStatusError
from opteryx.connectors.io_systems.object_listing import DEFAULT_ANONYMOUS_LISTING_LIMIT
from opteryx.connectors.io_systems.object_listing import anonymous_listing_limit
from opteryx.connectors.io_systems.object_listing import list_gcs_objects
from opteryx.connectors.io_systems.object_listing import list_s3_objects
from opteryx.exceptions import DatasetReadError
from _recording_object_store import Store

UPDATED = "2026-10-25T21:02:08.697Z"


def _gcs_page(names, token=None):
    body = {"items": [{"name": n, "size": str(len(n)), "updated": UPDATED} for n in names]}
    if token:
        body["nextPageToken"] = token
    return json.dumps(body).encode()


def _s3_page(keys, truncated=False, token=None):
    contents = "".join(
        f"<Contents><Key>{k}</Key><Size>{len(k)}</Size><LastModified>{UPDATED}</LastModified></Contents>" for k in keys
    )
    more = f"<IsTruncated>true</IsTruncated><NextContinuationToken>{token}</NextContinuationToken>" if truncated else "<IsTruncated>false</IsTruncated>"
    return f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{contents}{more}</ListBucketResult>'.encode()


class TestGcsListing:
    def test_pages_are_followed_and_paths_are_fully_schemed(self):
        calls = []
        pages = {None: _gcs_page(["d/a.avro", "d/b.avro"], "t2"), "t2": _gcs_page(["d/c.avro"])}

        def get(url, headers):
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
            calls.append((url, headers))
            return pages[query.get("pageToken", [None])[0]]

        found = list_gcs_objects(get, "gs://bkt/d", True, headers={})
        assert [f.path for f in found] == ["gs://bkt/d/a.avro", "gs://bkt/d/b.avro", "gs://bkt/d/c.avro"]
        assert len(calls) == 2

    def test_a_placeholder_folder_object_is_not_a_file(self):
        found = list_gcs_objects(lambda u, h: _gcs_page(["d/", "d/a.orc"]), "bkt/d", True, headers={})
        assert [f.path for f in found] == ["gs://bkt/d/a.orc"]

    def test_the_prefix_is_a_directory_so_a_sibling_is_not_matched(self):
        seen = []
        list_gcs_objects(lambda u, h: seen.append(u) or _gcs_page([]), "bkt/space_missions", True, headers={})
        assert "prefix=space_missions%2F" in seen[0]

    def test_non_recursive_asks_for_a_delimiter(self):
        seen = []
        list_gcs_objects(lambda u, h: seen.append(u) or _gcs_page([]), "bkt/d", False, headers={})
        assert "delimiter=%2F" in seen[0]

    def test_headers_are_exactly_what_the_caller_supplied(self):
        seen = []
        list_gcs_objects(lambda u, h: seen.append(h) or _gcs_page([]), "bkt/d", True, headers={})
        list_gcs_objects(lambda u, h: seen.append(h) or _gcs_page([]), "bkt/d", True, headers={"Authorization": "Bearer x"})
        assert seen == [{}, {"Authorization": "Bearer x"}]

    @pytest.mark.parametrize("status", [401, 403])
    def test_a_denied_anonymous_listing_says_so_and_is_never_retried(self, status):
        attempts = []

        def get(url, headers):
            attempts.append(headers)
            raise HttpStatusError("HTTP denied", status)

        with pytest.raises(DatasetReadError, match=r"does not allow anonymous listing.*Name the file exactly"):
            list_gcs_objects(get, "bkt/d", True, headers={}, anonymous=True)
        assert attempts == [{}]

    def test_a_denied_credentialed_listing_keeps_its_ordinary_error(self):
        def get(url, headers):
            raise HttpStatusError("HTTP 403", 403)

        with pytest.raises(DatasetReadError, match="Unable to list"):
            list_gcs_objects(get, "bkt/d", True, headers={"Authorization": "Bearer x"})

    def test_a_listing_over_the_limit_raises_naming_the_limit(self):
        names = [f"d/f{i}" for i in range(11)]
        with pytest.raises(DatasetReadError, match="more than 10 objects"):
            list_gcs_objects(lambda u, h: _gcs_page(names), "bkt/d", True, headers={}, limit=10)
        assert len(list_gcs_objects(lambda u, h: _gcs_page(names), "bkt/d", True, headers={}, limit=11)) == 11


class TestS3Listing:
    def test_pages_placeholders_and_paths(self):
        pages = {None: _s3_page(["d/", "d/a.orc"], True, "tok"), "tok": _s3_page(["d/b.orc"])}

        def get(url, headers):
            return pages[urllib.parse.parse_qs(urllib.parse.urlsplit(url).query).get("continuation-token", [None])[0]]

        found = list_s3_objects(get, lambda b, q: f"http://h/{b}/?" + urllib.parse.urlencode(q), "s3://bkt/d", True)
        assert [f.path for f in found] == ["s3://bkt/d/a.orc", "s3://bkt/d/b.orc"]

    def test_denied_anonymous_listing(self):
        def get(url, headers):
            raise HttpStatusError("HTTP 403", 403)

        with pytest.raises(DatasetReadError, match="does not allow anonymous listing"):
            list_s3_objects(get, lambda b, q: "http://h/", "bkt/d", True, anonymous=True)

    def test_over_the_limit(self):
        with pytest.raises(DatasetReadError, match="more than 1 objects"):
            list_s3_objects(lambda u, h: _s3_page(["d/a", "d/b"]), lambda b, q: "http://h/", "bkt/d", True, limit=1)


def test_the_limit_setting(monkeypatch):
    monkeypatch.delenv("READ_FUNCTION_LISTING_LIMIT", raising=False)
    assert anonymous_listing_limit() == DEFAULT_ANONYMOUS_LISTING_LIMIT
    monkeypatch.setenv("READ_FUNCTION_LISTING_LIMIT", "25")
    assert anonymous_listing_limit() == 25
    for bad in ("0", "-3", "many"):
        monkeypatch.setenv("READ_FUNCTION_LISTING_LIMIT", bad)
        with pytest.raises(ValueError, match="positive integer"):
            anonymous_listing_limit()


# ── the anonymous filesystems, against a local object store that records requests ──


@pytest.fixture
def store():
    server = Store()
    server.objects.update(
        {
            "pub/formats/orc/": b"",
            "pub/formats/orc/space_missions.orc": b"o" * 10,
            "pub/formats/orc/space_missions.orc.bak": b"x",
            "pub/formats/orc_backup/other.orc": b"y",
        }
    )
    yield server
    server.shutdown()


def _assert_anonymous(store):
    assert store.requests, "nothing was requested"
    for _, query, headers in store.requests:
        lowered = {k.lower() for k in headers}
        assert "authorization" not in lowered
        assert "x-goog-api-key" not in lowered
        assert "X-Amz-" not in query and "x-amz-security-token" not in lowered


def test_anonymous_gcs_lists_with_no_credential_and_skips_the_placeholder(store):
    from opteryx.connectors.io_systems.anonymous_gcs_filesystem import AnonymousGcsFileSystem

    fs = AnonymousGcsFileSystem()
    fs.api = store.base
    assert fs.list_files("gs://pub/formats/orc") == [
        "gs://pub/formats/orc/space_missions.orc",
        "gs://pub/formats/orc/space_missions.orc.bak",
    ]
    _assert_anonymous(store)


def test_anonymous_gcs_denied_listing_fails_without_a_second_attempt(store):
    from opteryx.connectors.io_systems.anonymous_gcs_filesystem import AnonymousGcsFileSystem

    store.deny = True
    fs = AnonymousGcsFileSystem()
    fs.api = store.base
    with pytest.raises(DatasetReadError, match="does not allow anonymous listing"):
        fs.list_files("gs://pub/formats/orc")
    assert len(store.requests) == 1
    _assert_anonymous(store)


def test_anonymous_s3_lists_unsigned(store, monkeypatch):
    from opteryx.connectors.io_systems.anonymous_s3_filesystem import AnonymousS3FileSystem

    monkeypatch.setenv("AWS_S3_ENDPOINT", store.base)
    fs = AnonymousS3FileSystem()
    assert fs.list_files("s3://pub/formats/orc") == [
        "s3://pub/formats/orc/space_missions.orc",
        "s3://pub/formats/orc/space_missions.orc.bak",
    ]
    _assert_anonymous(store)


def test_anonymous_s3_denied_listing(store, monkeypatch):
    from opteryx.connectors.io_systems.anonymous_s3_filesystem import AnonymousS3FileSystem

    store.deny = True
    monkeypatch.setenv("AWS_S3_ENDPOINT", store.base)
    with pytest.raises(DatasetReadError, match="does not allow anonymous listing"):
        AnonymousS3FileSystem().list_files("s3://pub/formats/orc")
    assert len(store.requests) == 1
    _assert_anonymous(store)


def test_the_anonymous_listing_limit_applies(store, monkeypatch):
    from opteryx.connectors.io_systems.anonymous_gcs_filesystem import AnonymousGcsFileSystem

    monkeypatch.setenv("READ_FUNCTION_LISTING_LIMIT", "1")
    fs = AnonymousGcsFileSystem()
    fs.api = store.base
    with pytest.raises(DatasetReadError, match="more than 1 objects"):
        fs.list_files("gs://pub/formats/orc")


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
