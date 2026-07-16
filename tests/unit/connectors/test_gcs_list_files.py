"""OpteryxGcsFileSystem.list_files — the blob listing ad-hoc GCS datasets depend on.

`FileSystemConnector.get_list_of_blob_names` calls `filesystem.list_files(...)`; without it
every ad-hoc GCS query dies at listing. These tests pin the three decisions in it that would
otherwise fail SILENTLY (wrong rows, not an error), using a fake HTTP client so no
credentials or network are needed:

  * paths come back `gs://`-schemed — bare paths would be classed local by `_is_local_path`
    and read off the local disk by the native scan path;
  * the prefix is `/`-terminated — GCS prefix matching is a plain string match, so a sibling
    dataset sharing a name stem would be silently absorbed into this one;
  * listings paginate — a dataset over the API's page limit would silently under-read.
"""

import json
import urllib.parse

import pytest

from opteryx.connectors.io_systems.gcs_filesystem import OpteryxGcsFileSystem


class FakeHttpClient:
    """Serves canned GCS JSON-API list responses and records the URLs requested."""

    def __init__(self, pages):
        self.pages = pages
        self.requested = []
        self._call = 0

    def get(self, url, headers=None):
        self.requested.append(url)
        page = self.pages[self._call]
        self._call += 1
        return json.dumps(page).encode("utf-8")


def _fs(pages):
    # Bypass __init__: it acquires real GCS credentials, which this test must not need.
    fs = OpteryxGcsFileSystem.__new__(OpteryxGcsFileSystem)
    fs.bucket = None
    fs.http_client = FakeHttpClient(pages)
    # `_bearer` is a property that would refresh a real credential; stub the token source.
    fs.client_credentials = type("C", (), {"valid": True, "token": "fake-token"})()
    return fs


def _query_params(url):
    return urllib.parse.parse_qs(urllib.parse.urlparse(url).query)


def test_paths_are_returned_gs_schemed():
    # Bare `bucket/object` paths would be treated as LOCAL downstream (_is_local_path) and
    # pread() off disk by the native scan path — a silently wrong read, not an error.
    fs = _fs([{"items": [{"name": "space_missions/a.parquet"}]}])
    assert fs.list_files("opteryx/space_missions") == [
        "gs://opteryx/space_missions/a.parquet"
    ]


def test_scheme_on_the_input_is_accepted_and_not_doubled():
    fs = _fs([{"items": [{"name": "space_missions/a.parquet"}]}])
    assert fs.list_files("gs://opteryx/space_missions") == [
        "gs://opteryx/space_missions/a.parquet"
    ]


def test_prefix_is_slash_terminated_so_siblings_cannot_leak_in():
    # Without the trailing slash, listing "space_missions" also returns
    # "space_missions_backup/..." — another dataset's blobs, silently merged into this one.
    fs = _fs([{"items": []}])
    fs.list_files("opteryx/space_missions")
    assert _query_params(fs.http_client.requested[0])["prefix"] == ["space_missions/"]


def test_whole_bucket_listing_uses_no_prefix_filter():
    fs = _fs([{"items": [{"name": "a.parquet"}]}])
    assert fs.list_files("opteryx") == ["gs://opteryx/a.parquet"]
    # An empty prefix is dropped by urlencode-with-empty-value semantics or sent empty;
    # either way it must not become a spurious "/" filter that matches nothing.
    assert _query_params(fs.http_client.requested[0]).get("prefix", [""]) == [""]


def test_listing_paginates_until_the_token_is_exhausted():
    # A dataset over the API's 1000-object page limit must not silently truncate.
    fs = _fs(
        [
            {"items": [{"name": "many/1.parquet"}], "nextPageToken": "t1"},
            {"items": [{"name": "many/2.parquet"}], "nextPageToken": "t2"},
            {"items": [{"name": "many/3.parquet"}]},
        ]
    )
    assert fs.list_files("opteryx/many") == [
        "gs://opteryx/many/1.parquet",
        "gs://opteryx/many/2.parquet",
        "gs://opteryx/many/3.parquet",
    ]
    assert len(fs.http_client.requested) == 3
    assert _query_params(fs.http_client.requested[1])["pageToken"] == ["t1"]
    assert _query_params(fs.http_client.requested[2])["pageToken"] == ["t2"]


def test_folder_placeholder_objects_are_skipped():
    # Console-created zero-byte "folder" markers are not readable data files.
    fs = _fs([{"items": [{"name": "ds/"}, {"name": "ds/real.parquet"}]}])
    assert fs.list_files("opteryx/ds") == ["gs://opteryx/ds/real.parquet"]


def test_non_recursive_listing_sets_a_delimiter():
    fs = _fs([{"items": []}])
    fs.list_files("opteryx/ds", recursive=False)
    assert _query_params(fs.http_client.requested[0])["delimiter"] == ["/"]


def test_recursive_listing_sets_no_delimiter():
    fs = _fs([{"items": []}])
    fs.list_files("opteryx/ds", recursive=True)
    assert "delimiter" not in _query_params(fs.http_client.requested[0])


def test_a_path_without_a_bucket_fails_loud():
    fs = _fs([{"items": []}])
    with pytest.raises(ValueError):
        fs.list_files("gs://")


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
