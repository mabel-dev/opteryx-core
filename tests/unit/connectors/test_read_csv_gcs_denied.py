"""READ_CSV must never sign a request or use this process's own ambient/platform
GCS service-account credentials for a user-supplied `gs://` path.

Identical reasoning and mechanism to test_read_jsonl_gcs_denied.py -- READ_CSV is
equally a bare dataset function (opteryx.planner.binder.dataset.
visit_function_dataset) with no per-query authorization check, so it must apply
the same anonymous-GCS-read policy READ_JSONL/READ_PARQUET already do. See that
file's module docstring for the full threat-model writeup; not repeated here.
"""

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
        "get_storage_credentials() was called -- READ_CSV must never touch platform "
        "GCS credentials for a user-supplied gs:// path."
    )


class _FakeAnonymousHttpClient:
    """See test_read_jsonl_gcs_denied.py's identical class for the full rationale."""

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


def test_read_csv_public_gcs_object_is_read_anonymously(anonymous_http):
    url = "https://storage.googleapis.com/opteryx/rugo_examples%2Fspace_missions.csv"
    anonymous_http.objects[url] = b"a,b\n1,2\n3,4\n"

    morsels = _run("SELECT * FROM READ_CSV('gs://opteryx/rugo_examples/space_missions.csv')")

    assert sum(m.num_rows for m in morsels) == 2
    # Fetched twice: once at bind time (schema resolution) and once at execution
    # time -- the same two-read shape READ_JSONL/READ_CSV already has for local
    # files; both reads must be anonymous, which is what this asserts.
    assert anonymous_http.requested == [url, url]


def test_read_csv_private_gcs_object_fails_loud_not_silent(anonymous_http):
    with pytest.raises(DatasetReadError):
        _run("SELECT * FROM READ_CSV('gs://opteryx_data/some/private/file.csv')")


def test_read_csv_gcs_glob_is_rejected_before_any_network_call(anonymous_http):
    with pytest.raises(NotSupportedError):
        _run("SELECT * FROM READ_CSV('gs://opteryx/rugo_examples/*.csv')")

    assert anonymous_http.requested == []


def test_read_csv_gcs_scheme_is_rejected_not_treated_as_gs_alias(anonymous_http):
    with pytest.raises(InvalidFunctionParameterError):
        _run("SELECT * FROM READ_CSV('gcs://opteryx/rugo_examples/space_missions.csv')")
    assert anonymous_http.requested == []


def test_read_csv_local_paths_are_unaffected(tmp_path):
    csv_file = tmp_path / "ok.csv"
    csv_file.write_text("a,b\n1,2\n3,4\n")

    morsels = _run(f"SELECT * FROM READ_CSV('{csv_file}')")
    assert sum(m.num_rows for m in morsels) == 2


def test_read_csv_local_glob_is_unaffected(tmp_path):
    (tmp_path / "a.csv").write_text("a\n1\n")
    (tmp_path / "b.csv").write_text("a\n2\n")

    morsels = _run(f"SELECT * FROM READ_CSV('{tmp_path}/*.csv')")
    assert sum(m.num_rows for m in morsels) == 2


def test_read_csv_rejects_column_rename_alias(tmp_path):
    csv_file = tmp_path / "ok.csv"
    csv_file.write_text("a,b\n1,2\n")

    with pytest.raises(NotSupportedError):
        _run(f"SELECT * FROM READ_CSV('{csv_file}') AS m(x, y)")


def test_read_csv_plain_alias_still_works(tmp_path):
    csv_file = tmp_path / "ok.csv"
    csv_file.write_text("a,b\n1,2\n")

    morsels = _run(f"SELECT * FROM READ_CSV('{csv_file}') AS m")
    assert sum(m.num_rows for m in morsels) == 1


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
