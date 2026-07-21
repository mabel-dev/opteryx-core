"""READ_PARQUET must never sign a request or use this process's own ambient/platform
GCS service-account credentials for a user-supplied `gs://` path -- and, more
generally, must produce correct results for local paths, globs, and pushdown.

Same reasoning as tests/unit/connectors/test_read_jsonl_gcs_denied.py: READ_PARQUET
is a bare dataset function (opteryx.planner.binder.dataset.visit_function_dataset)
with no per-query authorization layer the way catalog-backed table scans have
(`can_perform_action`), so `gs://` support goes through the same
anonymous_gcs_filesystem() used by READ_JSONL -- a plain, unauthenticated HTTPS GET,
never the ambient service-account credential, never a signed URL. GCS's own
object-level IAM decides the outcome, not Opteryx.

Unlike READ_JSONL, READ_PARQUET reuses the engine's existing native ParquetReadNode
end-to-end (no bespoke reader) -- a real FileSystemTable connector + Manifest +
RelationSchema are built at bind time from the resolved file(s)' own Parquet
footer(s), exactly as a catalog-backed/ad-hoc Scan would, so this also exercises
that wiring: projection/predicate pushdown, glob resolution, and non-.parquet
files being silently excluded from a glob (mirroring FileSystemConnector's own
established behavior for ordinary directory-backed Parquet datasets).
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import InvalidFunctionParameterError
from opteryx.exceptions import NotSupportedError

SPACE_MISSIONS = "testdata/missions/space_missions.parquet"


def _run(sql):
    session = opteryx.session()
    return list(session.execute_to_morsels(sql))


def _credentials_must_not_be_touched(*args, **kwargs):
    raise AssertionError(
        "get_storage_credentials() was called -- READ_PARQUET must never touch "
        "platform GCS credentials for a user-supplied gs:// path."
    )


class _FakeAnonymousHttpClient:
    """See test_read_jsonl_gcs_denied.py's identical fake for the rationale: its
    `.get(url)` signature carries no headers/Authorization argument at all, so using
    it proves no credential could have been attached, not merely that this call site
    omitted one."""

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


def _real_parquet_bytes():
    with open(SPACE_MISSIONS, "rb") as f:
        return f.read()


def test_read_parquet_public_gcs_object_is_read_anonymously(anonymous_http):
    url = "https://storage.googleapis.com/opteryx/rugo_examples%2Fspace_missions.parquet"
    anonymous_http.objects[url] = _real_parquet_bytes()

    morsels = _run(
        "SELECT * FROM READ_PARQUET('gs://opteryx/rugo_examples/space_missions.parquet') LIMIT 3"
    )

    assert sum(m.num_rows for m in morsels) == 3
    # Fetched at least once anonymously, at the correctly-translated public URL --
    # never through OpteryxGcsFileSystem (get_storage_credentials would have raised).
    assert url in anonymous_http.requested


def test_read_parquet_private_gcs_object_fails_loud_not_silent(anonymous_http):
    # Nothing registered for this URL -- the fake 403s, exactly like a real private
    # object fetched with no Authorization header. Opteryx makes no allow/deny call
    # of its own; GCS's response is what decides this.
    with pytest.raises(DatasetReadError):
        _run(
            "SELECT * FROM READ_PARQUET("
            "'gs://opteryx_data/mitre/attack/attack_pattern/data/data-1767139228503.parquet')"
        )


def test_read_parquet_gcs_glob_is_rejected_before_any_network_call(anonymous_http):
    with pytest.raises(NotSupportedError):
        _run("SELECT * FROM READ_PARQUET('gs://opteryx/rugo_examples/*.parquet')")

    assert anonymous_http.requested == []


def test_read_parquet_gcs_scheme_is_rejected_not_treated_as_gs_alias(anonymous_http):
    # "gcs://" is deliberately NOT accepted as an alias for "gs://" here: the
    # native Parquet scan gate (opteryx.connectors.parquet_io.pool_reader.
    # native_scan_supported) only recognizes the literal "gs://" prefix as a
    # remote path -- "gcs://" isn't matched, so it would try to os.stat() the URI
    # as local and raise a raw RuntimeError instead of gracefully declining to
    # the trampoline scan. Rejected outright at bind time instead, before any
    # network call.
    with pytest.raises(InvalidFunctionParameterError):
        _run("SELECT * FROM READ_PARQUET('gcs://opteryx/rugo_examples/space_missions.parquet')")
    assert anonymous_http.requested == []


def test_read_parquet_local_path_reads_all_rows():
    morsels = _run(f"SELECT * FROM READ_PARQUET('{SPACE_MISSIONS}')")
    assert sum(m.num_rows for m in morsels) == 4630
    assert morsels[0].column_names == [
        b"Company", b"Location", b"Price", b"Lauched_at",
        b"Rocket", b"Rocket_Status", b"Mission", b"Mission_Status",
    ]


def test_read_parquet_projection_and_predicate_pushdown():
    morsels = _run(
        f"SELECT Company FROM READ_PARQUET('{SPACE_MISSIONS}') WHERE Company = 'SpaceX'"
    )
    assert morsels[0].column_names == [b"Company"]
    total = sum(m.num_rows for m in morsels)
    assert total > 0
    for m in morsels:
        for i in range(m.num_rows):
            assert m.column(b"Company")[i] == "SpaceX"


def test_read_parquet_glob_matches_local_directory(tmp_path):
    import shutil

    shutil.copy(SPACE_MISSIONS, tmp_path / "a.parquet")
    shutil.copy(SPACE_MISSIONS, tmp_path / "b.parquet")
    # A non-.parquet file in the same directory must be silently excluded from the
    # glob's matched set, mirroring FileSystemConnector's own established behavior
    # for ordinary directory-backed Parquet datasets -- not a stricter policy
    # invented for READ_PARQUET.
    (tmp_path / "readme.txt").write_text("not parquet")

    morsels = _run(f"SELECT * FROM READ_PARQUET('{tmp_path}/*.parquet')")
    assert sum(m.num_rows for m in morsels) == 4630 * 2


def test_read_parquet_glob_with_zero_matches_fails_loud(tmp_path):
    with pytest.raises(DatasetNotFoundError):
        _run(f"SELECT * FROM READ_PARQUET('{tmp_path}/*.parquet')")


def test_read_parquet_rejects_named_options():
    # Unlike READ_JSONL, Parquet's schema is read straight off the file's own
    # footer -- there is nothing analogous to ignore_errors/infer_schema to
    # configure, so any named option is a mistake, not silently ignored.
    with pytest.raises(InvalidFunctionParameterError):
        _run(f"SELECT * FROM READ_PARQUET('{SPACE_MISSIONS}', ignore_errors => true)")


def test_read_parquet_rejects_column_rename_alias():
    # AS alias(col1, col2, ...) -- renaming columns via the alias's own column
    # list -- is rejected outright rather than attempted: node.columns holds
    # pre-bind plain strings that are never replaced with bound LogicalColumn
    # objects downstream, so this shape used to crash later with an opaque
    # AttributeError instead of failing loud at the actual problem.
    with pytest.raises(NotSupportedError):
        _run(
            f"SELECT * FROM READ_PARQUET('{SPACE_MISSIONS}') "
            "AS m(a, b, c, d, e, f, g, h)"
        )


def test_read_parquet_plain_alias_still_works():
    # A plain relation rename (no column list) is unaffected by the rejection above.
    morsels = _run(f"SELECT * FROM READ_PARQUET('{SPACE_MISSIONS}') AS m LIMIT 1")
    assert sum(m.num_rows for m in morsels) == 1


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
