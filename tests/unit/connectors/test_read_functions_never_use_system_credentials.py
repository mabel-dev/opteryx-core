# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""THE INVARIANT (docs/ANONYMOUS_REMOTE_READS_DESIGN.md §0): a READ_* call never uses the
platform's credentials. Not as a fallback, not to retry a denied listing, not ever.

Tested structurally, not by hoping a request looks anonymous: the credentialed filesystem classes and
the credential resolvers are rigged to RAISE if touched, real-looking credentials are put in the
environment, and every READ_* over gs:// and s3:// - an exact file and a glob - must succeed without
tripping them. And on the wire: a recording object store sees no Authorization header and no SigV4
query on ANY request, listings included. A bucket that denies anonymous listing fails the query; it
is never retried with a credential."""

import io
import os
import sys
import time

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)))
os.environ["TZ"] = "UTC"
time.tzset()

import pyarrow as pa
import pyarrow.orc as po

import opteryx
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import NotSupportedError
from rugo import avro, csv, jsonl, orc, parquet

from _recording_object_store import Store

ROWS = 40
EXTENSIONS = {"parquet": "parquet", "avro": "avro", "jsonl": "jsonl", "csv": "csv", "orc": "orc"}
FUNCTIONS = {"parquet": "READ_PARQUET", "avro": "READ_AVRO", "jsonl": "READ_JSONL", "csv": "READ_CSV", "orc": "READ_ORC"}


def _files():
    table = pa.table(
        {
            "id": pa.array(range(ROWS), pa.int64()),
            "name": pa.array([f"n{i}" for i in range(ROWS)]),
            "score": pa.array([i / 2 for i in range(ROWS)], pa.float64()),
        }
    )
    orc_buf = io.BytesIO()
    po.write_table(table, orc_buf, compression="zstd")
    orc_bytes = orc_buf.getvalue()
    morsels = list(orc.read_orc(orc_bytes))
    morsel = morsels[0]
    for other in morsels[1:]:
        morsel = morsel.combine(other)
    return {
        "parquet": parquet.write_parquet(morsel),
        "avro": avro.write_avro(morsel),
        "jsonl": jsonl.write_jsonl(morsel),
        "csv": csv.write_csv(morsel),
        "orc": orc_bytes,
    }


@pytest.fixture
def world(monkeypatch):
    """A recording object store reachable as gs://pub/... and s3://pub/..., plus tripwires on every
    credentialed path. Real-looking platform credentials sit in the environment throughout."""
    store = Store()
    for fmt, data in _files().items():
        store.objects[f"pub/formats/{fmt}/space_missions.{EXTENSIONS[fmt]}"] = data
    store.objects["pub/formats/parquet/"] = b""          # a console "folder" placeholder
    store.objects["pub/formats/parquet_backup/other.parquet"] = b"x"   # a sibling prefix

    # gs:// -> the local store, for listing and for object fetches
    import opteryx.connectors.io_systems.object_listing as object_listing
    from opteryx.connectors.io_systems.anonymous_gcs_filesystem import AnonymousGcsFileSystem

    monkeypatch.setattr(object_listing, "GCS_API", store.base)
    monkeypatch.setattr(
        AnonymousGcsFileSystem,
        "_to_public_https_url",
        staticmethod(lambda path: f"{store.base}/{path.split('://', 1)[1] if '://' in path else path}"),
    )
    # s3:// -> the local store (path-style, no signature)
    monkeypatch.setenv("AWS_S3_ENDPOINT", store.base)
    monkeypatch.setenv("AWS_REGION", "eu-west-2")
    # real-looking platform credentials: present, and must be ignored
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAPLATFORMCREDENTIAL")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "platform-secret-must-never-be-used")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent/platform-key.json")

    # the tripwires
    from opteryx.connectors.io_systems import gcs_filesystem, s3_filesystem

    def tripped(what):
        def raiser(*args, **kwargs):
            raise AssertionError(f"a READ_* request touched the platform's credentials: {what}")
        return raiser

    monkeypatch.setattr(gcs_filesystem.OpteryxGcsFileSystem, "__init__", tripped("OpteryxGcsFileSystem()"))
    monkeypatch.setattr(gcs_filesystem, "get_storage_credentials", tripped("get_storage_credentials"))
    monkeypatch.setattr(s3_filesystem.OpteryxS3FileSystem, "__init__", tripped("OpteryxS3FileSystem()"))
    monkeypatch.setattr(s3_filesystem.CredentialChain, "resolve", tripped("CredentialChain.resolve"))
    yield store
    store.shutdown()


def _scalar(sql):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        rows.extend(zip(*[morsel.column(n).to_pylist() for n in morsel.column_names]))
    return rows


def _assert_nothing_authenticated(store):
    assert store.requests, "nothing was requested"
    for path, query, headers in store.requests:
        lowered = {k.lower() for k in headers}
        assert "authorization" not in lowered, (path, headers)
        assert "x-amz-security-token" not in lowered and "x-goog-api-key" not in lowered, (path, headers)
        assert "x-amz-signature" not in query.lower() and "x-goog-signature" not in query.lower(), (path, query)
        assert "AKIAPLATFORMCREDENTIAL" not in query and "AKIAPLATFORMCREDENTIAL" not in str(headers)


@pytest.mark.parametrize("scheme", ["gs", "s3"])
@pytest.mark.parametrize("fmt", sorted(EXTENSIONS))
def test_an_exact_object_is_read_without_the_platforms_credentials(world, scheme, fmt):
    path = f"{scheme}://pub/formats/{fmt}/space_missions.{EXTENSIONS[fmt]}"
    rows = _scalar(f"SELECT COUNT(*), SUM(id) FROM {FUNCTIONS[fmt]}('{path}')")
    assert rows == [(ROWS, sum(range(ROWS)))]
    _assert_nothing_authenticated(world)


@pytest.mark.parametrize("scheme", ["gs", "s3"])
@pytest.mark.parametrize("fmt", sorted(EXTENSIONS))
def test_a_glob_is_listed_anonymously_and_read(world, scheme, fmt):
    path = f"{scheme}://pub/formats/{fmt}/*.{EXTENSIONS[fmt]}"
    rows = _scalar(f"SELECT COUNT(*), SUM(id) FROM {FUNCTIONS[fmt]}('{path}')")
    assert rows == [(ROWS, sum(range(ROWS)))]
    listings = [r for r in world.requests if "list-type" in r[1] or r[0].startswith("/storage/v1/b/")]
    assert listings, "a glob must LIST the prefix"
    _assert_nothing_authenticated(world)


@pytest.mark.parametrize("scheme", ["gs", "s3"])
def test_a_denied_listing_fails_the_query_and_is_never_retried_with_a_credential(world, scheme):
    world.deny = True
    with pytest.raises((DatasetReadError, NotSupportedError), match="does not allow anonymous listing"):
        _scalar(f"SELECT COUNT(*) FROM READ_PARQUET('{scheme}://pub/formats/parquet/*.parquet')")
    _assert_nothing_authenticated(world)
    assert len(world.requests) == 1


@pytest.mark.parametrize("scheme", ["gs", "s3"])
def test_a_sibling_prefix_and_the_folder_placeholder_are_not_read(world, scheme):
    rows = _scalar(f"SELECT COUNT(*) FROM READ_PARQUET('{scheme}://pub/formats/parquet/*.parquet')")
    assert rows == [(ROWS,)]   # parquet_backup/other.parquet would have failed to decode


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
