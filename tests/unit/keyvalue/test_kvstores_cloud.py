# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The GCS-backed key-value store, against a fake client.

What is faked and why: `GCSKeyValueStore` gets its client from `_gcs_client()`,
which imports `google.cloud.storage` and constructs a `Client()` from Application
Default Credentials. Exercising get/set/contains/delete for real would need
credentials and a bucket, so the client is replaced - but the STORE is real, and
its key naming, prefixing and not-found handling are what is under test.

The client is replaced at `_gcs_client`, the function the store calls. This used
to be done by putting fake modules into `sys.modules["google"]`,
`["google.cloud"]` and `["google.cloud.storage"]` and DELETING all three
afterwards, which does not restore the real ones - it evicts them. Any module
imported underneath (`google.auth`, imported by
opteryx/connectors/io_systems/gcs_filesystem.py, and by any test which patches
it) then stayed in `sys.modules` while its parent package did not, so the next
`import google.auth` was a no-op that never re-bound the attribute and
`google.auth.default(...)` raised

    AttributeError: module 'google' has no attribute 'auth'

in whatever ran later - the GCS storage tests, in a full-suite run. Patching one
function needs no global surgery, and what it replaces it puts back.

The s3/minio tests that were here are gone: there is no s3 key-value store. The
factory serves file, gs/gcs, valkey, memory and null, and `s3://` raises
`Unknown KV store scheme: s3` - which is what those tests had been failing with.
"""

import os
import sys
from contextlib import contextmanager
from typing import Dict

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.managers.kvstores import create_kv_store
from opteryx.managers.kvstores import gcs_kv_store

_CTX = {"query_id": "q1", "operator_id": "op1"}


class FakeBlob:
    def __init__(self, data: bytes = b""):
        self._data = data

    def download_as_bytes(self):
        if self._data is None:
            raise KeyError("NotFound")
        return self._data

    def upload_from_string(self, value: bytes):
        self._data = value

    def exists(self):
        return self._data is not None

    def delete(self):
        self._data = None


class FakeBucket:
    def __init__(self):
        self._blobs: Dict[str, FakeBlob] = {}

    def blob(self, name: str):
        if name not in self._blobs:
            self._blobs[name] = FakeBlob(None)
        return self._blobs[name]


class FakeGCSClient:
    def __init__(self):
        self._buckets = {"bucket": FakeBucket()}

    def bucket(self, name: str):
        if name not in self._buckets:
            self._buckets[name] = FakeBucket()
        return self._buckets[name]


@contextmanager
def fake_client(error_class=None):
    """Point the store at a fake client, and put back what was there.

    Saving and restoring, never deleting: what was replaced is what goes back.
    """
    original_client = gcs_kv_store._gcs_client
    original_error = gcs_kv_store.GoogleAPIError

    gcs_kv_store._gcs_client = lambda: FakeGCSClient()
    if error_class is not None:
        gcs_kv_store.GoogleAPIError = error_class
    try:
        yield
    finally:
        gcs_kv_store._gcs_client = original_client
        gcs_kv_store.GoogleAPIError = original_error


def test_gcs_kv_store_with_fake_client():
    with fake_client():
        store = create_kv_store("gs://bucket/pfx")
        key = b"0x1"
        assert store.get(key, **_CTX) is None
        val = b"hello"
        store.set(key, val, **_CTX)
        assert store.get(key, **_CTX) == val
        assert store.contains([key], **_CTX) == [key]
        store.delete(key, **_CTX)
        assert store.get(key, **_CTX) is None


def test_gcs_kv_store_with_fake_client_and_googleapierror_class():
    """A missing object reads as not-found whether the client raises KeyError or
    the real `google.api_core.exceptions.GoogleAPIError`.

    `_gcs_client()` sets the module-level `GoogleAPIError` when it can import the
    real one, so that is what gets replaced - the previous version faked the
    `google.api_core.exceptions` module to make it happen.
    """

    class GoogleErr(Exception):
        pass

    with fake_client(error_class=GoogleErr):
        store = create_kv_store("gs://bucket/pfx")
        assert store.get(b"0x1", **_CTX) is None


def test_the_store_is_real_even_though_the_client_is_not():
    """The prefix in the location becomes part of the object name - the naming is
    the store's own, and is the thing worth testing here."""
    with fake_client():
        store = create_kv_store("gs://bucket/pfx")
        assert store._object_name(b"0x1").startswith("pfx/")

        unprefixed = create_kv_store("gs://bucket")
        assert "/" not in unprefixed._object_name(b"0x1")


def test_faking_the_client_leaves_the_google_package_alone():
    """The regression the sys.modules surgery caused: `google.auth` has to remain
    reachable through the `google` package after a test has used a fake client."""
    with fake_client():
        create_kv_store("gs://bucket/pfx").get(b"0x1", **_CTX)

    import google.auth

    assert callable(google.auth.default)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
