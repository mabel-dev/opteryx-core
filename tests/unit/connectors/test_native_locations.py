# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""native_locations: where a native whole-file Source reads each file from, and how the read
authenticates - the filesystem's own declaration, exactly one mechanism
(docs/REMOTE_AVRO_ORC_DATA_FILES_DESIGN.md §3, §4.1)."""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.connectors.io_systems.native_locations import native_locations
from opteryx.exceptions import NotSupportedError


class HeaderFilesystem:
    """GCS-like: authenticates by header, URLs unsigned."""

    signs_urls = False

    def native_auth_header(self):
        return "Bearer token-1"

    def object_url(self, path):
        return "https://objects.example/" + path.split("://", 1)[1]

    def rewrite_to_signed_url(self, path, expiry_seconds=3600):  # pragma: no cover
        raise AssertionError("a header filesystem must not be asked to sign")


class SigningFilesystem:
    """S3-like: every URL carries its credential, no header."""

    signs_urls = True

    def native_auth_header(self):
        return None

    def rewrite_to_signed_url(self, path, expiry_seconds=3600):
        return "https://objects.example/" + path.split("://", 1)[1] + "?X-Amz-Signature=abc"

    def object_url(self, path):  # pragma: no cover
        raise AssertionError("a signing filesystem must not be asked for an unsigned URL")


class NeitherFilesystem:
    signs_urls = False

    def native_auth_header(self):
        return None

    def object_url(self, path):  # pragma: no cover
        raise AssertionError("not reached")


FILES = ["gs://b/t/a.avro", "gs://b/t/b.avro"]


def test_local_paths_are_memory_mapped_and_need_no_filesystem_mechanism():
    assert native_locations(NeitherFilesystem(), ["/data/a.avro", "rel/b.avro"], "t") == (["", ""], "")


def test_a_header_filesystem_gives_unsigned_urls_and_one_header():
    urls, header = native_locations(HeaderFilesystem(), FILES, "t")
    assert urls == ["https://objects.example/b/t/a.avro", "https://objects.example/b/t/b.avro"]
    assert header == "Bearer token-1"


def test_a_signing_filesystem_gives_signed_urls_and_no_header():
    urls, header = native_locations(SigningFilesystem(), ["s3://b/t/a.orc"], "t")
    assert urls == ["https://objects.example/b/t/a.orc?X-Amz-Signature=abc"]
    assert header == ""


def test_exactly_one_mechanism_never_neither():
    with pytest.raises(NotSupportedError, match="declares neither signed URLs nor an authorization header"):
        native_locations(NeitherFilesystem(), FILES, "t")


@pytest.mark.parametrize("path", ["s3a://b/a.avro", "gcs://b/a.avro", "https://h/a.avro", "abfs://c/a.avro"])
def test_a_scheme_the_sources_do_not_read_names_the_file(path):
    with pytest.raises(NotSupportedError, match=r"scheme '[a-z0-9]+://'"):
        native_locations(HeaderFilesystem(), [path], "t")


def test_a_dataset_mixing_local_and_remote_files_is_refused():
    with pytest.raises(NotSupportedError, match="mixes local and remote"):
        native_locations(HeaderFilesystem(), ["/local/a.avro", "gs://b/a.avro"], "t")


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
