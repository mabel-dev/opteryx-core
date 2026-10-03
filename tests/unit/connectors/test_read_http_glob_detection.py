"""
http(s) paths in READ_PARQUET / READ_JSONL / READ_CSV.

A URL's '?' opens its query string, not a glob -- `...file.parquet?authuser=0` used
to be treated as a pattern, sent to `list_files` on the http filesystem (which has
none), and escaped as `AttributeError: 'OpteryxHttpFileSystem' object has no
attribute 'list_files'`. A real glob over http(s) has nothing to list either, so it
is refused as NotSupportedError instead of crashing.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.connectors.io_systems.http_filesystem import OpteryxHttpFileSystem
from opteryx.exceptions import NotSupportedError
from opteryx.planner.binder.dataset import _is_glob_pattern
from opteryx.planner.binder.dataset import _resolve_glob_files


@pytest.mark.parametrize(
    "path",
    [
        "https://storage.example.com/bucket/file.parquet?authuser=0",
        "https://example.com/data.jsonl?a=1&b=[2]",
        "http://example.com/data.csv#section*",
    ],
)
def test_query_string_and_fragment_are_not_globs(path):
    assert not _is_glob_pattern(path)


@pytest.mark.parametrize(
    "path",
    [
        "https://example.com/*.parquet",
        "https://example.com/a[12].jsonl?x=1",
        "local/dir/file?.csv",
        "gs://bucket/*.parquet",
    ],
)
def test_globs_in_the_path_are_still_globs(path):
    assert _is_glob_pattern(path)


def test_http_glob_is_refused_not_an_attribute_error():
    with pytest.raises(NotSupportedError, match="not supported for https://"):
        _resolve_glob_files("https://example.com/*.parquet", OpteryxHttpFileSystem())


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
