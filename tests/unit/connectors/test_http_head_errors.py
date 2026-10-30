"""HTTP HEAD: only a 404/410 means "not found". A 403, a timeout or a refused
connection used to be reported as a missing file of size 0 and bind anyway."""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.http_client import HttpStatusError
from opteryx.connectors.io_systems.http_filesystem import FileType
from opteryx.connectors.io_systems.http_filesystem import OpteryxHttpFileSystem


class _Head:
    def __init__(self, status):
        self.status = status

    def head(self, url):
        raise HttpStatusError(f"HEAD {url} failed", self.status)


@pytest.mark.parametrize("status", [404, 410])
def test_missing_object_is_not_found(status):
    fs = OpteryxHttpFileSystem()
    fs.http_client = _Head(status)
    assert fs.get_file_info("https://example.com/x.parquet").type == FileType.NotFound


@pytest.mark.parametrize("status", [403, 405, 500, 0])
def test_other_failures_raise(status):
    fs = OpteryxHttpFileSystem()
    fs.http_client = _Head(status)
    with pytest.raises(HttpStatusError):
        fs.get_file_info("https://example.com/x.parquet")


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
