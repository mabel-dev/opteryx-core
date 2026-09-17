"""The local filesystem must refuse pushdown it cannot perform.

`columns` and `filters` are part of the opener signature every filesystem shares,
and none of them can honour either — reads that need column selection go through
the native Parquet scan path instead. HTTP, GCS and S3 all raise on both openers;
local raised on `open_input_stream` but accepted and silently DROPPED both
arguments in `open_input_file`, which is the shape of failure the engine contract
calls out by name: a caller asking for a filtered read got an unfiltered one back
with no signal that the filter had been discarded.

These pin the symmetry, on both openers, so it cannot drift apart again.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.connectors.io_systems.local_filesystem import OpteryxLocalFileSystem

BLOB = os.path.join("testdata", "astronauts", "astronauts.parquet")


@pytest.mark.parametrize("opener", ["open_input_stream", "open_input_file"])
def test_local_filesystem_rejects_column_projection(opener):
    fs = OpteryxLocalFileSystem()

    with pytest.raises(NotImplementedError, match="Column projection"):
        getattr(fs, opener)(BLOB, columns=["name"])


@pytest.mark.parametrize("opener", ["open_input_stream", "open_input_file"])
def test_local_filesystem_rejects_filters(opener):
    fs = OpteryxLocalFileSystem()

    with pytest.raises(NotImplementedError, match="filtering"):
        getattr(fs, opener)(BLOB, filters=[("year", ">", 1970)])


@pytest.mark.parametrize("opener", ["open_input_stream", "open_input_file"])
def test_local_filesystem_opens_without_pushdown(opener):
    """The guard must not fire on the ordinary read -- it gates the arguments,
    not the call."""
    fs = OpteryxLocalFileSystem()

    handle = getattr(fs, opener)(BLOB)
    try:
        assert bytes(handle.memoryview[:4]) == b"PAR1"
    finally:
        handle.close()
