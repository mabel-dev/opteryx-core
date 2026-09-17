"""FileSystemTable's file-handle discipline.

These pin that a footer read releases the mapping it decoded from -- on the
schema path (`read_blob`) and on the metadata path (`get_dataset_metadata`).
Both run against the real local filesystem and real fixtures, so the decode
under test is the one production runs, and both hold the handle while asserting
(see RecordingFileSystem for why that matters).

This file also held two tests for the OLD selection hand-off, where `read_blob`
took a `decoder` callable and withheld `selection` from it when the filesystem
reported it had already applied the filters. That protocol no longer exists --
`read_blob` takes no decoder, nothing in the tree reads `filters_applied`, and
no filesystem can apply filters at all (GCS and S3 raise NotImplementedError
when `filters` is passed to `open_input_*`, as does the local filesystem's
`open_input_stream`). They were deleted as dead rather than left asserting a
capability the engine does not have; the pushdown-safety contract that replaced
them lives in `can_push` and the `supports_*_pushdown` gates.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.connectors import filesystem_connector
from opteryx.connectors.filesystem_connector import FileSystemTable
from opteryx.connectors.io_systems.local_filesystem import OpteryxLocalFileSystem
from opteryx.models.query_telemetry import QueryTelemetry

PARQUET_DATASET = os.path.join("testdata", "astronauts")
PARQUET_BLOB = os.path.join(PARQUET_DATASET, "astronauts.parquet")
SKENE_DATASET = os.path.join("testdata", "tpch_1_skene", "nation")

# Same convention as tests/unit/connectors/test_skene_footer_statistics.py --
# the skene fixtures are not populated in every checkout.
needs_skene = pytest.mark.skipif(
    not os.path.isdir(SKENE_DATASET), reason=f"{SKENE_DATASET} not populated"
)


class OpenedHandle:
    """One handle the connector was given, and what it did with it."""

    def __init__(self, path, handle):
        self.path = path
        self.handle = handle
        self.close_calls = 0


class RecordingFileSystem:
    """The real filesystem, with every handle it hands out kept and counted.

    Keeping the handle is the point: `MemoryMappedFile.__del__` closes too, so
    a handle the test has dropped would report `closed` even if the connector
    never released it. Holding a reference keeps the finalizer out of the way,
    leaving `closed` a statement about the connector alone.
    """

    def __init__(self, inner):
        self._inner = inner
        self.opened = []

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def _record(self, path, handle):
        opened = OpenedHandle(path, handle)
        real_close = handle.close

        def close():
            opened.close_calls += 1
            return real_close()

        handle.close = close
        self.opened.append(opened)
        return handle

    def open_input_stream(self, path, columns=None, filters=None):
        return self._record(path, self._inner.open_input_stream(path, columns=columns, filters=filters))

    def open_input_file(self, path, columns=None, filters=None):
        return self._record(path, self._inner.open_input_file(path, columns=columns, filters=filters))


def test_read_blob_closes_stream_after_decode():
    """The schema read must not leak the mapping it decodes the footer from.

    `read_blob` is schema-only now (data reads raise; parquet scans go through
    ParquetReadNode), so "after decode" means after rugo has parsed the footer
    out of the stream's memoryview. The handle is held here for the length of
    the assertion, which is what gives it teeth: `MemoryMappedFile.__del__`
    also closes, so an unreferenced handle would report closed whether the
    connector released it or not.
    """
    telemetry = QueryTelemetry("test_fs_close_read_blob")
    fs = RecordingFileSystem(OpteryxLocalFileSystem())
    # The footer cache short-circuits before the open on a hit, and it is
    # module-global, so a warm entry from an earlier test would leave nothing
    # to assert against.
    filesystem_connector._FOOTER_METADATA_CACHE.clear()

    table = FileSystemTable(
        dataset=PARQUET_DATASET, filesystem=fs, storage_type="TEST", telemetry=telemetry
    )
    schema = table.read_blob(blob_name=PARQUET_BLOB, just_schema=True)

    assert schema.columns, "the footer decode produced no columns"
    assert len(fs.opened) == 1, f"expected one open, saw {len(fs.opened)}"
    assert fs.opened[0].close_calls == 1
    assert fs.opened[0].handle.closed is True


@needs_skene
def test_get_dataset_metadata_closes_stream():
    """Every footer handle `get_dataset_metadata` opens must be released.

    Skene is the format whose statistics are read from Python here (the parquet
    branch batches its footers in C++ through `fetch_column_stats_many` and
    opens nothing on this side), so this is the path where a missed close leaks
    a mapping per file, per query.
    """
    telemetry = QueryTelemetry("test_fs_close_metadata")
    fs = RecordingFileSystem(OpteryxLocalFileSystem())
    # Same reason as the footer cache above: a manifest cache hit returns before
    # any file is opened.
    filesystem_connector._MANIFEST_CACHE.clear()

    table = FileSystemTable(
        dataset=SKENE_DATASET, filesystem=fs, storage_type="TEST", telemetry=telemetry
    )
    _, manifest = table.get_dataset_metadata()

    assert manifest.get_file_count() == 1
    assert fs.opened, "no file was opened, so nothing about closing was proven"
    for opened in fs.opened:
        assert opened.handle.closed is True, f"{opened.path} was left open"
