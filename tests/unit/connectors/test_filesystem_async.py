from io import BytesIO

import pytest

from opteryx.connectors.filesystem_connector import FileSystemTable
from opteryx.models.query_telemetry import QueryTelemetry


class FakeFS:
    def __init__(self, data: bytes):
        self._data = data

    def open_input_stream(self, path: str, columns=None, filters=None):
        return BytesIO(self._data)


class FakePool:
    def __init__(self, to_return=None):
        self.calls = 0
        self.to_return = to_return

    async def commit(self, data: bytes):
        self.calls += 1
        if isinstance(self.to_return, list):
            # pop from list to simulate transient failures
            return self.to_return.pop(0)
        return self.to_return


@pytest.mark.asyncio
async def test_async_read_blob_commits_and_returns_ref():
    telemetry = QueryTelemetry("test_fs_1")
    fs = FakeFS(b"hello world")
    table = FileSystemTable(dataset="some/path", filesystem=fs, storage_type="TEST", telemetry=telemetry)

    pool = FakePool(to_return=42)
    ref = await table.async_read_blob(blob_name="some/path/file.parquet", pool=pool, telemetry=telemetry)

    assert ref == 42
    assert telemetry.bytes_read == len(b"hello world")


@pytest.mark.asyncio
async def test_async_read_blob_retries_on_commit_failure():
    telemetry = QueryTelemetry("test_fs_2")
    fs = FakeFS(b"abcd")
    table = FileSystemTable(dataset="some/path", filesystem=fs, storage_type="TEST", telemetry=telemetry)

    # Simulate two failures (None) before success (100)
    pool = FakePool(to_return=[None, None, 100])

    ref = await table.async_read_blob(blob_name="some/path/f", pool=pool, telemetry=telemetry)

    assert ref == 100
    # Should have retried at least twice
    assert pool.calls >= 3
    assert telemetry.stalls_io_waiting_on_engine >= 2
