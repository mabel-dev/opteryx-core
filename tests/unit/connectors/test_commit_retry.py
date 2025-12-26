
# Note: GcpCloudStorageConnector no longer exists as a separate class
# It's now implemented via FileSystemConnector with GCS filesystem
# This test mocks the async blob reading behavior
from opteryx.models.query_telemetry import QueryTelemetry


class FakeResponse:
    def __init__(self, data: bytes, status: int = 200):
        self._data = data
        self.status = status

    async def read(self):
        return self._data


class FakeSession:
    def __init__(self, data: bytes):
        self._data = data

    async def get(self, url, headers=None, timeout=None):
        return FakeResponse(self._data, status=200)


class FlakyPool:
    """Simulate MemoryPool.commit returning -1 first, then a valid ref."""

    def __init__(self):
        self._calls = 0

    async def commit(self, data):
        self._calls += 1
        # first call fails
        if self._calls == 1:
            return -1
        return 123


import pytest

# Note: This test was for GcpCloudStorageConnector.async_read_blob which no longer exists.
# The FileSystemConnector uses a different architecture with PyArrow FileSystem.
# This test is now obsolete and should be removed or rewritten for the new architecture.

@pytest.mark.skip(reason="Obsolete test - GcpCloudStorageConnector replaced by FileSystemConnector")
def test_async_read_blob_retry_on_commit_failure():
    """
    This test verified retry behavior on memory pool commit failures.
    
    The old GcpCloudStorageConnector had custom async_read_blob logic.
    The new FileSystemConnector uses PyArrow's FileSystem interface which
    has its own retry and error handling mechanisms.
    """
    assert stats.bytes_read >= len(data)
