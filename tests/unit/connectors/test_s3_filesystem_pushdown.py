import asyncio

import pytest

from opteryx.connectors.io_systems.s3_filesystem import OpteryxS3FileSystem, S3File


class FakeStream:
    def __init__(self, data: bytes):
        self._data = data

    def read(self):
        return self._data

    def close(self):
        return None


class FakeMinioClient:
    def __init__(self, data: bytes):
        self._data = data

    def select_object_content(self, **kwargs):
        return FakeStream(self._data)

    def get_object(self, **kwargs):
        return FakeStream(self._data)


def test_s3file_sets_filters_applied_and_memoryview():
    fs = FakeMinioClient(b"hello world")
    # Ensure adapters imports don't blow up in test env - provide a minimal stub
    import sys
    import types

    adapters = types.ModuleType("adapters")
    adapters.minio = types.ModuleType("adapters.minio")
    mod = types.ModuleType("adapters.minio.parquet_output_serialization")

    class ParquetOutputSerialization:
        def __init__(self, *args, **kwargs):
            pass

    mod.ParquetOutputSerialization = ParquetOutputSerialization
    sys.modules["adapters"] = adapters
    sys.modules["adapters.minio"] = adapters.minio
    sys.modules["adapters.minio.parquet_output_serialization"] = mod

    # Avoid going through PredicatePushable.to_dnf machinery in this unit test
    s3mod = __import__("opteryx.connectors.io_systems.s3_filesystem", fromlist=["*"])
    s3mod._build_select_query = lambda columns, filters: "SELECT * FROM s3object"

    # Pass a simple truthy filters value
    f = S3File("s3://bucket/path/file.parquet", fs, columns=None, filters=[("a", "=", 1)])

    assert hasattr(f, "memoryview")
    assert f.memoryview.tobytes() == b"hello world"
    assert getattr(f, "filters_applied", False) is True


@pytest.mark.asyncio
async def test_opteryx_s3_async_read_blob_returns_tuple(monkeypatch):
    # patch a fake minio module so OpteryxS3FileSystem can be instantiated without network deps
    import sys

    class _FakeMinioModule:
        class Minio:
            def __init__(self, *args, **kwargs):
                pass

    sys.modules["minio"] = _FakeMinioModule()

    fs = OpteryxS3FileSystem(
        S3_END_POINT="dummy", S3_ACCESS_KEY="a", S3_SECRET_KEY="b", S3_SECURE=False
    )

    # replace event loop to return bytes quickly from "_read_from_s3"
    class FakeLoop:
        async def run_in_executor(self, *args, **kwargs):
            return b"data"

        def is_closed(self):
            return True

        def close(self):
            return None

    monkeypatch.setattr(asyncio, "get_event_loop", lambda: FakeLoop())

    # create a fake pool with async commit
    class FakePool:
        def __init__(self, to_return):
            self.to_return = to_return

        async def commit(self, data):
            return self.to_return

    pool = FakePool(123)
    telemetry = type(
        "t", (), {"bytes_read": 0, "stalls_io_waiting_on_engine": 0, "cpu_wait_seconds": 0}
    )()

    # Avoid invoking rugo/parquet C code on invalid bytes; stub the metadata reader
    import opteryx.compiled.rugo.parquet as parquet_meta

    monkeypatch.setattr(
        parquet_meta, "read_metadata_from_memoryview", lambda mv, **kwargs: {"num_rows": 1}
    )

    # Call with filters and expect a tuple return
    res = await fs.async_read_blob(
        blob_name="s3://mybucket/file.parquet",
        pool=pool,
        session=None,
        telemetry=telemetry,
        columns=None,
        filters=[("a", "=", 1)],
    )

    assert isinstance(res, tuple) and len(res) == 2
    ref, filters_applied = res
    assert ref == 123
    assert filters_applied is True
