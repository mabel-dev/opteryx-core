import sys

import pytest

from opteryx.connectors.io_systems.s3_filesystem import OpteryxS3FileSystem


class InvalidResponseError(Exception):
    pass


@pytest.mark.asyncio
async def test_async_read_blob_falls_back_on_select(monkeypatch):
    # Patch minio module
    class _FakeMinioModule:
        class Minio:
            def __init__(self, *args, **kwargs):
                pass

    sys.modules["minio"] = _FakeMinioModule()

    fs = OpteryxS3FileSystem(S3_END_POINT="dummy", S3_ACCESS_KEY="a", S3_SECRET_KEY="b", S3_SECURE=False)

    # Replace module-level _read_from_s3 to raise first then succeed
    import opteryx.connectors.io_systems.s3_filesystem as s3mod

    calls = {"n": 0}

    def fake_read_from_s3(client, bucket, object_name, columns, filters):
        calls["n"] += 1
        if calls["n"] == 1:
            raise InvalidResponseError("select failed")
        return b"full-object-bytes"

    monkeypatch.setattr(s3mod, "_read_from_s3", fake_read_from_s3)

    class FakePool:
        async def commit(self, data):
            return 88

    pool = FakePool()
    telemetry = type("t", (), {"bytes_read": 0, "stalls_io_waiting_on_engine": 0, "cpu_wait_seconds": 0})()

    # Should succeed and return a tuple (ref, filters_applied) where filters_applied False
    res = await fs.async_read_blob(blob_name="s3://mybucket/file.parquet", pool=pool, session=None, telemetry=telemetry, columns=None, filters=[("a", "=", 1)])

    assert isinstance(res, tuple)
    ref, filters_applied = res
    assert ref == 88
    assert filters_applied is False
