import asyncio
import queue
from types import SimpleNamespace

import pytest

from opteryx.operators.iceberg_read_node import fetch_data


class NoAsyncConnector:
    pass


class GoodConnector:
    async def async_read_blob(self, *, blob_name, pool, telemetry, **kwargs):
        # Simulate committing and returning a pool reference
        await asyncio.sleep(0)
        return f"ref:{blob_name}"


@pytest.mark.asyncio
async def test_fetch_data_requires_async_read_blob():
    data_queue = queue.Queue()
    telemetry = SimpleNamespace(time_reading_blobs=0)

    await fetch_data(["blob1"], None, NoAsyncConnector(), data_queue, telemetry)

    item = data_queue.get()
    assert item[0] == "blob1"
    assert isinstance(item[1], Exception)

    sentinel = data_queue.get()
    assert sentinel is None


@pytest.mark.asyncio
async def test_fetch_data_with_async_read_blob_puts_reference_and_none():
    data_queue = queue.Queue()
    telemetry = SimpleNamespace(time_reading_blobs=0)

    await fetch_data(["blobA", "blobB"], None, GoodConnector(), data_queue, telemetry)

    seen_refs = []
    while True:
        item = data_queue.get()
        if item is None:
            break
        blob_name, reference = item
        assert reference == f"ref:{blob_name}"
        seen_refs.append(blob_name)

    assert set(seen_refs) == {"blobA", "blobB"}


@pytest.mark.asyncio
async def test_fetch_data_uses_protocol_reader_when_connector_has_no_async():
    data_queue = queue.Queue()
    telemetry = SimpleNamespace(time_reading_blobs=0)

    class NoAsyncConnector:
        pass

    class ProtoReader:
        async def async_read_blob(self, *, blob_name, pool, telemetry, **kwargs):
            await asyncio.sleep(0)
            return 123

    from opteryx.operators import iceberg_read_node as irn

    old = irn.PROTOCOLS.get("x")
    irn.PROTOCOLS["x"] = ProtoReader
    try:
        await fetch_data(["x://file"], None, NoAsyncConnector(), data_queue, telemetry)
        item = data_queue.get()
        assert item[1] == 123
        assert data_queue.get() is None
    finally:
        if old is None:
            del irn.PROTOCOLS["x"]
        else:
            irn.PROTOCOLS["x"] = old


@pytest.mark.asyncio
async def test_fetch_data_prefers_connector_reader():
    data_queue = queue.Queue()
    telemetry = SimpleNamespace(time_reading_blobs=0)

    class ProtoReader:
        async def async_read_blob(self, *, blob_name, pool, telemetry, **kwargs):
            await asyncio.sleep(0)
            return 999

    from opteryx.operators import iceberg_read_node as irn

    old = irn.PROTOCOLS.get("g")
    irn.PROTOCOLS["g"] = ProtoReader
    try:
        await fetch_data(["g://file"], None, GoodConnector(), data_queue, telemetry)
        item = data_queue.get()
        assert item[1] == f"ref:{item[0]}"
        assert data_queue.get() is None
    finally:
        if old is None:
            del irn.PROTOCOLS["g"]
        else:
            irn.PROTOCOLS["g"] = old
