import pytest

from opteryx.compiled.structures.memory_view_stream import (
    MemoryViewStream,
    MemoryViewStreamOptimized,
)


def test_read_accepts_none_and_returns_all_bytes():
    data = b"hello world"
    mv = memoryview(data)
    s = MemoryViewStream(mv)

    # read(None) should behave like read(-1)
    assert s.read(None) == data
    # subsequent read should return empty
    assert s.read(None) == b""


def test_read_memoryview_accepts_none_and_returns_full_memoryview():
    data = b"abc123"
    mv = memoryview(data)
    s = MemoryViewStreamOptimized(mv)

    # read_memoryview(None) should return a memoryview with full contents
    mem = s.read_memoryview(None)
    assert bytes(mem) == data
    # ensure the stream advanced
    assert s.read(None) == b""
