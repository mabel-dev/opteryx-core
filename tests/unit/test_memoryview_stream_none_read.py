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


def test_read_with_a_count_is_unchanged():
    s = MemoryViewStream(memoryview(b"abcdefgh"))
    assert s.read(3) == b"abc"
    assert s.read(0) == b""
    assert s.read(2) == b"de"
    assert s.read1(10) == b"fgh"
    assert s.read(1) == b""


def test_a_negative_count_and_none_both_read_everything_left():
    for everything in (-1, -5, None):
        s = MemoryViewStream(memoryview(b"abcdef"))
        assert s.read(2) == b"ab"
        assert s.read(everything) == b"cdef"
        s = MemoryViewStreamOptimized(memoryview(b"abcdef"))
        assert bytes(s.read_memoryview(everything)) == b"abcdef"


def test_read1_accepts_none():
    assert MemoryViewStream(memoryview(b"xyz")).read1(None) == b"xyz"


def test_a_count_that_is_not_an_integer_is_refused():
    s = MemoryViewStream(memoryview(b"abc"))
    with pytest.raises(TypeError):
        s.read("2")
    with pytest.raises(TypeError):
        MemoryViewStreamOptimized(memoryview(b"abc")).read_memoryview("2")


def test_a_closed_stream_still_refuses_a_read_of_none():
    s = MemoryViewStream(memoryview(b"abc"))
    s.close()
    with pytest.raises(ValueError):
        s.read(None)
