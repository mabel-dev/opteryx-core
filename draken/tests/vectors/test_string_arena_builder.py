"""Smoke tests for StringArenaBuilder and StringArenaHandle.

Covers:
- Inline encoding (len <= 12) — bytes live inside the slot, arena untouched.
- Extern encoding (len > 12)  — bytes live in the arena, slot holds prefix + offset.
- Boundary lengths (0, 1, 4, 11, 12, 13).
- Null rows + validity bitmap.
- Round-trip via StringArenaHandle.to_pylist.
- Equality (str_equals) on slot pairs within the same arena.

These tests exercise the storage layer in isolation; they do not wire the
arena into StringVector or any kernel.
"""
import pytest

from draken.vectors.string_arena_builder import StringArenaBuilder


def _build(values, *, estimate_avg_bytes=8):
    """Convenience: build a resizable arena from a list of bytes | None."""
    b = StringArenaBuilder.with_estimate(len(values), estimate_avg_bytes)
    for v in values:
        if v is None:
            b.append_null()
        else:
            b.append(v)
    return b.finish_handle()


def test_empty_builder():
    h = _build([])
    assert h.length() == 0
    assert h.arena_used() == 0
    assert h.to_pylist() == []


def test_all_inline_short():
    values = [b"", b"a", b"abc", b"abcd", b"hello"]
    h = _build(values)
    assert h.length() == len(values)
    assert h.arena_used() == 0  # all inline, arena untouched
    for i, v in enumerate(values):
        assert h.is_inline(i)
        assert not h.is_null(i)
        assert h.slot_length(i) == len(v)
        assert h.slot_bytes(i) == v
    assert h.to_pylist() == values


def test_inline_boundary_12_bytes():
    """Length 12 is the largest inline form."""
    val = b"abcdefghijkl"  # exactly 12 bytes
    assert len(val) == 12
    h = _build([val])
    assert h.is_inline(0)
    assert h.arena_used() == 0
    assert h.slot_bytes(0) == val


def test_extern_boundary_13_bytes():
    """Length 13 is the smallest extern form."""
    val = b"abcdefghijklm"  # 13 bytes
    assert len(val) == 13
    h = _build([val])
    assert not h.is_inline(0)
    assert h.arena_used() == 13
    assert h.slot_bytes(0) == val


def test_extern_long_string():
    val = b"the quick brown fox jumps over the lazy dog"
    assert len(val) > 12
    h = _build([val])
    assert not h.is_inline(0)
    assert h.arena_used() == len(val)
    assert h.slot_bytes(0) == val


def test_mixed_inline_and_extern():
    values = [
        b"",
        b"short",
        b"this one is definitely long enough",
        b"abcdefghijkl",       # exactly 12 — inline
        b"abcdefghijklm",      # 13 — extern
        b"another reasonably long string that exceeds inline storage",
    ]
    h = _build(values)
    assert h.to_pylist() == values
    # Arena holds only the long values
    expected_arena = sum(len(v) for v in values if len(v) > 12)
    assert h.arena_used() == expected_arena
    for i, v in enumerate(values):
        assert h.is_inline(i) == (len(v) <= 12)


def test_null_rows():
    values = [b"alpha", None, b"beta", None, None, b"gamma"]
    h = _build(values)
    assert h.length() == len(values)
    for i, v in enumerate(values):
        assert h.is_null(i) == (v is None)
    assert h.to_pylist() == values


def test_all_null():
    h = _build([None, None, None])
    assert all(h.is_null(i) for i in range(3))
    assert h.to_pylist() == [None, None, None]


def test_equals_inline_inline():
    """str_equals on two inline slots holding the same short bytes."""
    h = _build([b"hello", b"hello", b"world"])
    assert h.slots_equal(0, 1)
    assert not h.slots_equal(0, 2)


def test_equals_extern_extern():
    """str_equals on two extern slots holding the same long bytes."""
    long_val = b"this string definitely exceeds twelve bytes"
    h = _build([long_val, long_val, long_val + b"!"])
    assert h.slots_equal(0, 1)
    assert not h.slots_equal(0, 2)


def test_equals_short_zero_padding():
    """Strings shorter than 4 bytes must compare equal byte-for-byte even
    though the slot has trailing prefix bytes — the builder zero-pads them
    deterministically."""
    h = _build([b"a", b"a", b"b", b"", b""])
    assert h.slots_equal(0, 1)      # "a" == "a"
    assert not h.slots_equal(0, 2)  # "a" != "b"
    assert h.slots_equal(3, 4)      # "" == ""
    assert not h.slots_equal(0, 3)  # "a" != ""


def test_strict_capacity_exact():
    """with_counts requires the arena to be fully consumed at finish()."""
    long_val = b"abcdefghijklmnop"  # 16 bytes — extern
    b = StringArenaBuilder.with_counts(2, len(long_val) * 2)
    b.append(long_val)
    b.append(long_val)
    h = b.finish_handle()
    assert h.arena_used() == len(long_val) * 2


def test_strict_capacity_underconsumed_raises():
    b = StringArenaBuilder.with_counts(1, 100)
    b.append(b"too short to fill arena, this is actually long")
    with pytest.raises(ValueError, match="arena bytes"):
        b.finish_handle()


def test_strict_capacity_overflow_raises():
    """Non-resizable builder refuses to grow."""
    b = StringArenaBuilder(2, 16)  # not resizable, not strict
    b.append(b"abcdefghijklmnop")  # 16 bytes — fills exact capacity
    with pytest.raises(ValueError, match="out of capacity"):
        b.append(b"abcdefghijklmnop")  # would need another 16


def test_incomplete_builder_raises():
    b = StringArenaBuilder.with_estimate(3, 8)
    b.append(b"only one")
    with pytest.raises(ValueError, match="builder incomplete"):
        b.finish_handle()


def test_finish_called_twice_raises():
    b = StringArenaBuilder.with_estimate(1, 8)
    b.append(b"hello")
    b.finish_handle()
    with pytest.raises(RuntimeError, match="finish called twice"):
        b.finish_handle()


def test_resizable_grows_arena():
    """with_estimate produces a resizable builder; long values trigger growth."""
    long_chunks = [b"x" * 100 for _ in range(10)]
    h = _build(long_chunks, estimate_avg_bytes=1)  # underestimate forces resize
    assert h.arena_used() == 1000
    assert h.to_pylist() == long_chunks
