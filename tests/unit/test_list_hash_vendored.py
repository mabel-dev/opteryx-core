"""Edge-case tests for the vendored MD5 implementation behind `vector_md5`.

Companion to test_list_hash_all.py (which does broad stdlib parity across every
algorithm). This file pins the boundary behaviours of the vendored digest code:
block-boundary lengths, non-ASCII bytes, and null/empty handling.

The kernel is in `opteryx.compiled.nanobind.vectors`, not
`opteryx.compiled.vector_ops`.
"""

import hashlib
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import draken
from opteryx.compiled.nanobind.vectors import vector_md5


def _varchar(values):
    return draken.vector_from_sequence(values, "VARCHAR")


def test_list_md5_simple():
    vals = ["hello", "world", ""]
    expected = [hashlib.md5(v.encode()).hexdigest() for v in vals]
    assert list(vector_md5(_varchar(vals))) == expected


def test_list_md5_none_rows():
    vals = [None, "123", "45.6", None]
    expected = [None if v is None else hashlib.md5(v.encode()).hexdigest() for v in vals]
    assert list(vector_md5(_varchar(vals))) == expected


def test_all_null_input():
    assert list(vector_md5(_varchar([None, None]))) == [None, None]


@pytest.mark.parametrize("length", [0, 1, 55, 56, 57, 63, 64, 65, 119, 120, 128])
def test_md5_block_boundary_lengths(length):
    """MD5 pads at the 56/64-byte boundary — walk across it."""
    value = "a" * length
    expected = hashlib.md5(value.encode()).hexdigest()
    assert list(vector_md5(_varchar([value]))) == [expected]


def test_md5_non_ascii_bytes():
    """Digests are over the stored UTF-8 bytes, not codepoints."""
    vals = ["héllo", "日本語", "🎉"]
    expected = [hashlib.md5(v.encode("utf-8")).hexdigest() for v in vals]
    assert list(vector_md5(_varchar(vals))) == expected


def test_md5_is_deterministic_across_calls():
    vals = ["hello", None, "abc"]
    assert list(vector_md5(_varchar(vals))) == list(vector_md5(_varchar(vals)))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
