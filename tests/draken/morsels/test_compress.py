#!/usr/bin/env python
"""Tests for Vector.compress default behavior and shim."""

import pytest
import pyarrow as pa
from array import array

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import opteryx.compiled.draken as draken
from opteryx.compiled.structures.relation_statistics import to_int


def _vector_compress_to_list(vector):
    """Return Python list of compressed int64 values using the pure-Python
    fallback helper to ensure deterministic behavior while extensions are
    rebuilt during development."""
    from opteryx.compiled.draken.vectors import _compress_vector
    buf = _compress_vector(vector)
    mv = memoryview(buf)
    assert mv.format == "q"
    return list(mv)


def test_compress_int64_vector():
    table = pa.table({"a": pa.array([1, 2, 3], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b"a")

    expected = [to_int(1), to_int(2), to_int(3)]
    assert _vector_compress_to_list(vec) == expected


def test_compress_float_vector():
    table = pa.table({"a": pa.array([1.0, float('nan'), float('inf')], type=pa.float64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b"a")

    expected = [to_int(1.0), to_int(float('nan')), to_int(float('inf'))]
    assert _vector_compress_to_list(vec) == expected


def test_compress_string_vector():
    table = pa.table({"a": ["", "abc", "🫖🔫"]})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b"a")

    expected = [to_int(""), to_int("abc"), to_int("🫖🔫")]
    assert _vector_compress_to_list(vec) == expected


def test_compress_date_vector():
    import datetime

    table = pa.table({"a": pa.array([datetime.date(1970, 1, 1), datetime.date(1970, 1, 2), None], type=pa.date32())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b"a")

    expected = [to_int(datetime.date(1970, 1, 1)), to_int(datetime.date(1970, 1, 2)), to_int(None)]
    assert _vector_compress_to_list(vec) == expected


def test_compress_nulls():
    table = pa.table({"a": pa.array([None, 1, None], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b"a")

    expected = [to_int(None), to_int(1), to_int(None)]
    assert _vector_compress_to_list(vec) == expected

if __name__ == "__main__":
    from tests import run_tests

    run_tests()