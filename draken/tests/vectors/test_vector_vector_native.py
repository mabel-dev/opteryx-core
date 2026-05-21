"""Native (no-PyArrow) construction tests for VectorVector (fp16 embeddings).

Builds via vector_from_sequence(..., dtype=VECTOR) which routes to the native
from_float_pylist builder.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from draken.interop.vector_sequence import vector_from_sequence
from opteryx.types import OrsoTypes


def _build(rows):
    return vector_from_sequence(rows, dtype=OrsoTypes.VECTOR)


def test_native_vector_roundtrip():
    rows = [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.5, -1.0, 0.25]]
    v = _build(rows)
    assert type(v).__name__ == "VectorVector"
    assert len(v) == 3
    # fp16 is exact for these values
    assert v.to_pylist() == rows
    assert v[0] == [1.0, 2.0, 3.0]


def test_native_vector_null_row_preserved():
    v = _build([[1.0, 2.0], None, [3.0, 4.0]])
    assert len(v) == 3
    assert v.to_pylist() == [[1.0, 2.0], None, [3.0, 4.0]]
    assert v[1] is None


def test_native_vector_ragged_rejected():
    with pytest.raises(ValueError):
        _build([[1.0, 2.0], [3.0]])


def test_native_vector_element_null_rejected():
    with pytest.raises(ValueError):
        _build([[1.0, None]])


def test_native_vector_all_none_rejected():
    with pytest.raises(ValueError):
        _build([None, None])


if __name__ == "__main__":
    test_native_vector_roundtrip()
    test_native_vector_null_row_preserved()
    test_native_vector_ragged_rejected()
    test_native_vector_element_null_rejected()
    test_native_vector_all_none_rejected()
    print("✅ okay")
