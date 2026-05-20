"""Verify ArrayVector.unified() populates `selection` per the unified format invariant."""

import pyarrow as pa

from draken import Vector
from draken.vectors.array_vector import ArrayVector


def test_unified_selection_is_identity_dense():
    arr = pa.array([[1, 2], [3], [], [4, 5, 6]], type=pa.list_(pa.int64()))
    vec = Vector.from_arrow(arr)
    assert isinstance(vec, ArrayVector)

    sel = vec._unified_selection_for_test()
    assert sel is not None, "selection must not be NULL for a dense ArrayVector"
    assert len(sel) == len(vec) == 4
    assert sel == [0, 1, 2, 3]


def test_unified_selection_with_nulls():
    arr = pa.array([[1], None, [2, 3], None, [4]], type=pa.list_(pa.int64()))
    vec = Vector.from_arrow(arr)
    assert isinstance(vec, ArrayVector)

    sel = vec._unified_selection_for_test()
    assert sel is not None
    assert sel == [0, 1, 2, 3, 4]


if __name__ == "__main__":
    test_unified_selection_is_identity_dense()
    test_unified_selection_with_nulls()
    print("ok")
