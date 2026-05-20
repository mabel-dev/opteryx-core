"""Verify VectorVector.unified() satisfies the unified-format invariant."""

import pyarrow as pa

from draken import Vector
from draken.vectors.vector_vector import VectorVector


def test_unified_dense_no_nulls():
    arr = pa.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]],
                   type=pa.list_(pa.float16(), 3))
    vec = Vector.from_arrow(arr)
    assert isinstance(vec, VectorVector)

    data_ok, sel, length, data_length, validity_ok = vec._unified_fields_for_test()

    assert data_ok, "data must be non-NULL"
    assert sel is not None, "selection must be non-NULL (dense identity)"
    assert length == 3
    assert data_length == 3
    assert sel == [0, 1, 2]
    assert not validity_ok, "validity must be NULL when all rows are present"


def test_unified_with_null_row():
    arr = pa.array([[1.0, 0.0], None, [0.0, 1.0]],
                   type=pa.list_(pa.float16(), 2))
    vec = Vector.from_arrow(arr)
    assert isinstance(vec, VectorVector)

    data_ok, sel, length, data_length, validity_ok = vec._unified_fields_for_test()

    assert data_ok, "data must be non-NULL even when some rows are null"
    assert sel is not None
    assert length == 3
    assert data_length == 3
    assert sel == [0, 1, 2]
    assert validity_ok, "validity must be non-NULL when at least one row is null"

    # Confirm the null row is correctly marked (bit 1 should be 0).
    assert vec.is_null_at(0) is False
    assert vec.is_null_at(1) is True
    assert vec.is_null_at(2) is False


def test_unified_single_row():
    arr = pa.array([[0.5, 0.5]], type=pa.list_(pa.float16(), 2))
    vec = Vector.from_arrow(arr)
    assert isinstance(vec, VectorVector)

    data_ok, sel, length, data_length, validity_ok = vec._unified_fields_for_test()

    assert data_ok
    assert sel == [0]
    assert length == 1
    assert data_length == 1
    assert not validity_ok


if __name__ == "__main__":
    test_unified_dense_no_nulls()
    test_unified_with_null_row()
    test_unified_single_row()
    print("ok")
