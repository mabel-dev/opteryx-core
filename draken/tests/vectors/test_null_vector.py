import pytest
from draken.vectors.null_vector import NullVector


def test_unified_view_constant_shape():
    vec = NullVector(7)
    data_length, length, sel_non_null, val_non_null, sel0, val0 = vec._unified_view_fields()

    assert data_length == 1, f"expected data_length=1, got {data_length}"
    assert length == 7, f"expected length=7, got {length}"
    assert sel_non_null, "selection must be non-NULL"
    assert val_non_null, "validity must be non-NULL"
    assert sel0 == 0, f"selection[0] must be 0 (maps to slot 0), got {sel0}"
    assert val0 == 0, f"validity[0] must be 0 (all-null byte), got {val0}"


def test_unified_view_zero_length():
    vec = NullVector(0)
    data_length, length, sel_non_null, val_non_null, sel0, val0 = vec._unified_view_fields()

    assert data_length == 1
    assert length == 0
    assert sel_non_null
    assert val_non_null
