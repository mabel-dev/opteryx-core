import numpy as np
import pyarrow as pa
import pytest

from opteryx.compiled.joins import (
    build_filtered_rows_indices_and_column,
    build_rows_indices_and_column,
)


def test_build_rows_indices_and_column_returns_draken_vector():
    """
    Test that the Arrow-native path in cross_join.pyx returns Draken Int64Vector
    instead of NumPy arrays for indices.
    """
    # Create a List<String> array
    data = [["a", "b"], ["c"], [], ["d", "e", "f"]]
    arr = pa.array(data, type=pa.list_(pa.string()))

    indices, flat_data = build_rows_indices_and_column(arr)

    # Check that indices is a Draken Int64Vector (or at least not a numpy array if wrapped)
    # The return type from int64_from_sequence is Int64Vector
    assert not isinstance(indices, np.ndarray), "Indices should be a Draken Vector, not NumPy array"
    assert hasattr(indices, "to_pylist"), "Indices should be a Draken Vector"

    expected_indices = [0, 0, 1, 3, 3, 3]
    assert indices.to_pylist() == expected_indices
    assert list(flat_data) == ["a", "b", "c", "d", "e", "f"]


def test_build_filtered_rows_indices_and_column_returns_draken_vector():
    """
    Test that the filtered Arrow-native path returns Draken Int64Vector.
    """
    data = [["apple", "banana"], ["cherry"], ["apple", "date"]]
    arr = pa.array(data, type=pa.list_(pa.string()))
    valid_values = {"apple", "date"}

    indices, flat_data = build_filtered_rows_indices_and_column(arr, valid_values)

    assert not isinstance(indices, np.ndarray), "Indices should be a Draken Vector"

    print(f"Indices: {indices.to_pylist()}")
    print(f"Flat Data: {flat_data}")

    # "apple" at row 0, "apple" at row 2, "date" at row 2
    expected_indices = [0, 2, 2]
    assert indices.to_pylist() == expected_indices
    # Note: build_filtered_rows_indices_and_column returns bytes in current implementation
    assert list(flat_data) == [b"apple", b"apple", b"date"]


if __name__ == "__main__":
    test_build_rows_indices_and_column_returns_draken_vector()
    test_build_filtered_rows_indices_and_column_returns_draken_vector()
    print("Cross-join index tests passed!")
