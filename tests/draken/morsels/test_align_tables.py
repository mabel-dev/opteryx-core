"""
Tests for the align_tables Cython implementation.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

import numpy as np
import pyarrow as pa
from draken import Morsel
from draken import align_tables
from opteryx.operators.group_state_store import DRAKEN_ENCODING_DICTIONARY


def test_basic_functionality():
    """Test basic alignment functionality."""
    source_table = pa.table({
        "a": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "b": pa.array([10.0, 20.0, 30.0, 40.0, 50.0]),
    })

    append_table = pa.table({
        "c": pa.array([100, 200, 300, 400, 500], type=pa.int64()),
        "d": pa.array([1.1, 2.2, 3.3, 4.4, 5.5]),
    })

    source_morsel = Morsel.from_arrow(source_table)
    append_morsel = Morsel.from_arrow(append_table)

    source_indices = np.array([0, 2, 4], dtype=np.int32)
    append_indices = np.array([1, 3, 4], dtype=np.int32)

    result = align_tables(source_morsel, append_morsel, source_indices, append_indices)
    result_arrow = result.to_arrow()

    assert result.num_rows == 3
    assert result.num_columns == 4
    assert set(result.column_names) == {b"a", b"b", b"c", b"d"}
    assert result_arrow["a"].to_pylist() == [1, 3, 5]
    assert result_arrow["b"].to_pylist() == [10.0, 30.0, 50.0]
    assert result_arrow["c"].to_pylist() == [200, 400, 500]
    assert result_arrow["d"].to_pylist() == [2.2, 4.4, 5.5]


def test_empty_indices():
    """Test with empty index arrays."""
    source_table = pa.table({"a": pa.array([1, 2, 3], type=pa.int64())})
    append_table = pa.table({"b": pa.array([4, 5, 6], type=pa.int64())})

    source_morsel = Morsel.from_arrow(source_table)
    append_morsel = Morsel.from_arrow(append_table)

    source_indices = np.array([], dtype=np.int32)
    append_indices = np.array([], dtype=np.int32)

    result = align_tables(source_morsel, append_morsel, source_indices, append_indices)

    assert result.num_rows == 0
    assert result.num_columns == 2


def test_duplicate_columns():
    """Test that duplicate column names are handled correctly."""
    source_table = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "value": pa.array([10.0, 20.0, 30.0]),
    })

    append_table = pa.table({
        "id": pa.array([4, 5, 6], type=pa.int64()),  # Duplicate column name
        "extra": pa.array([100, 200, 300], type=pa.int64()),
    })

    source_morsel = Morsel.from_arrow(source_table)
    append_morsel = Morsel.from_arrow(append_table)

    source_indices = np.array([0, 1, 2], dtype=np.int32)
    append_indices = np.array([0, 1, 2], dtype=np.int32)

    result = align_tables(source_morsel, append_morsel, source_indices, append_indices)

    # 'id' from source kept; 'id' from append dropped; 'value' and 'extra' kept
    assert result.num_columns == 3
    assert set(result.column_names) == {b"id", b"value", b"extra"}


def test_align_tables_preserves_null_markers():
    """-1 indices should produce null-padded output across vector types."""
    source_table = pa.table({
        "flag": pa.array([True, None, False], type=pa.bool_()),
        "value": pa.array([b"a", None, b"c"]),
    })
    append_table = pa.table({
        "rhs": pa.array([10, 20, None], type=pa.int64()),
    })

    source_morsel = Morsel.from_arrow(source_table)
    append_morsel = Morsel.from_arrow(append_table)

    source_indices = np.array([0, -1, 1, 2], dtype=np.int32)
    append_indices = np.array([2, 1, -1, 0], dtype=np.int32)

    result = align_tables(source_morsel, append_morsel, source_indices, append_indices)
    result_arrow = result.to_arrow()

    assert result_arrow["flag"].to_pylist() == [True, None, None, False]
    assert result_arrow["value"].to_pylist() == [b"a", None, None, b"c"]
    assert result_arrow["rhs"].to_pylist() == [None, 20, None, 10]


def test_align_tables_preserves_dictionary_columns_with_null_padding():
    source_table = pa.table(
        {
            "k": pa.DictionaryArray.from_arrays(
                pa.array([0, 1, 2], type=pa.int8()),
                pa.array([b"one", b"two", b"three"]),
            )
        }
    )
    append_table = pa.table({"rhs": pa.array([10, 20, 30], type=pa.int64())})

    source_morsel = Morsel.from_arrow(source_table)
    append_morsel = Morsel.from_arrow(append_table)

    source_indices = np.array([0, -1, 2], dtype=np.int32)
    append_indices = np.array([2, 1, -1], dtype=np.int32)

    result = align_tables(source_morsel, append_morsel, source_indices, append_indices)

    assert result.column(b"k").encoding == DRAKEN_ENCODING_DICTIONARY
    assert result.to_arrow()["k"].to_pylist() == [b"one", None, b"three"]
    assert result.to_arrow()["rhs"].to_pylist() == [30, 20, None]
