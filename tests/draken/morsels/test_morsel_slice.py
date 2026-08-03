#!/usr/bin/env python
"""Comprehensive tests for Morsel.slice.

Fixtures are built natively — `Morsel.from_vectors` over typed draken vector
constructors. No Arrow on the construction path.

`slice` is STRICT about bounds: `start + length` must not exceed the vector
length, and an over-slice raises `IndexError` rather than truncating. Every
engine caller clamps before calling (see opteryx/query_session.py:596,
opteryx/operators/read/read.pyx:243), so the strict contract is what the tests
pin here.
"""

import datetime
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest

import draken.draken_native as dn
from draken.morsels.morsel import Morsel


def test_slice_basic():
    """Test basic slice functionality."""
    morsel = Morsel.from_vectors(
        [b"a", b"b"],
        [
            dn.vector_from_sequence([1, 2, 3, 4, 5]),
            dn.vector_from_string_sequence([b"a", b"b", b"c", b"d", b"e"]),
        ],
    )

    sliced = morsel.slice(1, 3)
    assert (sliced.num_rows, sliced.num_columns) == (3, 2)

    assert [sliced.column(b"a")[i] for i in range(sliced.num_rows)] == [2, 3, 4]
    assert sliced.column(b"b").to_pylist() == ["b", "c", "d"]


def test_slice_offset_zero():
    """Test slicing from the beginning."""
    morsel = Morsel.from_vectors([b"x"], [dn.vector_from_sequence([10, 20, 30, 40])])

    sliced = morsel.slice(0, 2)
    assert (sliced.num_rows, sliced.num_columns) == (2, 1)
    assert sliced.column(b"x").to_pylist() == [10, 20]


def test_slice_to_end():
    """Test slicing to the end of the morsel."""
    morsel = Morsel.from_vectors([b"x"], [dn.vector_from_sequence([1, 2, 3, 4, 5])])

    sliced = morsel.slice(3, 2)
    assert (sliced.num_rows, sliced.num_columns) == (2, 1)
    assert sliced.column(b"x").to_pylist() == [4, 5]


def test_slice_single_row():
    """Test slicing a single row."""
    morsel = Morsel.from_vectors(
        [b"a", b"b"],
        [dn.vector_from_sequence([1, 2, 3]), dn.vector_from_sequence([10, 20, 30])],
    )

    sliced = morsel.slice(1, 1)
    assert (sliced.num_rows, sliced.num_columns) == (1, 2)

    assert sliced.column(b"a")[0] == 2
    assert sliced.column(b"b")[0] == 20


def test_slice_full_morsel():
    """Test slicing entire morsel."""
    morsel = Morsel.from_vectors([b"a"], [dn.vector_from_sequence([1, 2, 3])])

    sliced = morsel.slice(0, 3)
    assert (sliced.num_rows, sliced.num_columns) == (morsel.num_rows, morsel.num_columns)

    for i in range(morsel.num_rows):
        assert sliced.column(b"a")[i] == morsel.column(b"a")[i]


def test_slice_empty_result():
    """Test slicing with length 0."""
    morsel = Morsel.from_vectors([b"a"], [dn.vector_from_sequence([1, 2, 3])])

    sliced = morsel.slice(1, 0)
    assert (sliced.num_rows, sliced.num_columns) == (0, 1)


def test_slice_beyond_end_raises():
    """Over-slicing past the end fails loud — it does NOT truncate."""
    morsel = Morsel.from_vectors([b"a"], [dn.vector_from_sequence([1, 2, 3])])

    with pytest.raises(IndexError):
        morsel.slice(1, 10)


def test_slice_offset_at_end_raises():
    """A zero-width slice AT the end is legal; asking for rows past it is not."""
    morsel = Morsel.from_vectors([b"a"], [dn.vector_from_sequence([1, 2, 3])])

    assert morsel.slice(3, 0).num_rows == 0

    with pytest.raises(IndexError):
        morsel.slice(3, 5)


def test_slice_offset_beyond_end_raises():
    """Test slicing starting beyond the end."""
    morsel = Morsel.from_vectors([b"a"], [dn.vector_from_sequence([1, 2, 3])])

    with pytest.raises(IndexError):
        morsel.slice(10, 5)


def test_slice_multiple_columns():
    """Test slicing with multiple columns of different types."""
    morsel = Morsel.from_vectors(
        [b"int_col", b"float_col", b"str_col", b"bool_col"],
        [
            dn.vector_from_sequence([1, 2, 3, 4, 5]),
            dn.vector_float64_from_sequence([1.1, 2.2, 3.3, 4.4, 5.5]),
            dn.vector_from_string_sequence([b"a", b"b", b"c", b"d", b"e"]),
            dn.vector_from_bool_sequence([True, False, True, False, True]),
        ],
    )

    sliced = morsel.slice(1, 3)
    assert (sliced.num_rows, sliced.num_columns) == (3, 4)

    assert sliced.column(b"int_col").to_pylist() == [2, 3, 4]
    assert [round(v, 1) for v in sliced.column(b"float_col").to_pylist()] == [2.2, 3.3, 4.4]
    assert sliced.column(b"str_col").to_pylist() == ["b", "c", "d"]
    assert sliced.column(b"bool_col").to_pylist() == [False, True, False]


def test_slice_with_nulls():
    """Test slicing columns containing null values."""
    morsel = Morsel.from_vectors(
        [b"a", b"b"],
        [
            dn.vector_from_sequence([1, None, 3, 4, None]),
            dn.vector_from_string_sequence([b"x", None, b"z", None, b"v"]),
        ],
    )

    sliced = morsel.slice(1, 3)
    assert (sliced.num_rows, sliced.num_columns) == (3, 2)

    # Nulls are preserved, and they move with their rows.
    assert sliced.column(b"a").to_pylist() == [None, 3, 4]
    assert sliced.column(b"b").to_pylist() == [None, "z", None]


def test_slice_preserves_column_names():
    """Test that slice preserves column names."""
    morsel = Morsel.from_vectors(
        [b"foo", b"bar", b"baz"],
        [
            dn.vector_from_sequence([1, 2]),
            dn.vector_from_sequence([3, 4]),
            dn.vector_from_sequence([5, 6]),
        ],
    )

    sliced = morsel.slice(0, 1)
    assert sliced.column_names == morsel.column_names


def test_slice_preserves_column_types():
    """Test that slice preserves the physical type of every column."""
    morsel = Morsel.from_vectors(
        [b"int64", b"float64", b"string"],
        [
            dn.vector_from_sequence([1, 2, 3]),
            dn.vector_float64_from_sequence([1.0, 2.0, 3.0]),
            dn.vector_from_string_sequence([b"a", b"b", b"c"]),
        ],
    )

    sliced = morsel.slice(1, 1)

    assert sliced.column_types == morsel.column_types
    assert sliced.column_types == [dn.DrakenType.INT64, dn.DrakenType.FLOAT64, dn.DrakenType.VARCHAR]


def test_slice_large_morsel():
    """Test slicing a large morsel."""
    n = 10000
    morsel = Morsel.from_vectors(
        [b"a", b"b"],
        [
            dn.vector_from_sequence(list(range(n))),
            dn.vector_from_string_sequence([f"val_{i}".encode("utf-8") for i in range(n)]),
        ],
    )

    sliced = morsel.slice(5000, 1000)
    assert (sliced.num_rows, sliced.num_columns) == (1000, 2)

    assert sliced.column(b"a")[0] == 5000
    assert sliced.column(b"a")[999] == 5999
    assert sliced.column(b"b")[0] == "val_5000"
    assert sliced.column(b"b")[999] == "val_5999"


def test_slice_consecutive():
    """Test consecutive slicing operations."""
    morsel = Morsel.from_vectors(
        [b"a"], [dn.vector_from_sequence([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])]
    )

    sliced1 = morsel.slice(2, 6)
    assert (sliced1.num_rows, sliced1.num_columns) == (6, 1)
    assert sliced1.column(b"a")[0] == 3
    assert sliced1.column(b"a")[5] == 8

    sliced2 = sliced1.slice(1, 3)
    assert (sliced2.num_rows, sliced2.num_columns) == (3, 1)
    assert sliced2.column(b"a").to_pylist() == [4, 5, 6]


def test_slice_single_column():
    """Test slicing morsel with single column."""
    morsel = Morsel.from_vectors([b"only_col"], [dn.vector_from_sequence([100, 200, 300, 400])])

    sliced = morsel.slice(1, 2)
    assert (sliced.num_rows, sliced.num_columns) == (2, 1)
    assert sliced.column(b"only_col").to_pylist() == [200, 300]


def test_slice_with_array_type():
    """Test slicing column with array/list type."""
    morsel = Morsel.from_vectors(
        [b"lists"], [dn.vector_array_from_sequence([[1, 2], [3, 4], [5, 6], [7, 8]])]
    )

    sliced = morsel.slice(1, 2)
    assert (sliced.num_rows, sliced.num_columns) == (2, 1)
    assert sliced.column_types == [dn.DrakenType.ARRAY]

    assert list(sliced.column(b"lists")[0]) == [3, 4]
    assert list(sliced.column(b"lists")[1]) == [5, 6]


def test_slice_empty_morsel():
    """Test slicing an already empty morsel."""
    morsel = Morsel.from_vectors([b"a"], [dn.vector_from_sequence([])])
    assert (morsel.num_rows, morsel.num_columns) == (0, 1)

    sliced = morsel.slice(0, 0)
    assert (sliced.num_rows, sliced.num_columns) == (0, 1)
    assert sliced.column_names == [b"a"]

    # Asking an empty morsel for rows is an over-slice like any other.
    with pytest.raises(IndexError):
        morsel.slice(0, 5)


def test_slice_returns_new_morsel():
    """Test that slice returns a new morsel instance and leaves the source alone."""
    morsel = Morsel.from_vectors([b"a"], [dn.vector_from_sequence([1, 2, 3, 4, 5])])

    sliced = morsel.slice(1, 2)

    assert sliced is not morsel
    assert (morsel.num_rows, morsel.num_columns) == (5, 1)
    assert (sliced.num_rows, sliced.num_columns) == (2, 1)
    assert morsel.column(b"a").to_pylist() == [1, 2, 3, 4, 5]


def test_slice_negative_offset_raises():
    """A negative offset is rejected, not silently clamped."""
    morsel = Morsel.from_vectors([b"a"], [dn.vector_from_sequence([1, 2, 3])])

    with pytest.raises(IndexError):
        morsel.slice(-1, 2)


def test_slice_negative_length_raises():
    """A negative length is rejected, not silently treated as zero."""
    morsel = Morsel.from_vectors([b"a"], [dn.vector_from_sequence([1, 2, 3])])

    with pytest.raises(IndexError):
        morsel.slice(1, -2)


def test_slice_with_different_numeric_types():
    """Test slicing columns with various numeric types.

    `vector_from_sequence` cannot build UNSIGNED vectors, so the uint column
    uses the typed `vector_uint8_from_sequence` constructor directly.
    """
    morsel = Morsel.from_vectors(
        [b"int8", b"int16", b"int32", b"int64", b"uint8", b"float32", b"float64"],
        [
            dn.vector_int8_from_sequence([1, 2, 3, 4]),
            dn.vector_int16_from_sequence([10, 20, 30, 40]),
            dn.vector_int32_from_sequence([100, 200, 300, 400]),
            dn.vector_from_sequence([1000, 2000, 3000, 4000]),
            dn.vector_uint8_from_sequence([5, 6, 7, 8]),
            dn.vector_float32_from_sequence([1.5, 2.5, 3.5, 4.5]),
            dn.vector_float64_from_sequence([10.5, 20.5, 30.5, 40.5]),
        ],
    )

    sliced = morsel.slice(1, 2)
    assert (sliced.num_rows, sliced.num_columns) == (2, 7)

    # Every physical type survives the slice — that is the point of this test.
    assert sliced.column_types == [
        dn.DrakenType.INT8,
        dn.DrakenType.INT16,
        dn.DrakenType.INT32,
        dn.DrakenType.INT64,
        dn.DrakenType.UINT8,
        dn.DrakenType.FLOAT32,
        dn.DrakenType.FLOAT64,
    ]

    assert sliced.column(b"int8").to_pylist() == [2, 3]
    assert sliced.column(b"int16").to_pylist() == [20, 30]
    assert sliced.column(b"int32").to_pylist() == [200, 300]
    assert sliced.column(b"int64").to_pylist() == [2000, 3000]
    assert sliced.column(b"uint8").to_pylist() == [6, 7]
    assert [round(v, 1) for v in sliced.column(b"float32").to_pylist()] == [2.5, 3.5]
    assert sliced.column(b"float64").to_pylist() == [20.5, 30.5]


def test_slice_timestamp_column():
    """Test slicing column with timestamp type."""
    stamps = [datetime.datetime(2024, 1, day) for day in (1, 2, 3, 4)]
    vector = dn.vector_timestamp_from_sequence(stamps)
    # Readback is tz-aware UTC, so compare against the vector's own readback.
    readback = vector.to_pylist()

    morsel = Morsel.from_vectors([b"timestamps"], [vector])

    sliced = morsel.slice(1, 2)
    assert (sliced.num_rows, sliced.num_columns) == (2, 1)
    assert sliced.column_types == [dn.DrakenType.TIMESTAMP64]  # NOT downgraded to INT64

    values = sliced.column(b"timestamps").to_pylist()
    assert values == [readback[1], readback[2]]
    assert (values[0].year, values[0].month, values[0].day) == (2024, 1, 2)


def test_slice_binary_column():
    """Test slicing column with binary type."""
    morsel = Morsel.from_vectors(
        [b"binary"], [dn.vector_from_bytes_sequence([b"aaa", b"bbb", b"ccc", b"ddd"])]
    )

    sliced = morsel.slice(1, 2)
    assert (sliced.num_rows, sliced.num_columns) == (2, 1)
    assert sliced.column_types == [dn.DrakenType.VARBINARY]
    assert sliced.column(b"binary").to_pylist() == [b"bbb", b"ccc"]


def test_slice_decimal_column():
    """Test slicing column with decimal type."""
    morsel = Morsel.from_vectors(
        [b"decimal"],
        [
            dn.vector_decimal_from_sequence(
                [Decimal("1.23"), Decimal("4.56"), Decimal("7.89")], 5, 2
            )
        ],
    )

    sliced = morsel.slice(1, 1)
    assert (sliced.num_rows, sliced.num_columns) == (1, 1)
    assert sliced.column_types == [dn.DrakenType.DECIMAL]
    assert sliced.column(b"decimal").to_pylist() == [Decimal("4.56")]


def test_slice_dictionary_encoded():
    """Test slicing a dict-encoded column.

    The dict shape is a layout hint (CLAUDE.md §11): the slice may densify it,
    but the VALUES it yields must be identical either way.
    """
    vector = dn.vector_from_string_dict_sequence([b"a", b"b", b"a", b"c", b"b"])
    assert vector.is_dict
    assert vector.data_length == 3

    morsel = Morsel.from_vectors([b"dict"], [vector])

    sliced = morsel.slice(1, 3)
    assert (sliced.num_rows, sliced.num_columns) == (3, 1)
    assert sliced.column_types == [dn.DrakenType.VARCHAR]
    assert sliced.column(b"dict").to_pylist() == ["b", "a", "c"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
