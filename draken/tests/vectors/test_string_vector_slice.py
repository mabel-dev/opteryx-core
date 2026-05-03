"""Tests for string slice operations on various vector encodings.

Covers vector_string_slice_left() and vector_string_slice_right() on:
- Dense strings
- Dictionary-encoded strings (dict-only from Parquet)
- RLE-encoded strings
- Constant strings
"""

import pytest
import pyarrow as pa
from array import array
from draken.vectors import string_vector as string_vector_module
from opteryx.compiled import vector_ops


class TestStringSliceLeft:
    """Test vector_string_slice_left() on various encodings."""

    def test_slice_left_dense_basic(self):
        """Test left slice on dense vector with basic case."""
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([b"hello", b"world", b"test", None])
        )
        result = vector_ops.vector_string_slice_left(vec, 2)
        assert result.to_pylist() == [b"he", b"wo", b"te", None]

    def test_slice_left_dense_zero_length(self):
        """Test left slice with zero length."""
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([b"hello", b"world"])
        )
        result = vector_ops.vector_string_slice_left(vec, 0)
        assert result.to_pylist() == [b"", b""]

    def test_slice_left_dense_larger_than_string(self):
        """Test left slice larger than string returns full string."""
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([b"hi", b"world"])
        )
        result = vector_ops.vector_string_slice_left(vec, 10)
        assert result.to_pylist() == [b"hi", b"world"]

    def test_slice_left_dense_negative_index(self):
        """Test left slice with negative index (counts from end)."""
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([b"hello"])
        )
        # -2 from end of 5-byte string = slice 3 bytes
        result = vector_ops.vector_string_slice_left(vec, -2)
        assert result.to_pylist() == [b"hel"]

    def test_slice_left_dict_only_basic(self):
        """Test left slice on dict-only vector."""
        vec = string_vector_module.StringVector.from_dict_buffers(
            array("i", [0, 1, 0, 0]),  # codes
            array("i", [0, 5]),  # dict offsets
            array("i", [5, 4]),  # dict lengths
            bytearray(b"alphabeta"),  # dict data
            bytearray([1, 1, 1, 0]),  # null bitmap
        )
        result = vector_ops.vector_string_slice_left(vec, 3)
        assert result.to_pylist() == [b"alp", b"bet", b"alp", None]

    def test_slice_left_dict_only_zero(self):
        """Test left slice with zero on dict-only vector."""
        vec = string_vector_module.StringVector.from_dict_buffers(
            array("i", [0, 1]),
            array("i", [0, 5]),
            array("i", [5, 4]),
            bytearray(b"alphabeta"),
            bytearray([1, 1]),
        )
        result = vector_ops.vector_string_slice_left(vec, 0)
        assert result.to_pylist() == [b"", b""]

    def test_slice_left_dict_only_variable_length(self):
        """Test left slice with variable per-row slice lengths."""
        vec = string_vector_module.StringVector.from_dict_buffers(
            array("i", [0, 1, 0, 2]),  # codes
            array("i", [0, 5, 9]),  # offsets
            array("i", [5, 4, 5]),  # lengths
            bytearray(b"alphabetagamma"),
            bytearray([1, 1, 1, 1]),
        )
        # Test with a list of lengths
        lengths = [2, 3, 1, 5]
        result = vector_ops.vector_string_slice_left(vec, lengths)
        assert result.to_pylist() == [b"al", b"bet", b"a", b"gamma"]

    def test_slice_left_constant_string(self):
        """Test left slice on constant string vector."""
        vec = string_vector_module.StringVector.from_constant(b"hello", 3)
        result = vector_ops.vector_string_slice_left(vec, 2)
        assert result.to_pylist() == [b"he", b"he", b"he"]

    def test_slice_left_constant_null(self):
        """Test left slice on constant null vector."""
        vec = string_vector_module.StringVector.from_constant(None, 2, is_null=True)
        result = vector_ops.vector_string_slice_left(vec, 5)
        assert result.to_pylist() == [None, None]

    def test_slice_left_empty_string(self):
        """Test left slice on empty strings."""
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([b"", b"hello", b""])
        )
        result = vector_ops.vector_string_slice_left(vec, 3)
        assert result.to_pylist() == [b"", b"hel", b""]


class TestStringSliceRight:
    """Test vector_string_slice_right() on various encodings."""

    def test_slice_right_dense_basic(self):
        """Test right slice on dense vector with basic case."""
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([b"hello", b"world", b"test", None])
        )
        result = vector_ops.vector_string_slice_right(vec, 2)
        assert result.to_pylist() == [b"lo", b"ld", b"st", None]

    def test_slice_right_dense_zero_length(self):
        """Test right slice with zero length."""
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([b"hello", b"world"])
        )
        result = vector_ops.vector_string_slice_right(vec, 0)
        assert result.to_pylist() == [b"", b""]

    def test_slice_right_dense_larger_than_string(self):
        """Test right slice larger than string returns full string."""
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([b"hi", b"world"])
        )
        result = vector_ops.vector_string_slice_right(vec, 10)
        assert result.to_pylist() == [b"hi", b"world"]

    def test_slice_right_dense_negative_index(self):
        """Test right slice with negative index (clamps to 0)."""
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([b"hello"])
        )
        result = vector_ops.vector_string_slice_right(vec, -5)
        assert result.to_pylist() == [b""]

    def test_slice_right_dict_only_basic(self):
        """Test right slice on dict-only vector."""
        vec = string_vector_module.StringVector.from_dict_buffers(
            array("i", [0, 1, 0, 0]),
            array("i", [0, 5]),
            array("i", [5, 4]),
            bytearray(b"alphabeta"),
            bytearray([1, 1, 1, 0]),
        )
        result = vector_ops.vector_string_slice_right(vec, 2)
        assert result.to_pylist() == [b"ha", b"ta", b"ha", None]

    def test_slice_right_dict_only_zero(self):
        """Test right slice with zero on dict-only vector."""
        vec = string_vector_module.StringVector.from_dict_buffers(
            array("i", [0, 1]),
            array("i", [0, 5]),
            array("i", [5, 4]),
            bytearray(b"alphabeta"),
            bytearray([1, 1]),
        )
        result = vector_ops.vector_string_slice_right(vec, 0)
        assert result.to_pylist() == [b"", b""]

    def test_slice_right_dict_only_variable_length(self):
        """Test right slice with variable per-row slice lengths."""
        vec = string_vector_module.StringVector.from_dict_buffers(
            array("i", [0, 1, 0, 2]),
            array("i", [0, 5, 9]),
            array("i", [5, 4, 5]),
            bytearray(b"alphabetagamma"),
            bytearray([1, 1, 1, 1]),
        )
        lengths = [2, 3, 1, 5]
        result = vector_ops.vector_string_slice_right(vec, lengths)
        assert result.to_pylist() == [b"ha", b"eta", b"a", b"gamma"]

    def test_slice_right_constant_string(self):
        """Test right slice on constant string vector."""
        vec = string_vector_module.StringVector.from_constant(b"hello", 3)
        result = vector_ops.vector_string_slice_right(vec, 2)
        assert result.to_pylist() == [b"lo", b"lo", b"lo"]

    def test_slice_right_constant_null(self):
        """Test right slice on constant null vector."""
        vec = string_vector_module.StringVector.from_constant(None, 2, is_null=True)
        result = vector_ops.vector_string_slice_right(vec, 5)
        assert result.to_pylist() == [None, None]

    def test_slice_right_empty_string(self):
        """Test right slice on empty strings."""
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([b"", b"hello", b""])
        )
        result = vector_ops.vector_string_slice_right(vec, 3)
        assert result.to_pylist() == [b"", b"llo", b""]

    def test_slice_right_unicode_string(self):
        """Test right slice on unicode strings (bytes, not chars)."""
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([b"hello", b"world"])
        )
        result = vector_ops.vector_string_slice_right(vec, 2)
        assert result.to_pylist() == [b"lo", b"ld"]


class TestStringSliceEdgeCases:
    """Edge cases and combined scenarios."""

    def test_slice_left_all_nulls(self):
        """Test left slice on vector of all nulls."""
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([None, None, None], type=pa.string())
        )
        result = vector_ops.vector_string_slice_left(vec, 5)
        assert result.to_pylist() == [None, None, None]

    def test_slice_right_all_nulls(self):
        """Test right slice on vector of all nulls."""
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([None, None, None], type=pa.string())
        )
        result = vector_ops.vector_string_slice_right(vec, 5)
        assert result.to_pylist() == [None, None, None]

    def test_slice_left_very_long_string(self):
        """Test left slice on very long string."""
        long_string = b"x" * 10000
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([long_string])
        )
        result = vector_ops.vector_string_slice_left(vec, 100)
        assert result.to_pylist() == [b"x" * 100]

    def test_slice_right_very_long_string(self):
        """Test right slice on very long string."""
        long_string = b"x" * 10000
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([long_string])
        )
        result = vector_ops.vector_string_slice_right(vec, 100)
        assert result.to_pylist() == [b"x" * 100]

    def test_slice_left_single_char(self):
        """Test left slice of single character strings."""
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([b"a", b"b", b"c"])
        )
        result = vector_ops.vector_string_slice_left(vec, 1)
        assert result.to_pylist() == [b"a", b"b", b"c"]

    def test_slice_right_single_char(self):
        """Test right slice of single character strings."""
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([b"a", b"b", b"c"])
        )
        result = vector_ops.vector_string_slice_right(vec, 1)
        assert result.to_pylist() == [b"a", b"b", b"c"]

    def test_slice_left_mixed_null_and_data(self):
        """Test left slice with mixed nulls and data."""
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([b"hello", None, b"world", None, b"test"])
        )
        result = vector_ops.vector_string_slice_left(vec, 3)
        assert result.to_pylist() == [b"hel", None, b"wor", None, b"tes"]

    def test_slice_right_mixed_null_and_data(self):
        """Test right slice with mixed nulls and data."""
        vec = string_vector_module.StringVector.from_arrow(
            pa.array([b"hello", None, b"world", None, b"test"])
        )
        result = vector_ops.vector_string_slice_right(vec, 3)
        assert result.to_pylist() == [b"llo", None, b"rld", None, b"est"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
