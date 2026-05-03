"""Tests for operations on dictionary-encoded StringVectors, especially dict-only vectors.

Dict-only vectors are created by the Parquet reader and have:
- ptr.data = NULL (no dense string data)
- ptr.offsets = NULL (no offset array)
- _dict_values, _dict_codes, _dict_code_width are populated

Operations must materialize dict-only vectors before accessing dense pointers.
"""

import pytest
import pyarrow as pa
from array import array
from draken.vectors import string_vector as string_vector_module
from opteryx.compiled import vector_ops


def test_dict_only_vector_creation():
    """Test that from_dict_buffers creates a dict-only vector."""
    vec = string_vector_module.StringVector.from_dict_buffers(
        array("i", [0, 1, 0, 0]),  # codes: [0, 1, 0, null]
        array("i", [0, 5]),  # dict offsets
        array("i", [5, 4]),  # dict data: "alpha" (5 bytes), "beta" (4 bytes)
        bytearray(b"alphabeta"),  # dict data
        bytearray([1, 1, 1, 0]),  # null bitmap
    )

    # Check that the vector is properly created
    # (can't easily check internal encoding from Python, so just verify functionality)
    assert vec.to_pylist() == [b"alpha", b"beta", b"alpha", None]


def test_vector_string_length_on_dict_only():
    """Test vector_string_length() on dict-only vectors."""
    vec = string_vector_module.StringVector.from_dict_buffers(
        array("i", [0, 1, 0, 0]),  # codes
        array("i", [0, 5]),  # dict offsets
        array("i", [5, 4]),  # dict data
        bytearray(b"alphabeta"),  # dict data
        bytearray([1, 1, 1, 0]),  # null bitmap
    )

    # This should materialize the dict-only vector internally
    lengths = vector_ops.vector_string_length(vec)

    # Check the result
    result = lengths.to_pylist()
    assert result == [5, 4, 5, 0]  # lengths: alpha=5, beta=4, alpha=5, null=0


def test_vector_string_length_on_dict_with_dense():
    """Test vector_string_length() on dictionary vectors (with dense data)."""
    # Create a dictionary vector with separate codes and data
    vec = string_vector_module.StringVector.from_dict(
        array("i", [0, 1, 0, 0]),  # codes
        [b"hello", b"world"],  # dictionary values
        bytearray([1, 1, 1, 0]),  # null bitmap
    )

    # Get lengths - should work on dict vectors
    lengths = vector_ops.vector_string_length(vec)

    result = lengths.to_pylist()
    assert result == [5, 5, 5, 0]  # hello=5, world=5, hello=5, null=0


def test_dict_vector_hash():
    """Test that hash_into() works on dictionary vectors."""
    vec = string_vector_module.StringVector.from_dict_buffers(
        array("i", [0, 1, 0, 0]),  # codes
        array("i", [0, 5]),  # dict offsets
        array("i", [5, 4]),  # dict data
        bytearray(b"alphabeta"),  # dict data
        bytearray([1, 1, 1, 0]),  # null bitmap
    )

    # This should not crash on dict-only vectors
    hashes = vec.hash()

    # Should have 4 hash values
    assert len(hashes) == 4
    # Hashes for "alpha", "beta", "alpha", and NULL should be predictable
    # (same strings should have same hashes)
    assert hashes[0] == hashes[2]  # both "alpha"


def test_dict_vector_materialization():
    """Test that dict-only vectors can be materialized and used in operations."""
    # Create a dict-only vector
    # Dictionary: "alpha" (5 bytes), "beta" (4 bytes), "gamma" (5 bytes)
    vec = string_vector_module.StringVector.from_dict_buffers(
        array("i", [0, 1, 0, 2]),  # codes: [alpha, beta, alpha, gamma]
        array("i", [0, 5, 9]),  # dict offsets (one per dictionary entry)
        array("i", [5, 4, 5]),  # dict lengths: alpha=5, beta=4, gamma=5
        bytearray(b"alphabetagamma"),  # dict data
        bytearray([1, 1, 1, 1]),  # null bitmap (all valid)
    )

    # Test that we can materialize and use the vector
    result = vec.to_pylist()
    assert result == [b"alpha", b"beta", b"alpha", b"gamma"]

    # Test hash function works on dict-only vectors
    hashes = vec.hash()
    assert len(hashes) == 4
    assert hashes[0] == hashes[2]  # "alpha" appears twice


def test_dict_only_lowercase():
    """Test vector_lowercase() on dict-only vectors."""
    vec = string_vector_module.StringVector.from_dict_buffers(
        array("i", [0, 1]),  # codes
        array("i", [0, 5]),  # dict offsets
        array("i", [5, 4]),  # dict data
        bytearray(b"ALPHAbeta"),  # mixed case dict data
        bytearray([1, 1]),  # null bitmap
    )

    result = vector_ops.vector_lowercase(vec)
    assert result.to_pylist() == [b"alpha", b"beta"]


def test_dict_only_trim():
    """Test vector_trim() on dict-only vectors."""
    vec = string_vector_module.StringVector.from_dict_buffers(
        array("i", [0, 1, 0]),
        array("i", [0, 7]),
        array("i", [7, 5]),
        bytearray(b"  hello world  "),
        bytearray([1, 1, 1]),
    )

    result = vector_ops.vector_trim(vec)
    assert result.to_pylist() == [b"hello", b"world", b"hello"]


def test_dict_only_slice_left():
    """Test vector_string_slice_left() on dict-only vectors."""
    vec = string_vector_module.StringVector.from_dict_buffers(
        array("i", [0, 1, 0, 0]),
        array("i", [0, 5]),
        array("i", [5, 4]),
        bytearray(b"alphabeta"),
        bytearray([1, 1, 1, 0]),
    )

    result = vector_ops.vector_string_slice_left(vec, 2)
    assert result.to_pylist() == [b"al", b"be", b"al", None]


def test_dict_only_slice_right():
    """Test vector_string_slice_right() on dict-only vectors."""
    vec = string_vector_module.StringVector.from_dict_buffers(
        array("i", [0, 1, 0, 0]),
        array("i", [0, 5]),
        array("i", [5, 4]),
        bytearray(b"alphabeta"),
        bytearray([1, 1, 1, 0]),
    )

    result = vector_ops.vector_string_slice_right(vec, 2)
    assert result.to_pylist() == [b"ha", b"ta", b"ha", None]




if __name__ == "__main__":
    pytest.main([__file__, "-v"])
