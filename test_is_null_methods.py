#!/usr/bin/env python
"""Test the new is_null() and is_null_with_nan() methods for Draken vectors."""

import os
import sys

# Add opteryx-core to path
sys.path.insert(0, os.path.dirname(__file__))

import math

from opteryx.compiled.draken.vectors.float64_vector import Float64Vector
from opteryx.compiled.draken.vectors.string_vector import StringVector


def test_stringvector_is_null():
    """Test StringVector.is_null() method."""
    print("Testing StringVector.is_null()...")

    # Create a StringVector with some nulls
    vec = StringVector(5, 100)

    # Add some test data
    vec.append(b"hello")
    vec.append_null()
    vec.append(b"world")
    vec.append_null()
    vec.append(b"test")

    result = vec.finish()
    null_mask = result.is_null()

    # Convert memoryview to list for easier inspection
    null_list = list(null_mask)

    print(f"  StringVector null mask: {null_list}")
    assert null_list == [0, 1, 0, 1, 0], f"Expected [0, 1, 0, 1, 0], got {null_list}"
    print("  ✓ StringVector.is_null() works correctly")


def test_stringvector_constant():
    """Test StringVector.is_null() with constant encoding."""
    print("Testing StringVector.is_null() with constant encoding...")

    # Test constant null
    vec_null = StringVector.from_constant(None, 3, is_null=True)
    null_mask = vec_null.is_null()
    null_list = list(null_mask)

    print(f"  Constant null mask: {null_list}")
    assert null_list == [1, 1, 1], f"Expected [1, 1, 1], got {null_list}"

    # Test constant non-null
    vec_val = StringVector.from_constant(b"constant", 3, is_null=False)
    null_mask = vec_val.is_null()
    null_list = list(null_mask)

    print(f"  Constant value mask: {null_list}")
    assert null_list == [0, 0, 0], f"Expected [0, 0, 0], got {null_list}"
    print("  ✓ StringVector constant encoding works correctly")


def test_float64vector_is_null():
    """Test Float64Vector.is_null() method."""
    print("Testing Float64Vector.is_null()...")

    # Create a Float64Vector with some nulls
    vec = Float64Vector(5)

    # Set some values and nulls
    vec.append(1.5)
    vec.append_null()
    vec.append(3.14)
    vec.append_null()
    vec.append(2.71)

    result = vec.finish()
    null_mask = result.is_null()
    null_list = list(null_mask)

    print(f"  Float64Vector null mask: {null_list}")
    assert null_list == [0, 1, 0, 1, 0], f"Expected [0, 1, 0, 1, 0], got {null_list}"
    print("  ✓ Float64Vector.is_null() works correctly")


def test_float64vector_is_null_with_nan():
    """Test Float64Vector.is_null_with_nan() method."""
    print("Testing Float64Vector.is_null_with_nan()...")

    # Create a Float64Vector with NaN and regular nulls
    vec = Float64Vector(5)

    # Set some values, NaNs, and nulls
    vec.append(1.5)
    vec.append_null()
    vec.append(math.nan)
    vec.append_null()
    vec.append(2.71)

    result = vec.finish()

    # Test regular is_null
    null_mask = result.is_null()
    null_list = list(null_mask)
    print(f"  Float64Vector null mask (without NaN): {null_list}")
    assert null_list == [0, 1, 0, 1, 0], f"Expected [0, 1, 0, 1, 0], got {null_list}"

    # Test is_null_with_nan
    nan_null_mask = result.is_null_with_nan()
    nan_null_list = list(nan_null_mask)
    print(f"  Float64Vector null mask (with NaN): {nan_null_list}")
    assert nan_null_list == [0, 1, 1, 1, 0], f"Expected [0, 1, 1, 1, 0], got {nan_null_list}"
    print("  ✓ Float64Vector.is_null_with_nan() works correctly")


def test_float64vector_constant():
    """Test Float64Vector.is_null() with constant encoding."""
    print("Testing Float64Vector.is_null() with constant encoding...")

    # Test constant null
    vec_null = Float64Vector.from_constant(0.0, 3, is_null=True)
    null_mask = vec_null.is_null()
    null_list = list(null_mask)

    print(f"  Constant null mask: {null_list}")
    assert null_list == [1, 1, 1], f"Expected [1, 1, 1], got {null_list}"

    # Test constant non-null
    vec_val = Float64Vector.from_constant(42.0, 3, is_null=False)
    null_mask = vec_val.is_null()
    null_list = list(null_mask)

    print(f"  Constant value mask: {null_list}")
    assert null_list == [0, 0, 0], f"Expected [0, 0, 0], got {null_list}"
    print("  ✓ Float64Vector constant encoding works correctly")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing new is_null() vector methods")
    print("=" * 60)

    try:
        test_stringvector_is_null()
        test_stringvector_constant()
        test_float64vector_is_null()
        test_float64vector_is_null_with_nan()
        test_float64vector_constant()

        print("=" * 60)
        print("✓ All tests passed!")
        print("=" * 60)
    except Exception as e:
        print("=" * 60)
        print(f"✗ Test failed: {e}")
        print("=" * 60)
        import traceback

        traceback.print_exc()
        sys.exit(1)
