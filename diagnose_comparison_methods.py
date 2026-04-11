#!/usr/bin/env python
"""Diagnostic to check IntegerVector comparison method outputs."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import pyarrow as pa
from opteryx.compiled.draken.interop.arrow import vector_from_arrow
from opteryx.compiled.draken.vectors.bool_vector import BoolVector

print("=" * 80)
print("DIAGNOSTIC: IntegerVector Comparison Methods (CORRECTED)")
print("=" * 80)

# Create test IntegerVector from Arrow int32
data = [1, 2, 3, 4, 5]
arrow_array = pa.array(data, type=pa.int32())
vec = vector_from_arrow(arrow_array)

print(f"\nTest vector (int32): {vec.to_pylist()}")
print(f"Vector type: {vec.__class__.__name__}")

# Test 1: greater_than(3)
print("\n[TEST 1] vec.greater_than(3) - expect [False, False, False, True, True]")
print("-" * 80)
try:
    result = vec.greater_than(3)
    print(f"Result type: {result.__class__.__name__}")
    result_list = result.to_pylist()
    print(f"Result: {result_list}")
    expected = [False, False, False, True, True]
    if result_list == expected:
        print("✓ PASS")
    else:
        print(f"✗ FAIL: Expected {expected}")
except Exception as e:
    print(f"✗ ERROR: {e}")
    import traceback

    traceback.print_exc()

# Test 2: less_than(3)
print("\n[TEST 2] vec.less_than(3) - expect [True, True, False, False, False]")
print("-" * 80)
try:
    result = vec.less_than(3)
    result_list = result.to_pylist()
    print(f"Result: {result_list}")
    expected = [True, True, False, False, False]
    if result_list == expected:
        print("✓ PASS")
    else:
        print(f"✗ FAIL: Expected {expected}")
except Exception as e:
    print(f"✗ ERROR: {e}")

# Test 3: equals(3)
print("\n[TEST 3] vec.equals(3) - expect [False, False, True, False, False]")
print("-" * 80)
try:
    result = vec.equals(3)
    result_list = result.to_pylist()
    print(f"Result: {result_list}")
    expected = [False, False, True, False, False]
    if result_list == expected:
        print("✓ PASS")
    else:
        print(f"✗ FAIL: Expected {expected}")
except Exception as e:
    print(f"✗ ERROR: {e}")

# Test 4: not_equals(3)
print("\n[TEST 4] vec.not_equals(3) - expect [True, True, False, True, True]")
print("-" * 80)
try:
    result = vec.not_equals(3)
    result_list = result.to_pylist()
    print(f"Result: {result_list}")
    expected = [True, True, False, True, True]
    if result_list == expected:
        print("✓ PASS")
    else:
        print(f"✗ FAIL: Expected {expected}")
except Exception as e:
    print(f"✗ ERROR: {e}")

# Test 5: greater_than_or_equals(3)
print("\n[TEST 5] vec.greater_than_or_equals(3) - expect [False, False, True, True, True]")
print("-" * 80)
try:
    result = vec.greater_than_or_equals(3)
    result_list = result.to_pylist()
    print(f"Result: {result_list}")
    expected = [False, False, True, True, True]
    if result_list == expected:
        print("✓ PASS")
    else:
        print(f"✗ FAIL: Expected {expected}")
except Exception as e:
    print(f"✗ ERROR: {e}")

# Test 6: less_than_or_equals(3)
print("\n[TEST 6] vec.less_than_or_equals(3) - expect [True, True, True, False, False]")
print("-" * 80)
try:
    result = vec.less_than_or_equals(3)
    result_list = result.to_pylist()
    print(f"Result: {result_list}")
    expected = [True, True, True, False, False]
    if result_list == expected:
        print("✓ PASS")
    else:
        print(f"✗ FAIL: Expected {expected}")
except Exception as e:
    print(f"✗ ERROR: {e}")

# Test 7: Test with int64 Arrow array
print("\n[TEST 7] int64 IntegerVector - vec.greater_than(3)")
print("-" * 80)
try:
    arrow_array_i64 = pa.array([1, 2, 3, 4, 5], type=pa.int64())
    vec_i64 = vector_from_arrow(arrow_array_i64)
    print(f"int64 vector type: {vec_i64.__class__.__name__}")
    result = vec_i64.greater_than(3)
    result_list = result.to_pylist()
    print(f"Result: {result_list}")
    expected = [False, False, False, True, True]
    if result_list == expected:
        print("✓ PASS: int64 support working!")
    else:
        print(f"✗ FAIL: Expected {expected}")
except Exception as e:
    print(f"✗ ERROR: {e}")
    import traceback

    traceback.print_exc()

# Test 8: BoolVector inversion
print("\n[TEST 8] BoolVector.not_vector() - check if inversion works")
print("-" * 80)
try:
    bool_array = pa.array([True, False, True, False, True], type=pa.bool_())
    bool_vec = BoolVector.from_arrow(bool_array)
    print(f"Original: {bool_vec.to_pylist()}")
    inverted = bool_vec.not_vector()
    print(f"Inverted: {inverted.to_pylist()}")
    expected = [False, True, False, True, False]
    if inverted.to_pylist() == expected:
        print("✓ PASS: BoolVector inversion works")
    else:
        print(f"✗ FAIL: Expected {expected}")
except Exception as e:
    print(f"✗ ERROR: {e}")

print("\n" + "=" * 80)
print("DIAGNOSTIC COMPLETE")
print("=" * 80)
