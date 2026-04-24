#!/usr/bin/env python3
"""Test to reproduce vector_abs_float64 segfault."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from opteryx.compiled.draken.vectors.float64_vector import Float64Vector

# Create a Float64Vector with some data
vec = Float64Vector(10)
for i in range(10):
    vec[i] = float(i - 5)

print(f"Created Float64Vector with {len(vec)} elements")
print(f"Data: {[vec[i] for i in range(len(vec))]}")

# Try to call ABS on it
try:
    from opteryx.compiled import vector_ops
    result = vector_ops.vector_abs_float64(vec)
    print(f"ABS result: {[result[i] for i in range(len(result))]}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
