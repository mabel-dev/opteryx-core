import sys
sys.path.insert(0, '.')

from opteryx.compiled.draken.vectors.integer_vector import IntegerVector
from opteryx.compiled.draken.vectors.bool_vector import BoolVector

# Create an integer vector directly with actual data (not dictionary-encoded)
import ctypes
from array import array as pyarray

# Create vector with actual int32 data
vec = IntegerVector(2, 5)  # DRAKEN_INT32=2, length=5

# Manually set data values [1, 2, 3, 4, 5]
data_ptr = ctypes.cast(vec.ptr.data, ctypes.POINTER(ctypes.c_int32))
for i in range(5):
    data_ptr[i] = i + 1

print(f"Vector values: {vec.to_pylist()}")

# Test equals
print("\nTesting equals(1)...")
result = vec.equals(1)
print(f"Result type: {type(result)}")
print(f"Result values: {result.to_pylist()}")

print("\nTesting equals(3)...")
result2 = vec.equals(3)
print(f"Result values: {result2.to_pylist()}")

