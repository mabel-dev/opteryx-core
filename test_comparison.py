import sys
sys.path.insert(0, '.')

from opteryx.compiled.draken.vectors.integer_vector import IntegerVector

# Create a simple integer vector with values [1, 2, 3, 4, 5]
from array import array as pyarray
codes = pyarray('i', [0, 1, 2, 3, 4])
dictionary = pyarray('q', [1, 2, 3, 4, 5])

vec = IntegerVector.from_dict(codes, dictionary)

print(f"Vector length: {len(vec)}")
print(f"Vector values: {vec.to_pylist()}")

# Test the equals method
print("\nTesting equals(1)...")
result = vec.equals(1)
print(f"Result type: {type(result)}")
print(f"Result length: {len(result)}")
print(f"Result values: {result.to_pylist()}")

