import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import pyarrow as pa
from opteryx.compiled.draken.interop.arrow import vector_from_arrow
from opteryx.compiled.draken.vectors.vector import Vector

print("=" * 80)
print("DIAGNOSTIC: IntegerVector Arrow Conversion")
print("=" * 80)

# Test 1: Simple int32 array
print("\n[TEST 1] Simple int32 array (no nulls)")
print("-" * 80)

data_1 = [1, 2, 3, 4, 5]
arrow_1 = pa.array(data_1, type=pa.int32())

print(f"Arrow array:")
print(f"  Type: {arrow_1.type}")
print(f"  Length: {len(arrow_1)}")
print(f"  Data: {arrow_1.to_pylist()}")
print(f"  Buffers: {arrow_1.buffers()}")
print(f"  Offset: {arrow_1.offset}")

vec_1 = vector_from_arrow(arrow_1)
print(f"\nDraken vector:")
print(f"  Type: {vec_1.__class__.__name__}")
print(f"  Length: {len(vec_1)}")
print(f"  Has data: {vec_1.ptr is not NULL if hasattr(vec_1, 'ptr') else 'N/A'}")
try:
    converted = vec_1.to_pylist()
    print(f"  Data: {converted}")
except Exception as e:
    print(f"  Error reading data: {e}")

# Test 2: int32 array with nulls
print("\n[TEST 2] int32 array with nulls")
print("-" * 80)

data_2 = [1, None, 3, None, 5]
arrow_2 = pa.array(data_2, type=pa.int32())

print(f"Arrow array:")
print(f"  Type: {arrow_2.type}")
print(f"  Length: {len(arrow_2)}")
print(f"  Data: {arrow_2.to_pylist()}")
print(f"  Buffers count: {len(arrow_2.buffers())}")
if arrow_2.buffers()[0] is not None:
    print(f"  Null bitmap size: {len(arrow_2.buffers()[0])}")
print(f"  Offset: {arrow_2.offset}")

vec_2 = vector_from_arrow(arrow_2)
print(f"\nDraken vector:")
print(f"  Type: {vec_2.__class__.__name__}")
print(f"  Length: {len(vec_2)}")
try:
    converted = vec_2.to_pylist()
    print(f"  Data: {converted}")
except Exception as e:
    print(f"  Error reading data: {e}")

# Test 3: int8 array
print("\n[TEST 3] int8 array")
print("-" * 80)

data_3 = [10, 20, 30, 40, 50]
arrow_3 = pa.array(data_3, type=pa.int8())

print(f"Arrow array:")
print(f"  Type: {arrow_3.type}")
print(f"  Length: {len(arrow_3)}")
print(f"  Data: {arrow_3.to_pylist()}")
print(f"  Offset: {arrow_3.offset}")

vec_3 = vector_from_arrow(arrow_3)
print(f"\nDraken vector:")
print(f"  Type: {vec_3.__class__.__name__}")
print(f"  Length: {len(vec_3)}")
try:
    converted = vec_3.to_pylist()
    print(f"  Data: {converted}")
except Exception as e:
    print(f"  Error reading data: {e}")

# Test 4: int16 array
print("\n[TEST 4] int16 array")
print("-" * 80)

data_4 = [100, 200, 300, 400, 500]
arrow_4 = pa.array(data_4, type=pa.int16())

print(f"Arrow array:")
print(f"  Type: {arrow_4.type}")
print(f"  Length: {len(arrow_4)}")
print(f"  Data: {arrow_4.to_pylist()}")
print(f"  Offset: {arrow_4.offset}")

vec_4 = vector_from_arrow(arrow_4)
print(f"\nDraken vector:")
print(f"  Type: {vec_4.__class__.__name__}")
print(f"  Length: {len(vec_4)}")
try:
    converted = vec_4.to_pylist()
    print(f"  Data: {converted}")
except Exception as e:
    print(f"  Error reading data: {e}")

# Test 5: Sliced/offset arrow array
print("\n[TEST 5] Sliced (offset) int32 array")
print("-" * 80)

data_5 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
arrow_5_full = pa.array(data_5, type=pa.int32())
arrow_5 = arrow_5_full.slice(2, 5)  # Skip first 2, take 5

print(f"Arrow array:")
print(f"  Type: {arrow_5.type}")
print(f"  Length: {len(arrow_5)}")
print(f"  Data: {arrow_5.to_pylist()}")
print(f"  Offset: {arrow_5.offset}")

vec_5 = vector_from_arrow(arrow_5)
print(f"\nDraken vector:")
print(f"  Type: {vec_5.__class__.__name__}")
print(f"  Length: {len(vec_5)}")
try:
    converted = vec_5.to_pylist()
    print(f"  Data: {converted}")
except Exception as e:
    print(f"  Error reading data: {e}")

print("\n" + "=" * 80)
print("DIAGNOSTIC COMPLETE")
print("=" * 80)
