import os
import sys

# exact path depends on working directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from opteryx.compiled.draken.interop.arrow import vector_from_sequence

import opteryx
from opteryx.types import OrsoTypes

print("=" * 80)
print("TEST 1: Direct vector creation with OrsoTypes.INTEGER")
print("=" * 80)

test_data = [1, 2, 3, 4, 5]
print(f"Creating vector from: {test_data}")
print(f"Using dtype: OrsoTypes.INTEGER")

vec_int = vector_from_sequence(test_data, dtype=OrsoTypes.INTEGER)
print(f"Result vector type: {vec_int.__class__.__name__}")
print(f"Result vector length: {len(vec_int)}")
print(f"Result vector data: {vec_int.to_pylist()}")
print(f"Vector has 'equals' method: {hasattr(vec_int, 'equals')}")

print("\n" + "=" * 80)
print("TEST 2: Direct vector creation without explicit dtype")
print("=" * 80)

vec_auto = vector_from_sequence(test_data)
print(f"Result vector type: {vec_auto.__class__.__name__}")
print(f"Result vector length: {len(vec_auto)}")
print(f"Result vector data: {vec_auto.to_pylist()}")
print(f"Vector has 'equals' method: {hasattr(vec_auto, 'equals')}")

print("\n" + "=" * 80)
print("TEST 3: Create Int64Vector explicitly")
print("=" * 80)

from opteryx.compiled.draken.vectors.int64_vector import Int64Vector

vec_int64 = Int64Vector.from_constant(5, len(test_data), is_null=False)
print(f"Result vector type: {vec_int64.__class__.__name__}")
print(f"Result vector length: {len(vec_int64)}")
print(f"Result vector data: {vec_int64.to_pylist()}")
print(f"Vector has 'equals' method: {hasattr(vec_int64, 'equals')}")
print(f"Calling equals(5)...")
result = vec_int64.equals(5)
print(f"Result type: {result.__class__.__name__}")
print(f"Result: {result.to_pylist()}")

print("\n" + "=" * 80)
print("TEST 4: Planet data vector creation flow")
print("=" * 80)

# Directly trace what planet_data.read() does
from opteryx.managers.virtual_datasets import planet_data

print("Calling planet_data.read()...")
morsel = planet_data.read()
print(f"Got morsel with {morsel.num_rows} rows, {morsel.num_columns} columns")
print(f"Column names: {morsel.column_names}")
print(f"Column types: {morsel.column_types}")

# Get the id vector
id_vec = morsel.column(b"id")
print(f"\nID vector type: {id_vec.__class__.__name__}")
print(f"ID vector length: {len(id_vec)}")
print(f"ID vector data: {id_vec.to_pylist()}")

# Get the diameter vector
diameter_vec = morsel.column(b"diameter")
print(f"\nDiameter vector type: {diameter_vec.__class__.__name__}")
print(f"Diameter vector length: {len(diameter_vec)}")
print(f"Diameter vector data: {diameter_vec.to_pylist()}")

print("\n" + "=" * 80)
print("TEST 5: Check OrsoTypes.INTEGER value")
print("=" * 80)

print(f"OrsoTypes.INTEGER: {OrsoTypes.INTEGER}")
print(f"OrsoTypes.INTEGER.value: {OrsoTypes.INTEGER.value}")
print(f"OrsoTypes.INTEGER name: {OrsoTypes.INTEGER.name}")

from opteryx.compiled.draken.core.buffers import DRAKEN_INT32, DRAKEN_INT64

print(f"DRAKEN_INT32: {DRAKEN_INT32}")
print(f"DRAKEN_INT64: {DRAKEN_INT64}")

print("\n" + "=" * 80)
print("TEST 6: Check constant vector encoding")
print("=" * 80)

vec_const = Int64Vector.from_constant(42, 5, is_null=False)
print(f"Constant vector type: {vec_const.__class__.__name__}")
print(f"Constant vector encoding: {vec_const.encoding}")
print(f"Constant vector data: {vec_const.to_pylist()}")

print("\n" + "=" * 80)
print("DEBUG SCRIPT COMPLETE")
print("=" * 80)
