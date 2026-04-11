import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import pyarrow as pa

from opteryx.managers.virtual_datasets import planet_data

print("=" * 80)
print("TEST: Check Arrow types from virtual data connector")
print("=" * 80)

# Get the morsel from planet_data
morsel = planet_data.read()

print(f"\nMorsel retrieved: {morsel.num_rows} rows, {morsel.num_columns} columns")
print(f"Morsel column types: {morsel.column_types}")

# Convert morsel to Arrow
arrow_table = None
print("\nConverting morsel to Arrow table...")
try:
    # Try to call to_arrow() if it exists
    if hasattr(morsel, "to_arrow"):
        arrow_table = morsel.to_arrow()
        print(f"Successfully converted morsel to Arrow table")
    else:
        print("Morsel does not have to_arrow() method")
except Exception as e:
    print(f"Error converting morsel to Arrow: {type(e).__name__}: {e}")

if arrow_table is not None:
    print(f"\nArrow table schema:")
    for i, field in enumerate(arrow_table.schema):
        print(f"  Column {i}: {field.name:20} -> {field.type}")

    # Check specific integer columns
    print(f"\nInteger column details:")
    id_col = arrow_table.column("id")
    diameter_col = arrow_table.column("diameter")

    print(f"  id column type: {id_col.type}")
    print(f"  id column chunk 0 type: {id_col.chunk(0).type if id_col.num_chunks > 0 else 'N/A'}")
    print(f"  diameter column type: {diameter_col.type}")
    print(
        f"  diameter column chunk 0 type: {diameter_col.chunk(0).type if diameter_col.num_chunks > 0 else 'N/A'}"
    )

# Now check what Vector.from_arrow creates from each type
print("\n" + "=" * 80)
print("TEST: Create vectors from Arrow columns")
print("=" * 80)

if arrow_table is not None:
    from opteryx.compiled.draken.vectors.vector import Vector

    id_arrow = arrow_table.column("id")
    if id_arrow.num_chunks > 0:
        id_chunk = id_arrow.chunk(0)
        print(f"\nID arrow chunk type: {id_chunk.type}")

        id_vec = Vector.from_arrow(id_chunk)
        print(f"Vector.from_arrow result:")
        print(f"  Type: {id_vec.__class__.__name__}")
        print(f"  Has equals: {hasattr(id_vec, 'equals')}")
        print(f"  Data: {id_vec.to_pylist()[:3]}...")

# Also check what happens with from_arrow on the whole column
print("\n" + "=" * 80)
print("TEST: Direct Arrow type analysis")
print("=" * 80)

int32_array = pa.array([1, 2, 3], type=pa.int32())
int64_array = pa.array([1, 2, 3], type=pa.int64())

print(f"\nint32 Arrow array type: {int32_array.type}")
vec_int32 = Vector.from_arrow(int32_array)
print(f"Vector.from_arrow result: {vec_int32.__class__.__name__}")

print(f"\nint64 Arrow array type: {int64_array.type}")
vec_int64 = Vector.from_arrow(int64_array)
print(f"Vector.from_arrow result: {vec_int64.__class__.__name__}")

print("\n" + "=" * 80)
print("TEST COMPLETE")
print("=" * 80)
