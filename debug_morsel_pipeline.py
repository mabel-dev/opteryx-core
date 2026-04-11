import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from opteryx.compiled.draken.morsels.morsel import Morsel

from opteryx.managers.virtual_datasets import planet_data

print("=" * 80)
print("TEST 1: Trace Morsel.from_arrow() conversion")
print("=" * 80)

# Get the original morsel
morsel_orig = planet_data.read()
print(f"\nOriginal morsel from planet_data.read():")
print(f"  Rows: {morsel_orig.num_rows}")
print(f"  Column types: {morsel_orig.column_types[:5]}")

# Get the ID vector before conversion
id_vec_orig = morsel_orig.column(b"id")
print(f"\nID vector (original):")
print(f"  Type: {id_vec_orig.__class__.__name__}")
print(f"  Data: {id_vec_orig.to_pylist()}")
print(f"  Has equals: {hasattr(id_vec_orig, 'equals')}")

# Convert to Arrow
arrow_table = morsel_orig.to_arrow()
print(f"\nConverted to Arrow:")
print(f"  ID column type: {arrow_table.column('id').type}")
print(f"  ID column data: {arrow_table.column('id').to_pylist()}")

# Convert back from Arrow
print(f"\nConverting back from Arrow with Morsel.from_arrow()...")
morsel_reconverted = Morsel.from_arrow(arrow_table)

print(f"Reconverted morsel:")
print(f"  Rows: {morsel_reconverted.num_rows}")
print(f"  Column types: {morsel_reconverted.column_types[:5]}")

# Get the ID vector after reconversion
id_vec_reconverted = morsel_reconverted.column(b"id")
print(f"\nID vector (reconverted):")
print(f"  Type: {id_vec_reconverted.__class__.__name__}")
print(f"  Data: {id_vec_reconverted.to_pylist()}")
print(f"  Has equals: {hasattr(id_vec_reconverted, 'equals')}")

print("\n" + "=" * 80)
print("TEST 2: Check Vector.to_arrow() for Int64Vector")
print("=" * 80)

# Check what Int64Vector.to_arrow() returns
int64_vec = id_vec_orig
print(f"\nOriginal Int64Vector:")
print(f"  Type: {int64_vec.__class__.__name__}")
print(f"  Data: {int64_vec.to_pylist()}")

arrow_from_int64 = int64_vec.to_arrow()
print(f"\nArrow from Int64Vector.to_arrow():")
print(f"  Type: {arrow_from_int64.type}")
print(f"  Data: {arrow_from_int64.to_pylist()}")

# Now convert that Arrow array back
from opteryx.compiled.draken.vectors.vector import Vector

vec_back = Vector.from_arrow(arrow_from_int64)
print(f"\nVector.from_arrow() on that Arrow array:")
print(f"  Type: {vec_back.__class__.__name__}")
print(f"  Data: {vec_back.to_pylist()}")
print(f"  Has equals: {hasattr(vec_back, 'equals')}")

print("\n" + "=" * 80)
print("TEST 3: Check if combining chunks affects types")
print("=" * 80)

arrow_table_chunked = morsel_orig.to_arrow()
if any(col.num_chunks > 1 for col in arrow_table_chunked.columns):
    print("Arrow table has multiple chunks, combining...")
    arrow_table_combined = arrow_table_chunked.combine_chunks()
else:
    print("Arrow table already has single chunks")
    arrow_table_combined = arrow_table_chunked

print(f"\nAfter combine_chunks():")
id_col_combined = arrow_table_combined.column("id")
print(f"  ID column type: {id_col_combined.type}")
print(f"  ID column num_chunks: {id_col_combined.num_chunks}")
if id_col_combined.num_chunks > 0:
    print(f"  First chunk type: {id_col_combined.chunk(0).type}")

print("\n" + "=" * 80)
print("TEST 4: Simulate FilterNode behavior")
print("=" * 80)

print("\nSimulating FilterNode.execute():")
morsel_for_filter = planet_data.read()
print(f"  Input morsel ID vector type: {morsel_for_filter.column(b'id').__class__.__name__}")

# This is what FilterNode does
if not isinstance(morsel_for_filter, Morsel):
    print("  Converting to Morsel from Arrow...")
    morsel_after_check = Morsel.from_arrow(morsel_for_filter.combine_chunks())
else:
    print("  Already a Morsel, checking if conversion happens...")
    # FilterNode checks if it's already a Morsel
    arrow_table_for_filter = morsel_for_filter.to_arrow()
    print(f"  Calling combine_chunks() on to_arrow() result...")
    combined = arrow_table_for_filter.combine_chunks()
    morsel_after_check = Morsel.from_arrow(combined)

print(f"  After FilterNode processing:")
id_vec_after = morsel_after_check.column(b"id")
print(f"    ID vector type: {id_vec_after.__class__.__name__}")
print(f"    ID vector data: {id_vec_after.to_pylist()}")
print(f"    Has equals: {hasattr(id_vec_after, 'equals')}")

print("\n" + "=" * 80)
print("TEST 5: Check if there's any special handling for OrsoTypes.INTEGER")
print("=" * 80)

from opteryx.types import OrsoTypes
from opteryx.types.schema import FlatColumn

schema = planet_data.schema()
id_column = schema.columns[0]
print(f"\nSchema column 'id':")
print(f"  Name: {id_column.name}")
print(f"  Type: {id_column.type}")
print(f"  Type == OrsoTypes.INTEGER: {id_column.type == OrsoTypes.INTEGER}")

print(f"\nOrsoTypes.INTEGER value: {OrsoTypes.INTEGER.value}")

print("\n" + "=" * 80)
print("DEBUG SCRIPT COMPLETE")
print("=" * 80)
