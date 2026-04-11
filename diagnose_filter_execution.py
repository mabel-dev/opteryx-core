#!/usr/bin/env python
"""Diagnostic to trace filter execution pipeline end-to-end."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import opteryx

print("=" * 80)
print("DIAGNOSTIC: Filter Execution Pipeline Trace")
print("=" * 80)

# Create a session and execute a simple filter query
session = opteryx.session()

print("\n[STEP 1] Execute: SELECT * FROM $planets WHERE id = 1")
print("-" * 80)

try:
    morsels = list(session.execute_to_morsels("SELECT * FROM $planets WHERE id = 1"))
    print(f"Number of morsels returned: {len(morsels)}")

    if len(morsels) > 0:
        morsel = morsels[0]
        print(f"First morsel row count: {morsel.length}")
        print(f"First morsel columns: {list(morsel)}")

        # Extract first column data
        col_count = 0
        for col_name in morsel:
            col_count += 1
            col_vector = morsel[col_name]
            print(f"  Column '{col_name}':")
            print(f"    Type: {col_vector.__class__.__name__}")
            print(f"    Length: {len(col_vector)}")
            print(
                f"    Data: {col_vector.to_pylist() if hasattr(col_vector, 'to_pylist') else 'N/A'}"
            )
    else:
        print("✗ NO MORSELS RETURNED - query filter failed!")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import traceback

    traceback.print_exc()

print("\n[STEP 2] Execute: SELECT * FROM $planets (no filter)")
print("-" * 80)

try:
    morsels = list(session.execute_to_morsels("SELECT * FROM $planets"))
    print(f"Number of morsels returned: {len(morsels)}")

    if len(morsels) > 0:
        morsel = morsels[0]
        print(f"First morsel row count: {morsel.length}")
        print(f"First morsel columns: {list(morsel)}")

        # Extract id column
        id_col = morsel["id"]
        print(f"ID column type: {id_col.__class__.__name__}")
        print(f"ID column data: {id_col.to_pylist() if hasattr(id_col, 'to_pylist') else 'N/A'}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import traceback

    traceback.print_exc()

print("\n[STEP 3] Direct comparison test on unfiltered data")
print("-" * 80)

try:
    morsels = list(session.execute_to_morsels("SELECT * FROM $planets"))
    if len(morsels) > 0:
        morsel = morsels[0]
        id_col = morsel["id"]

        print(f"ID vector type: {id_col.__class__.__name__}")
        print(f"ID vector length: {len(id_col)}")
        print(f"ID vector data: {id_col.to_pylist()}")

        # Try direct comparison
        print(f"\nAttempting direct comparison: id_col.equals(1)")
        result = id_col.equals(1)
        print(f"Result type: {result.__class__.__name__}")
        print(f"Result length: {len(result)}")
        print(f"Result data: {result.to_pylist()}")
        print(f"Result sum (expected 1 if id=1 found): {sum(result.to_pylist())}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import traceback

    traceback.print_exc()

print("\n[STEP 4] Test different filter types")
print("-" * 80)

test_queries = [
    ("SELECT * FROM $planets WHERE id = 1", "equality"),
    ("SELECT * FROM $planets WHERE id > 5", "greater than"),
    ("SELECT * FROM $planets WHERE id IN (1, 3, 5)", "IN list"),
]

for query, desc in test_queries:
    print(f"\n{desc.upper()}: {query}")
    try:
        morsels = list(session.execute_to_morsels(query))
        row_count = sum(m.length for m in morsels)
        print(f"  Result: {row_count} rows")
        if row_count > 0:
            print(f"  ✓ PASS")
        else:
            print(f"  ✗ FAIL (expected > 0)")
    except Exception as e:
        print(f"  ✗ ERROR: {e}")

print("\n" + "=" * 80)
print("DIAGNOSTIC COMPLETE")
print("=" * 80)
