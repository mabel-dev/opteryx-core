import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from opteryx.compiled.draken.morsels.morsel import Morsel

import opteryx

print("=" * 80)
print("TEST: Trace Morsel.column() calls")
print("=" * 80)

# Patch Morsel.column to trace calls
_original_column = Morsel.column

call_count = [0]


def _patched_column(self, identity, column_name=b""):
    call_count[0] += 1
    call_num = call_count[0]

    print(f"\n[Call #{call_num}] Morsel.column() called:")
    print(f"  identity: {identity}")
    print(f"  column_name: {column_name}")
    print(f"  morsel rows: {self.ptr.num_rows if hasattr(self, 'ptr') else 'N/A'}")
    print(f"  morsel columns: {self.ptr.num_columns if hasattr(self, 'ptr') else 'N/A'}")

    # Call original
    result = _original_column(self, identity, column_name)

    print(f"  result type: {result.__class__.__name__}")
    print(f"  result length: {len(result) if hasattr(result, '__len__') else 'N/A'}")
    if hasattr(result, "to_pylist"):
        data = result.to_pylist()
        print(f"  result data: {data[:5] if len(data) > 5 else data}...")
    else:
        print(f"  result data: N/A")
    print(f"  has equals: {hasattr(result, 'equals')}")
    print(f"  has greater_than: {hasattr(result, 'greater_than')}")

    return result


Morsel.column = _patched_column

print("Running query: SELECT id FROM $planets WHERE id > 5")
print("=" * 80)

try:
    session = opteryx.session()
    morsels = list(session.execute_to_morsels("SELECT id FROM $planets WHERE id > 5"))
    print(f"\n\nSUCCESS: Got {len(morsels)} morsels")
    if morsels:
        m = morsels[0]
        print(f"Final morsel: {m.num_rows} rows")
except Exception as e:
    print(f"\n\nERROR: {type(e).__name__}: {e}")
