import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import opteryx
from opteryx.connectors.virtual_data_connector import VirtualDataTable

# Store original read_dataset method
_original_read_dataset = VirtualDataTable.read_dataset

call_count = [0]


def _patched_read_dataset(self, columns=None, **kwargs):
    """Patched read_dataset to trace morsel creation."""
    call_count[0] += 1
    call_num = call_count[0]

    print(f"\n{'=' * 80}")
    print(f"VirtualDataTable.read_dataset() call #{call_num}")
    print(f"{'=' * 80}")
    print(f"Dataset: {self.dataset}")
    print(f"Columns: {columns}")

    # Call original generator
    for morsel in _original_read_dataset(self, columns=columns, **kwargs):
        print(f"\n[YIELDED MORSEL]")
        print(f"  Type: {type(morsel).__name__}")
        print(f"  Rows: {morsel.num_rows if hasattr(morsel, 'num_rows') else 'N/A'}")

        if hasattr(morsel, "column_names"):
            print(f"  Column names: {morsel.column_names[:3]}")
            print(f"  Column types: {morsel.column_types[:3]}")

        if hasattr(morsel, "column"):
            try:
                id_vec = morsel.column(b"id")
                print(f"  ID vector type: {id_vec.__class__.__name__}")
                print(f"  ID vector data: {id_vec.to_pylist()[:3]}")
                print(f"  Has equals: {hasattr(id_vec, 'equals')}")
            except Exception as e:
                print(f"  Could not get ID vector: {type(e).__name__}")

        yield morsel


# Monkey patch the read_dataset method
VirtualDataTable.read_dataset = _patched_read_dataset

print("=" * 80)
print("DEBUG: VirtualDataTable patched")
print("=" * 80)

print("\nRunning query: SELECT id FROM $planets WHERE id > 5")
print("=" * 80)

try:
    session = opteryx.session()
    morsels = list(session.execute_to_morsels("SELECT id FROM $planets WHERE id > 5"))
    print(f"\n\nSUCCESS: Got {len(morsels)} morsels")
    if morsels:
        m = morsels[0]
        print(f"Final morsel: {m.num_rows} rows")
        id_vec = m.column(b"id")
        print(f"ID values: {id_vec.to_pylist()}")
except Exception as e:
    print(f"\n\nERROR: {type(e).__name__}: {e}")

print("\n" + "=" * 80)
print("DEBUG COMPLETE")
print("=" * 80)
