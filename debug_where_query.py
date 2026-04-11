import os
import sys

# exact path depends on working directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import opteryx

print("=" * 80)
print("TEST: Trace vector types during query execution")
print("=" * 80)

# Patch the _int64_compare function to log what's happening
original_code = """
from opteryx.expression.evaluator import comparisons

_original_int64_compare = comparisons._int64_compare

def _patched_int64_compare(op: str, vec, right):
    print(f"\\n_int64_compare called:")
    print(f"  op: {op}")
    print(f"  vec type: {vec.__class__.__name__}")
    print(f"  vec length: {len(vec) if hasattr(vec, '__len__') else 'N/A'}")
    print(f"  right type: {type(right).__name__}")
    print(f"  right value: {right}")
    print(f"  vec has 'equals': {hasattr(vec, 'equals')}")
    print(f"  vec has 'greater_than': {hasattr(vec, 'greater_than')}")
    print(f"  vec methods: {[m for m in dir(vec) if not m.startswith('_') and callable(getattr(vec, m))][:10]}")

    return _original_int64_compare(op, vec, right)

comparisons._int64_compare = _patched_int64_compare
"""

exec(original_code)

# Now try to run a query
print("\nRunning query: SELECT id FROM $planets WHERE id > 5")
print("=" * 80)

try:
    session = opteryx.session()
    morsels = list(session.execute_to_morsels("SELECT id FROM $planets WHERE id > 5"))
    print(f"\nSUCCESS: Got {len(morsels)} morsels")
    if morsels:
        m = morsels[0]
        print(f"First morsel: {m.num_rows} rows")
        id_vec = m.column(b"id")
        print(f"ID values: {id_vec.to_pylist()}")
except Exception as e:
    print(f"\nERROR: {type(e).__name__}: {e}")
    import traceback

    traceback.print_exc()
