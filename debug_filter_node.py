import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from opteryx.operators.filter_node import FilterNode

import opteryx

# Store original execute method
_original_execute = FilterNode.execute

call_count = [0]


def _patched_execute(self, morsel):
    """Patched execute method to trace morsel conversion."""
    call_count[0] += 1
    call_num = call_count[0]

    print(f"\n{'=' * 80}")
    print(f"FilterNode.execute() call #{call_num}")
    print(f"{'=' * 80}")

    from opteryx.compiled.draken.morsels.morsel import Morsel

    from opteryx import EOS
    from opteryx.expression.evaluator import evaluate_draken

    print(f"\n[BEFORE] Input morsel:")
    if morsel is EOS:
        print("  EOS marker")
        yield morsel
        return

    print(f"  Type: {type(morsel).__name__}")
    print(f"  Is Morsel: {isinstance(morsel, Morsel)}")

    if isinstance(morsel, Morsel):
        id_vec = morsel.column(b"id")
        print(f"  ID vector type: {id_vec.__class__.__name__}")
        print(f"  ID vector data: {id_vec.to_pylist()[:3]}")
        print(f"  Has equals: {hasattr(id_vec, 'equals')}")

    # This is the actual code from FilterNode.execute()
    if not isinstance(morsel, Morsel):
        print(f"\n[CONVERSION] Not a Morsel, converting from Arrow...")
        combined = morsel.combine_chunks()
        morsel_converted = Morsel.from_arrow(combined)
    else:
        print(f"\n[NO CONVERSION NEEDED] Already a Morsel")
        morsel_converted = morsel

    print(f"\n[AFTER CONVERSION] Morsel state:")
    print(f"  Type: {type(morsel_converted).__name__}")

    if isinstance(morsel_converted, Morsel):
        id_vec_after = morsel_converted.column(b"id")
        print(f"  ID vector type: {id_vec_after.__class__.__name__}")
        print(f"  ID vector data: {id_vec_after.to_pylist()[:3]}")
        print(f"  Has equals: {hasattr(id_vec_after, 'equals')}")

    print(f"\n[EVALUATE] Running filter evaluation...")
    print(f"  Filter expression: {self.filter}")

    try:
        mask = evaluate_draken(self.filter, morsel_converted)
        print(f"  Filter result type: {mask.__class__.__name__}")
        print(f"  Filter result data: {mask.to_pylist()[:5]}...")
    except Exception as e:
        print(f"  ERROR during evaluation: {type(e).__name__}: {e}")
        raise

    filtered = morsel_converted.filter_mask(mask)

    print(f"\n[FILTERED] Result morsel:")
    print(f"  Rows: {filtered.num_rows}")

    if filtered.num_rows > 0:
        yield filtered
    else:
        yield morsel_converted.slice(0, 0)


# Monkey patch the execute method
FilterNode.execute = _patched_execute

print("=" * 80)
print("DEBUG: FilterNode patched")
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
    import traceback

    traceback.print_exc()

print("\n" + "=" * 80)
print("DEBUG COMPLETE")
print("=" * 80)
