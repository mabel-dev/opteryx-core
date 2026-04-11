import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import opteryx

print("=" * 80)
print("TEST: Examine query plan")
print("=" * 80)

session = opteryx.session()

# Parse and plan a query
query = "SELECT id FROM $planets WHERE id > 5"
print(f"\nQuery: {query}")

# Get the query plan
plan = session.parse_and_plan(query)

print(f"\nQuery plan type: {type(plan).__name__}")
print(f"Plan: {plan}")


# Try to traverse the plan
def print_plan_node(node, indent=0):
    prefix = "  " * indent
    print(f"{prefix}Node: {type(node).__name__}")
    if hasattr(node, "name"):
        print(f"{prefix}  Name: {node.name}")
    if hasattr(node, "config"):
        print(f"{prefix}  Config: {node.config}")
    if hasattr(node, "sources"):
        print(f"{prefix}  Sources: {len(node.sources)} source(s)")
        for source in node.sources:
            print_plan_node(source, indent + 1)


print_plan_node(plan)

print("\n" + "=" * 80)
print("TEST: Trace operator execution with morsel inspection")
print("=" * 80)

# Patch the operator base class to trace morsel conversions
from opteryx.operators.base_plan_node import BasePlanNode

_original_call = BasePlanNode.__call__

call_sequence = [0]


def _patched_call(self, morsel):
    call_sequence[0] += 1
    seq = call_sequence[0]

    print(f"\n[Operator {seq}] {self.name}")

    if morsel is not opteryx.EOS:
        if hasattr(morsel, "column_names"):
            print(f"  Input columns: {len(morsel.column_names)}")
            if b"id" in morsel.column_names:
                id_vec = morsel.column(b"id")
                print(f"  Input ID vector type: {id_vec.__class__.__name__}")
                print(
                    f"  Input ID data: {id_vec.to_pylist()[:3] if hasattr(id_vec, 'to_pylist') else 'N/A'}"
                )

    # Call original
    gen = _original_call(self, morsel)

    # Trace outputs
    for output_morsel in gen:
        if output_morsel is not opteryx.EOS:
            if hasattr(output_morsel, "column_names"):
                print(f"  Output columns: {len(output_morsel.column_names)}")
                if b"id" in output_morsel.column_names:
                    id_vec = output_morsel.column(b"id")
                    print(f"  Output ID vector type: {id_vec.__class__.__name__}")
                    print(
                        f"  Output ID data: {id_vec.to_pylist()[:3] if hasattr(id_vec, 'to_pylist') else 'N/A'}"
                    )
        yield output_morsel


BasePlanNode.__call__ = _patched_call

print("\nRunning query with operator tracing...")

try:
    morsels = list(session.execute_to_morsels(query))
    print(f"\nSUCCESS: Got {len(morsels)} morsels")
except Exception as e:
    print(f"\nERROR: {type(e).__name__}: {e}")

print("\n" + "=" * 80)
print("DEBUG COMPLETE")
print("=" * 80)
