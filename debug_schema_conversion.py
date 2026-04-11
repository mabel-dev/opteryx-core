import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import opteryx
from opteryx.managers.virtual_datasets import planet_data
from opteryx.types import OrsoTypes

print("=" * 80)
print("TEST 1: Check schema information")
print("=" * 80)

schema = planet_data.schema()
print(f"Schema name: {schema.name}")
print(f"Schema num columns: {len(schema.columns)}")

for col in schema.columns[:5]:  # First 5 columns
    print(f"\nColumn: {col.name}")
    print(f"  Type: {col.type}")
    print(f"  Type value: {col.type.value if hasattr(col.type, 'value') else 'N/A'}")

print("\n" + "=" * 80)
print("TEST 2: Check morsel creation directly")
print("=" * 80)

morsel = planet_data.read()
print(f"Morsel has {morsel.num_rows} rows, {morsel.num_columns} columns")
print(f"Column types from morsel: {morsel.column_types}")

# Get vectors
id_vec = morsel.column(b"id")
diameter_vec = morsel.column(b"diameter")

print(f"\nID vector: {id_vec.__class__.__name__} (data: {id_vec.to_pylist()[:3]}...)")
print(
    f"Diameter vector: {diameter_vec.__class__.__name__} (data: {diameter_vec.to_pylist()[:3]}...)"
)

print("\n" + "=" * 80)
print("TEST 3: Patch _eval_value to trace vector conversion")
print("=" * 80)

from opteryx.expression.evaluator import evaluation

_original_eval_value = evaluation._eval_value


def _patched_eval_value(node, morsel):
    from opteryx.expression import NodeType

    result = _original_eval_value(node, morsel)

    if node.node_type == NodeType.IDENTIFIER and hasattr(node, "schema_column"):
        print(f"\n_eval_value for column: {node.schema_column.name}")
        print(f"  Schema column type: {node.schema_column.type}")
        print(f"  Result vector type: {result.__class__.__name__}")
        print(
            f"  Result vector data sample: {result.to_pylist()[:3] if hasattr(result, 'to_pylist') else 'N/A'}"
        )
        print(
            f"  Has comparison methods: equals={hasattr(result, 'equals')}, greater_than={hasattr(result, 'greater_than')}"
        )

    return result


evaluation._eval_value = _patched_eval_value

print("Running query: SELECT id FROM $planets WHERE id > 5")

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

print("\n" + "=" * 80)
print("TEST 4: Trace Morsel.from_vectors")
print("=" * 80)

from opteryx.compiled.draken.interop.arrow import vector_from_sequence
from opteryx.compiled.draken.morsels.morsel import Morsel

# Create vectors manually like planet_data does
col_names = ["id", "diameter"]
vectors = [
    vector_from_sequence([1, 2, 3, 4, 5], dtype=OrsoTypes.INTEGER),
    vector_from_sequence([4879, 12104, 12756, 6792, 142984], dtype=OrsoTypes.INTEGER),
]

print(f"Vectors before Morsel.from_vectors:")
for i, vec in enumerate(vectors):
    print(f"  {col_names[i]}: {vec.__class__.__name__} - {vec.to_pylist()[:3]}")

morsel_manual = Morsel.from_vectors(col_names, vectors)

print(f"\nVectors after Morsel.from_vectors:")
id_vec_after = morsel_manual.column(b"id")
diameter_vec_after = morsel_manual.column(b"diameter")

print(f"  id: {id_vec_after.__class__.__name__} - {id_vec_after.to_pylist()[:3]}")
print(f"  diameter: {diameter_vec_after.__class__.__name__} - {diameter_vec_after.to_pylist()[:3]}")

print("\n" + "=" * 80)
print("DEBUG COMPLETE")
print("=" * 80)
