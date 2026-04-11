import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from opteryx.compiled.draken.interop.arrow import vector_from_sequence
from opteryx.compiled.draken.morsels.morsel import Morsel

import opteryx
from opteryx.managers.virtual_datasets import planet_data
from opteryx.types import OrsoTypes

print("=" * 80)
print("TEST 1: Create vectors manually and check addresses")
print("=" * 80)

test_data = [1, 2, 3, 4, 5]
vec1 = vector_from_sequence(test_data, dtype=OrsoTypes.INTEGER)

print(f"\nVector created with vector_from_sequence:")
print(f"  Type: {vec1.__class__.__name__}")
print(f"  Object ID: {id(vec1)}")
print(f"  Data: {vec1.to_pylist()}")
print(f"  Has comparison methods: {hasattr(vec1, 'equals')}")

# Create a morsel with this vector
morsel_manual = Morsel.from_vectors(["test_col"], [vec1])
vec1_retrieved = morsel_manual.column(b"test_col")

print(f"\nAfter storing in Morsel and retrieving:")
print(f"  Type: {vec1_retrieved.__class__.__name__}")
print(f"  Object ID: {id(vec1_retrieved)}")
print(f"  Same object? {id(vec1) == id(vec1_retrieved)}")
print(f"  Data: {vec1_retrieved.to_pylist()}")
print(f"  Has comparison methods: {hasattr(vec1_retrieved, 'equals')}")

print("\n" + "=" * 80)
print("TEST 2: Planet data morsel creation and retrieval")
print("=" * 80)

morsel = planet_data.read()

id_vec_direct = morsel.column(b"id")
print(f"\nID vector retrieved directly from planet_data morsel:")
print(f"  Type: {id_vec_direct.__class__.__name__}")
print(f"  Object ID: {id(id_vec_direct)}")
print(f"  Data: {id_vec_direct.to_pylist()}")
print(f"  Has comparison methods: {hasattr(id_vec_direct, 'equals')}")

print("\n" + "=" * 80)
print("TEST 3: Trace Vector.from_arrow behavior")
print("=" * 80)

import pyarrow as pa

# Create an Arrow array and convert it
arrow_array = pa.array([1, 2, 3, 4, 5], type=pa.int64())
print(f"\nArrow array created: type={arrow_array.type}")

from opteryx.compiled.draken.vectors.vector import Vector

vec_from_arrow = Vector.from_arrow(arrow_array)
print(f"Vector.from_arrow result:")
print(f"  Type: {vec_from_arrow.__class__.__name__}")
print(f"  Object ID: {id(vec_from_arrow)}")
print(f"  Data: {vec_from_arrow.to_pylist()}")
print(f"  Has comparison methods: {hasattr(vec_from_arrow, 'equals')}")

# Now try with smaller integers
arrow_array_small = pa.array([1, 2, 3, 4, 5], type=pa.int32())
print(f"\nArrow array created with int32: type={arrow_array_small.type}")

vec_from_arrow_small = Vector.from_arrow(arrow_array_small)
print(f"Vector.from_arrow result:")
print(f"  Type: {vec_from_arrow_small.__class__.__name__}")
print(f"  Object ID: {id(vec_from_arrow_small)}")
print(f"  Data: {vec_from_arrow_small.to_pylist()}")
print(f"  Has comparison methods: {hasattr(vec_from_arrow_small, 'equals')}")

print("\n" + "=" * 80)
print("TEST 4: Trace query execution with vector monitoring")
print("=" * 80)

from opteryx.expression.evaluator import evaluation

_original_eval_value = evaluation._eval_value

call_count = [0]


def _patched_eval_value(node, morsel):
    from opteryx.expression import NodeType

    result = _original_eval_value(node, morsel)

    if node.node_type == NodeType.IDENTIFIER and hasattr(node, "schema_column"):
        call_count[0] += 1
        print(f"\n[_eval_value call #{call_count[0]}] Column: {node.schema_column.name}")
        print(f"  Schema type: {node.schema_column.type}")
        print(f"  Result vector type: {result.__class__.__name__}")
        print(f"  Result object ID: {id(result)}")
        print(f"  Has equals: {hasattr(result, 'equals')}")
        print(f"  Has greater_than: {hasattr(result, 'greater_than')}")

        if hasattr(result, "to_pylist"):
            data = result.to_pylist()
            print(f"  Data sample: {data[:3]}...")

        # For IntegerVector, try to get internal ptr address
        if result.__class__.__name__ == "IntegerVector":
            print(f"  WARNING: IntegerVector detected (not Int64Vector)")
            print(f"  Vector ptr attribute: {hasattr(result, 'ptr')}")

    return result


evaluation._eval_value = _patched_eval_value

print("\nRunning query: SELECT id FROM $planets WHERE id > 5")

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
print("DEBUG SCRIPT COMPLETE")
print("=" * 80)
