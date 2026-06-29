"""
06_read_jsonl.py — Read JSONL with rugo: schema inference, projection, predicates.

Run from any directory:
    python rugo/examples/06_read_jsonl.py

"""
import os

from rugo.jsonl import read_jsonl, get_jsonl_schema

data = (
    b'{"id": 1, "name": "Alice", "score": 9.5, "active": true}\n'
    b'{"id": 2, "name": "Bob",   "score": 8.1, "active": false}\n'
    b'{"id": 3, "name": "Carol", "score": 7.7, "active": true}\n'
    b'{"id": 4, "name": "Dave",  "score": 6.2, "active": false}\n'
)

# ── Schema inference ──────────────────────────────────────────────────────────
schema = get_jsonl_schema(data)
print("inferred schema:")
for col in schema["columns"]:
    print(f"  {col['name']:10s}  {col['type']:10s}  nullable={col['nullable']}")

# ── Read all columns ──────────────────────────────────────────────────────────
result = read_jsonl(data)
assert result["success"]
print(f"\n{result['num_rows']} rows, columns: {result['column_names']}")
for name, vec in zip(result["column_names"], result["columns"]):
    print(f"  {name}: {vec.to_pylist()}")

# ── Column projection ─────────────────────────────────────────────────────────
result = read_jsonl(data, columns=["name", "score"])
assert result["success"]
print("\nprojection [name, score]:")
for name, vec in zip(result["column_names"], result["columns"]):
    print(f"  {name}: {vec.to_pylist()}")

# ── Predicate pushdown ────────────────────────────────────────────────────────
# Predicate column (score) need not appear in the projection list.
result = read_jsonl(data, columns=["name"], predicates=[("score", ">", 8.0)])
assert result["success"]
print("\nname WHERE score > 8.0:")
print(f"  {result['columns'][0].to_pylist()}")
