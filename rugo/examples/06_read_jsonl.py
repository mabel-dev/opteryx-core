"""
06_read_jsonl.py — Read JSONL with rugo: schema inference, projection, predicates.

To run, execute:
    python 06_read_jsonl.py
"""
from rugo.jsonl import read_jsonl, read_metadata

data = (
    b'{"id": 1, "name": "Alice", "score": 9.5, "active": true}\n'
    b'{"id": 2, "name": "Bob",   "score": 8.1, "active": false}\n'
    b'{"id": 3, "name": "Carol", "score": 7.7, "active": true}\n'
    b'{"id": 4, "name": "Dave",  "score": 6.2, "active": false}\n'
)

# ── Schema inference ──────────────────────────────────────────────────────────
meta = read_metadata(data)
print("inferred schema:")
for col in meta.schema_columns:
    print(f"  {col['name']:10s}  {col['type']:10s}  nullable={col['nullable']}")

# ── Read all columns ──────────────────────────────────────────────────────────
with read_jsonl(data) as reader:
    for morsel in reader:
        print(f"\n{morsel.num_rows} rows:")
        for name in morsel.column_names:
            vec = morsel.column(name)
            print(f"  {name.decode()}: {vec.to_pylist()}")

# ── Column projection ─────────────────────────────────────────────────────────
with read_jsonl(data, columns=["name", "score"]) as reader:
    for morsel in reader:
        print("\nprojection [name, score]:")
        for name in morsel.column_names:
            vec = morsel.column(name)
            print(f"  {name.decode()}: {vec.to_pylist()}")

# ── Predicate pushdown ────────────────────────────────────────────────────────
with read_jsonl(data, columns=["name"], predicates=[("score", ">", 8.0)]) as reader:
    for morsel in reader:
        vec = morsel.column(b"name")
        print(f"\nname WHERE score > 8.0:\n  {vec.to_pylist()}")
