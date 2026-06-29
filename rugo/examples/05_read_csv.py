"""
05_read_csv.py — Read CSV with rugo: projection, predicate pushdown, TSV, nulls.

Run from the repo root:
    python rugo/examples/05_read_csv.py

Note: requires rugo built as part of opteryx-core (the draken extension must be
importable on sys.path so the CSV reader can resolve its symbols).
"""
import os, sys
sys.path.insert(0, os.path.join(sys.path[0], "..", ".."))

from rugo.csv import read_csv

# ── Basic read ────────────────────────────────────────────────────────────────
data = (
    b"id,name,score\n"
    b"1,Alice,95\n"
    b"2,Bob,82\n"
    b"3,Carol,77\n"
    b"4,Dave,45\n"
)

result = read_csv(data)
assert result["success"]
print("all columns:")
for name, vec in zip(result["column_names"], result["columns"]):
    print(f"  {name}: {vec.to_pylist()}")

# ── Column projection ─────────────────────────────────────────────────────────
result = read_csv(data, columns=["name", "score"])
assert result["success"]
print("\nprojection [name, score]:")
for name, vec in zip(result["column_names"], result["columns"]):
    print(f"  {name}: {vec.to_pylist()}")

# ── Predicate pushdown ────────────────────────────────────────────────────────
result = read_csv(data, columns=["name"], predicates=[("score", ">", 60)])
assert result["success"]
print("\nname WHERE score > 60:")
print(f"  {result['columns'][0].to_pylist()}")

# ── TSV ───────────────────────────────────────────────────────────────────────
tsv = (
    b"id\tname\tscore\n"
    b"1\tAlice\t95\n"
    b"2\tBob\t82\n"
)
result = read_csv(tsv, delimiter="\t")
assert result["success"]
print("\nTSV column names:", result["column_names"])

# ── Null handling — empty unquoted fields ─────────────────────────────────────
data_nulls = (
    b"id,name,score\n"
    b"1,Alice,95\n"
    b"2,,82\n"
    b"3,Carol,\n"
)
result = read_csv(data_nulls)
assert result["success"]
print("\nnull handling:")
for name, vec in zip(result["column_names"], result["columns"]):
    print(f"  {name}: {vec.to_pylist()}")
