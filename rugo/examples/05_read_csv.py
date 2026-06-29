"""
05_read_csv.py — Read CSV with rugo: projection, predicate pushdown, TSV, nulls.

To run, execute:
    python 05_read_csv.py
"""
from rugo.csv import read_csv, read_metadata

# ── Basic read ────────────────────────────────────────────────────────────────
data = (
    b"id,name,score\n"
    b"1,Alice,95\n"
    b"2,Bob,82\n"
    b"3,Carol,77\n"
    b"4,Dave,45\n"
)

with read_csv(data) as reader:
    for morsel in reader:
        print("all columns:")
        for name in morsel.column_names:
            vec = morsel.column(name)
            print(f"  {name.decode()}: {vec.to_pylist()}")

# ── Column projection ─────────────────────────────────────────────────────────
with read_csv(data, columns=["name", "score"]) as reader:
    for morsel in reader:
        print("\nprojection [name, score]:")
        for name in morsel.column_names:
            vec = morsel.column(name)
            print(f"  {name.decode()}: {vec.to_pylist()}")

# ── Predicate pushdown ────────────────────────────────────────────────────────
with read_csv(data, columns=["name"], predicates=[("score", ">", 60)]) as reader:
    for morsel in reader:
        vec = morsel.column(b"name")
        print(f"\nname WHERE score > 60:\n  {vec.to_pylist()}")

# ── TSV ───────────────────────────────────────────────────────────────────────
tsv = (
    b"id\tname\tscore\n"
    b"1\tAlice\t95\n"
    b"2\tBob\t82\n"
)
with read_csv(tsv, delimiter="\t") as reader:
    for morsel in reader:
        print(f"\nTSV column names: {[n.decode() for n in morsel.column_names]}")

# ── Null handling — empty unquoted fields ─────────────────────────────────────
data_nulls = (
    b"id,name,score\n"
    b"1,Alice,95\n"
    b"2,,82\n"
    b"3,Carol,\n"
)
with read_csv(data_nulls) as reader:
    for morsel in reader:
        print("\nnull handling:")
        for name in morsel.column_names:
            vec = morsel.column(name)
            print(f"  {name.decode()}: {vec.to_pylist()}")
