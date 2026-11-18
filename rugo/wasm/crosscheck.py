"""Compare the wasm footer JSON (stdin) with native rugo on the same file.
Usage: node -e '...' | python rugo/wasm/crosscheck.py <file.parquet>"""
import json, mmap, sys
import rugo.parquet as p
from rugo.rugo_native import read_rowgroup_stats

path = sys.argv[1]
w = json.load(sys.stdin)
m = p.read_metadata(path)
assert w["num_rows"] == m.num_rows and w["num_row_groups"] == m.num_row_groups
native_schema = [(c["name"], c.get("physical_type"), c.get("logical_type")) for c in m.schema_columns] \
    if isinstance(m.schema_columns[0], dict) else None
with open(path, "rb") as f:
    rgs = read_rowgroup_stats(memoryview(mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)))
assert len(rgs) == len(w["row_groups"])
n = 0
for a, b in zip(rgs, w["row_groups"]):
    assert len(a["columns"]) == len(b["columns"])
    for ca, cb in zip(a["columns"], b["columns"]):
        for k in ("name", "physical_type", "logical_type", "null_count"):
            assert ca[k] == cb[k], (k, ca[k], cb[k])
        n += 1
print(f"OK: {m.num_rows} rows, {m.num_row_groups} row groups, {n} column chunks match native"
      + ("" if native_schema is None else f", schema {len(native_schema)} cols"))
