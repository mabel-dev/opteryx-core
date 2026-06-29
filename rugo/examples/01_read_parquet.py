"""
01_read_parquet.py — Read a Parquet file with rugo.

To run, execute:
    python 01_read_parquet.py
"""
import os, urllib.request

from rugo.parquet import read_metadata, read_parquet

_URL  = "https://raw.githubusercontent.com/mabel-dev/opteryx-core/main/testdata/astronauts/astronauts.parquet"
_FILE = "astronauts.parquet"

if not os.path.exists(_FILE):
    print(f"downloading {_URL} ...")
    urllib.request.urlretrieve(_URL, _FILE)

# ── Schema (footer only — no column data read) ────────────────────────────────
meta = read_metadata(_FILE)
print(f"rows: {meta.num_rows}")
print()
print("schema:")
for col in meta.schema_columns:
    nullable = "nullable" if col.nullable else "required"
    print(f"  {col.name:30s}  {col.physical_type:12s}  {col.logical_type or '':15s}  {nullable}")

# ── Read all columns ──────────────────────────────────────────────────────────
print()
with read_parquet(_FILE) as reader:
    for morsel in reader:
        print(f"morsel: {morsel.num_rows} rows, {morsel.num_columns} columns")

# ── Column projection ─────────────────────────────────────────────────────────
print()
with read_parquet(_FILE, columns=["name", "space_flights", "space_flight_hours"]) as reader:
    for morsel in reader:
        for col_name in morsel.column_names:
            vec = morsel.column(col_name)
            label = col_name.decode() if isinstance(col_name, bytes) else col_name
            print(f"{label:25s}  type={vec.type.name:12s}  first 3: {vec.to_pylist()[:3]}")
