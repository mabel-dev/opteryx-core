"""
01_read_parquet.py — Read a Parquet file with rugo.

Run from any directory:
    python 01_read_parquet.py
"""
import os, urllib.request

from rugo.parquet_reader import read_metadata, read_parquet

_URL  = "https://raw.githubusercontent.com/mabel-dev/opteryx-core/main/testdata/astronauts/astronauts.parquet"
_FILE = "astronauts.parquet"

if not os.path.exists(_FILE):
    print(f"downloading {_URL} ...")
    urllib.request.urlretrieve(_URL, _FILE)

PARQUET_FILE = _FILE

# ── Schema (footer only — no column data read) ────────────────────────────────
meta = read_metadata(PARQUET_FILE)
print(f"rows: {meta.num_rows}")
print()
print("schema:")
for col in meta.schema_columns:
    nullable = "nullable" if col.nullable else "required"
    print(f"  {col.name:30s}  {col.physical_type:12s}  {col.logical_type or '':15s}  {nullable}")

# ── Read all columns ──────────────────────────────────────────────────────────
print()
with open(PARQUET_FILE, "rb") as f:
    data = f.read()

morsels = read_parquet(data)
for morsel in morsels:
    print(f"morsel: {morsel.num_rows} rows, {morsel.num_columns} columns")

# ── Column projection ─────────────────────────────────────────────────────────
print()
morsels = read_parquet(data, column_names=["name", "space_flights", "space_flight_hours"])
morsel = morsels[0]
for col_name in morsel.column_names:
    vec = morsel.column(col_name)
    label = col_name.decode() if isinstance(col_name, bytes) else col_name
    print(f"{label:25s}  type={vec.type.name:12s}  first 3: {vec.to_pylist()[:3]}")
