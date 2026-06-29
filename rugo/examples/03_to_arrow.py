"""
03_to_arrow.py — Export Vectors and Morsels to PyArrow via the Arrow C Data Interface.

Requires: pip install pyarrow

Run from the repo root:
    python rugo/examples/03_to_arrow.py
"""
import os, sys
sys.path.insert(0, os.path.join(sys.path[0], "..", ".."))

import pyarrow as pa
from rugo.parquet_reader import read_parquet

with open("testdata/astronauts/astronauts.parquet", "rb") as f:
    data = f.read()

morsels = read_parquet(data, column_names=["name", "space_flights", "space_flight_hours"])
morsel = morsels[0]

# ── Vector.to_arrow() ─────────────────────────────────────────────────────────
# Dense numeric and string types go through the C++ Arrow C Data Interface exporter
# (draken/interop/draken_to_arrow.h) — no Python object boxing per value.
names_vec    = morsel.column("name")
flights_vec  = morsel.column("space_flights")
hours_vec    = morsel.column("space_flight_hours")

names_arrow   = names_vec.to_arrow()
flights_arrow = flights_vec.to_arrow()
hours_arrow   = hours_vec.to_arrow()

print(f"name                {names_arrow.type}   len={len(names_arrow)}")
print(f"space_flights       {flights_arrow.type}  len={len(flights_arrow)}")
print(f"space_flight_hours  {hours_arrow.type}  len={len(hours_arrow)}")
print()
print("first 3 names:", names_arrow[:3].to_pylist())
print("total flights:", flights_arrow.sum().as_py())

# ── Morsel.to_arrow() → pa.Table ──────────────────────────────────────────────
table = morsel.to_arrow()
print()
print(table.schema)
print()
print(table.slice(0, 3).to_pydict())
