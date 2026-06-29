"""
03_to_arrow.py — Export Vectors and Morsels to PyArrow via the Arrow C Data Interface.

Requires: pip install pyarrow

To run, execute:
    python 03_to_arrow.py
"""
import os, urllib.request

import pyarrow as pa
from rugo.parquet import read_parquet

_URL  = "https://raw.githubusercontent.com/mabel-dev/opteryx-core/main/testdata/astronauts/astronauts.parquet"
_FILE = "astronauts.parquet"

if not os.path.exists(_FILE):
    print(f"downloading {_URL} ...")
    urllib.request.urlretrieve(_URL, _FILE)

with read_parquet(_FILE, columns=["name", "space_flights", "space_flight_hours"]) as reader:
    morsel = next(iter(reader))

# ── Vector.to_arrow() ─────────────────────────────────────────────────────────
names_vec   = morsel.column("name")
flights_vec = morsel.column("space_flights")
hours_vec   = morsel.column("space_flight_hours")

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
