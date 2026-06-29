"""
02_morsel_api.py — Morsel schema accessor, row iterator, column access.

Run from any directory:
    python 02_morsel_api.py
"""
import os, urllib.request

from rugo.parquet_reader import read_parquet

_URL  = "https://raw.githubusercontent.com/mabel-dev/opteryx-core/main/testdata/astronauts/astronauts.parquet"
_FILE = "astronauts.parquet"

if not os.path.exists(_FILE):
    print(f"downloading {_URL} ...")
    urllib.request.urlretrieve(_URL, _FILE)

with open(_FILE, "rb") as f:
    data = f.read()

morsels = read_parquet(data, column_names=["name", "space_flights", "space_flight_hours"])
morsel = morsels[0]

# ── schema accessor: {column_name: DrakenType} ────────────────────────────────
print("schema:")
for col_name, draken_type in morsel.schema.items():
    print(f"  {col_name:25s}  {draken_type.name}")

# ── column access by name (str or bytes) ──────────────────────────────────────
print()
names_vec = morsel.column("name")
print(f"column('name')    type={names_vec.type.name}  len={len(names_vec)}")
print(f"  first 3: {names_vec.to_pylist()[:3]}")

# bytes key also works
flights_vec = morsel.column(b"space_flights")
print(f"column(b'space_flights')  type={flights_vec.type.name}")
print(f"  first 3: {flights_vec.to_pylist()[:3]}")

# ── named-tuple row iterator ──────────────────────────────────────────────────
print()
print("first 3 rows as named tuples:")
for i, row in enumerate(morsel):
    print(f"  {row}")
    if i >= 2:
        break
