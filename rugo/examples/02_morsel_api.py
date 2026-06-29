"""
02_morsel_api.py — Morsel schema accessor, row iterator, column access.

To run, execute:
    python 02_morsel_api.py
"""
import os, urllib.request

from rugo.parquet import read_parquet

_URL  = "https://raw.githubusercontent.com/mabel-dev/opteryx-core/main/testdata/astronauts/astronauts.parquet"
_FILE = "astronauts.parquet"

if not os.path.exists(_FILE):
    print(f"downloading {_URL} ...")
    urllib.request.urlretrieve(_URL, _FILE)

with read_parquet(_FILE, columns=["name", "space_flights", "space_flight_hours"]) as reader:
    morsel = next(iter(reader))

# ── schema accessor: {column_name: DrakenType} ────────────────────────────────
print("schema:")
for col_name, draken_type in morsel.schema.items():
    print(f"  {col_name:25s}  {draken_type.name}")

# ── column access by name (str or bytes) ──────────────────────────────────────
print()
names_vec = morsel.column("name")
print(f"column('name')    type={names_vec.type.name}  len={len(names_vec)}")
print(f"  first 3: {names_vec.to_pylist()[:3]}")

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
