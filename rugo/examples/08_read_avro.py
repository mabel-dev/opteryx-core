"""
08_read_avro.py — Read an Avro file with rugo: header, projection into nested records,
nested JSON, and reading with a reader schema.

The sample is the 4,630 space launches from testdata/flat/space_missions, written as
Avro (deflate) by fastavro — see dev/generate_avro_sample.py.

To run, execute:
    python 08_read_avro.py
"""
import collections
import os
import sys
import urllib.request

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from rugo.avro import read_avro, read_metadata

_LOCAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "testdata", "avro", "space_missions.avro")
_URL = "https://raw.githubusercontent.com/mabel-dev/opteryx-core/main/testdata/avro/space_missions.avro"
_FILE = _LOCAL if os.path.exists(_LOCAL) else "space_missions.avro"

if not os.path.exists(_FILE):
    print(f"downloading {_URL} ...")
    urllib.request.urlretrieve(_URL, _FILE)

# ── Header (no block read) ────────────────────────────────────────────────────
meta = read_metadata(_FILE)
print(f"codec: {meta.codec}")
print("schema:")
for field in meta.schema["fields"]:
    print(f"  {field['name']:15s}  {field['type']}")

# ── Read every column ─────────────────────────────────────────────────────────
print()
rows = 0
with read_avro(_FILE) as reader:
    for morsel in reader:
        rows += morsel.num_rows
        print(f"morsel: {morsel.num_rows} rows, {morsel.num_columns} columns")
        for name in morsel.column_names:
            vec = morsel.column(name)
            print(f"  {name.decode():15s}  {vec.type.name:12s}  {vec.to_pylist()[0]!r}")
print(f"total rows: {rows}")

# ── Projection, including fields inside the nested Rocket record ──────────────
# A whole record (Rocket, above) comes back as JSON text; a dotted name reads one
# field of it as an ordinary column. Rocket.Status is an Avro enum: one entry per
# symbol, a position per row.
print()
launches = collections.Counter()
active = collections.Counter()
with read_avro(_FILE, columns=["Company", "Rocket.Name", "Rocket.Status"]) as reader:
    for morsel in reader:
        companies = morsel.column(b"Company").to_pylist()
        statuses = morsel.column(b"Rocket.Status").to_pylist()
        for company, status in zip(companies, statuses):
            launches[company] += 1
            active[company] += status == "Active"
print("most launches (rockets still active):")
for company, n in launches.most_common(5):
    print(f"  {company:20s} {n:5d}  ({active[company]} on active rockets)")

# ── Timestamps and nulls ──────────────────────────────────────────────────────
print()
with read_avro(_FILE, columns=["Lauched_at", "Price"]) as reader:
    when, price = [], []
    for morsel in reader:
        when.extend(morsel.column(b"Lauched_at").to_pylist())
        price.extend(morsel.column(b"Price").to_pylist())
dated = [w for w in when if w is not None]
priced = [p for p in price if p is not None]
print(f"first launch {min(dated)}, last launch {max(dated)}")
print(f"{len(priced)} of {len(price)} launches have a price; mean {sum(priced) / len(priced):.1f}M")

# ── Reading with a reader schema ──────────────────────────────────────────────
# The reader schema names what you want, not what the file has: fields can be
# reordered or dropped, a field the file lacks is its default (a constant column),
# and an int / long / float can be widened.
print()
reader_schema = {
    "type": "record",
    "name": "Launch",
    "fields": [
        {"name": "Mission", "type": "string"},
        {"name": "Mission_Status", "type": "string"},
        {"name": "Source", "type": "string", "default": "space_missions"},
        {"name": "Rocket", "type": {"type": "record", "name": "Rocket", "fields": [
            {"name": "Status", "type": {"type": "enum", "name": "RocketStatus", "symbols": ["Retired", "Active"]}},
            {"name": "Name", "type": "string"},
            {"name": "Stages", "type": ["null", "int"], "default": None},
        ]}},
    ],
}
with read_avro(_FILE, reader_schema=reader_schema) as reader:
    morsel = next(iter(reader))
    for name in morsel.column_names:
        vec = morsel.column(name)
        print(f"  {name.decode():15s}  {vec.type.name:12s}  {vec.to_pylist()[0]!r}")
    print(f"  Source is constant: {morsel.column(b'Source').is_constant}")
