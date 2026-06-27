"""
rugo — reading and writing examples for all three formats (Parquet, CSV, JSONL).

Run from the repo root (after `make c`):
    PYTHON_GIL=0 PYENV_VERSION=3.14.5t pyenv exec python dev/rugo_example.py
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import opteryx  # only to produce a Morsel to write in the example

from rugo import parquet
from rugo.csv import write_csv
from rugo.jsonl import write_jsonl

PLANETS = "testdata/planets/planets.parquet"


# --------------------------------------------------------------------------- #
# Reading
# --------------------------------------------------------------------------- #

# Schema-only metadata (footer parse, no column data). Accepts a path OR bytes.
meta = parquet.read_metadata(PLANETS)
print("rows:", meta.num_rows)
print("columns:", [c.name for c in meta.schema_columns])

# Streaming read: one Morsel per row group. Source may be a filename or bytes.
# `columns` projects; `filters` prune whole row groups via footer statistics.
with parquet.read_parquet(PLANETS, columns=["id", "name", "gravity"]) as reader:
    for morsel in reader:
        print("morsel:", morsel.column_names, "rows:", len(morsel))
        print("  ids:", morsel.column(b"id").to_pylist()[:5])

# Predicate pushdown — row groups that cannot match are never decoded.
# Filters are (column, op, value); ops: =, ==, !=, <, <=, >, >=, in, not in.
with parquet.read_parquet(PLANETS, columns=["name"], filters=[("id", ">", 4)]) as reader:
    kept = [m.column(b"name").to_pylist() for m in reader]
    print("names where a row group with id>4 survived:", kept)

# A filter that prunes every row group decodes nothing:
with parquet.read_parquet(PLANETS, filters=[("id", ">", 10_000)]) as reader:
    print("fully pruned ->", list(reader))


# --------------------------------------------------------------------------- #
# Writing
# --------------------------------------------------------------------------- #

# Any Draken Morsel can be written. Here we get one from a query.
morsel = next(
    iter(
        opteryx.session().execute_to_morsels(
            "SELECT id, name, gravity FROM $planets"
        )
    )
)

# write_parquet returns the file as bytes (ZSTD by default; "none" to disable).
data = parquet.write_parquet(morsel, compression="zstd")
out_path = "/tmp/rugo_planets.parquet"
with open(out_path, "wb") as f:
    f.write(data)
print(f"wrote {len(data)} bytes -> {out_path}")

# Round-trip: read back what we just wrote.
with parquet.read_parquet(out_path) as reader:
    rt = next(iter(reader))
    print("round-trip names:", rt.column(b"name").to_pylist()[:5])


# --------------------------------------------------------------------------- #
# CSV and JSONL — same Morsel, different formats (all native, no pyarrow)
# --------------------------------------------------------------------------- #

small = next(
    iter(opteryx.session().execute_to_morsels("SELECT id, name, gravity FROM $planets LIMIT 3"))
)

# CSV: RFC 4180; header on by default, comma delimiter; nulls are empty fields.
csv_bytes = write_csv(small, delimiter=",", header=True)
print("\nCSV:\n" + csv_bytes.decode())

# JSONL: one JSON object per row.
jsonl_bytes = write_jsonl(small)
print("JSONL:\n" + jsonl_bytes.decode())
