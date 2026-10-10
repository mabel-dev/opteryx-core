"""Build testdata/avro/space_missions.avro from testdata/flat/space_missions (dev tooling).

The same 4,630 launches as the parquet sample, written as an Avro object container
file by fastavro (an independent writer — rugo has no Avro writer) with the deflate
codec. The schema adds what a flat parquet file does not show off:

  - Rocket is a nested record {Name, Status}, Status an enum (Active / Retired)
  - Price and Lauched_at are nullable unions, Lauched_at a timestamp-micros

Run from the repo root:

    python dev/generate_avro_sample.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import fastavro

from rugo import parquet

SRC = "testdata/flat/space_missions/space_missions.parquet"
OUT = "testdata/avro/space_missions.avro"

SCHEMA = {
    "type": "record",
    "name": "Launch",
    "namespace": "opteryx.sample",
    "fields": [
        {"name": "Company", "type": "string"},
        {"name": "Location", "type": "string"},
        {"name": "Price", "type": ["null", "double"]},
        {"name": "Lauched_at", "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]},
        {"name": "Rocket", "type": {"type": "record", "name": "Rocket", "fields": [
            {"name": "Name", "type": "string"},
            {"name": "Status", "type": {"type": "enum", "name": "RocketStatus", "symbols": ["Active", "Retired"]}},
        ]}},
        {"name": "Mission", "type": "string"},
        {"name": "Mission_Status", "type": "string"},
    ],
}


def main():
    columns = {}
    with parquet.read_parquet(SRC) as reader:
        for morsel in reader:
            for name in morsel.column_names:
                columns.setdefault(name.decode(), []).extend(morsel.column(name).to_pylist())
    n = len(columns["Company"])
    records = (
        {
            "Company": columns["Company"][i],
            "Location": columns["Location"][i],
            "Price": columns["Price"][i],
            "Lauched_at": columns["Lauched_at"][i],
            "Rocket": {"Name": columns["Rocket"][i], "Status": columns["Rocket_Status"][i]},
            "Mission": columns["Mission"][i],
            "Mission_Status": columns["Mission_Status"][i],
        }
        for i in range(n)
    )
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "wb") as f:
        fastavro.writer(f, SCHEMA, records, codec="deflate")
    print(f"wrote {n} records to {OUT} ({os.path.getsize(OUT)} bytes)")


if __name__ == "__main__":
    main()
