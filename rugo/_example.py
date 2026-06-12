import os
import sys

from numpy.random.mtrand import beta

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.parquet_reader import read_metadata, read_parquet

# Schema-only metadata (footer parse, no column data)
meta = read_metadata("testdata/planets/planets.parquet")
# -> {"num_rows": int, "schema_columns": [ {name, physical_type, logical_type, nullable}, ... ]}

# Decode columns from an in-memory buffer
with open("testdata/planets/planets.parquet", "rb") as f:
    morsels = read_parquet(f.read(), column_names=["id", "name"])


*** why does read_metadata accept a filename but read_parquet does not
*** I think the interface should be more like this (I'm open to discuss)
"""python
from rugo import parquet

filename = "testdata/planets/planets.parquet"
with parquet.read_parquet(filename, column_names=[], filters=[]) as f:
    for morsel in f:
        print(morsel)

from rugo import json

filename = "testdata/planets/planets.json"
with json.read_json(filename, column_names=[], filters=[]) as f:
    for morsel in f:
        print(morsel)
"""
