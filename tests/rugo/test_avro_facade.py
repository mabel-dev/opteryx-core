# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
rugo.avro (the facade over rugo_native.read_avro) against the committed sample
testdata/avro/space_missions.avro, whose oracle is the parquet file it was built from
(dev/generate_avro_sample.py): the same 4,630 launches, value for value.
"""

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from rugo import avro, parquet  # noqa: E402

AVRO = str(REPO_ROOT / "testdata" / "avro" / "space_missions.avro")
PARQUET = str(REPO_ROOT / "testdata" / "flat" / "space_missions" / "space_missions.parquet")


def _columns(reader):
    out = {}
    with reader as r:
        for morsel in r:
            for name in morsel.column_names:
                out.setdefault(name.decode(), []).extend(morsel.column(name).to_pylist())
    return out


def test_sample_matches_its_parquet_source():
    want = _columns(parquet.read_parquet(PARQUET))
    got = _columns(avro.read_avro(AVRO, columns=[
        "Company", "Location", "Price", "Lauched_at", "Rocket.Name", "Rocket.Status", "Mission", "Mission_Status"]))
    for name in ("Company", "Location", "Price", "Lauched_at", "Mission", "Mission_Status"):
        assert got[name] == want[name], name
    assert got["Rocket.Name"] == want["Rocket"]
    assert got["Rocket.Status"] == want["Rocket_Status"]


def test_path_and_bytes_read_the_same():
    with open(AVRO, "rb") as f:
        data = f.read()
    assert _columns(avro.read_avro(data)) == _columns(avro.read_avro(AVRO))


def test_whole_record_is_json():
    got = _columns(avro.read_avro(AVRO, columns=["Rocket"]))
    first = json.loads(got["Rocket"][0])
    assert first == {"Name": "Sputnik 8K71PS", "Status": "Retired"}


def test_metadata():
    meta = avro.read_metadata(AVRO)
    assert meta.codec == "deflate"
    assert meta.columns == ["Company", "Location", "Price", "Lauched_at", "Rocket", "Mission", "Mission_Status"]


def test_reader_schema_as_dict_or_text():
    schema = {"type": "record", "name": "Launch", "fields": [
        {"name": "Mission", "type": "string"},
        {"name": "Source", "type": "string", "default": "sample"}]}
    a = _columns(avro.read_avro(AVRO, reader_schema=schema))
    b = _columns(avro.read_avro(AVRO, reader_schema=json.dumps(schema)))
    assert a == b
    assert set(a["Source"]) == {"sample"} and len(a["Source"]) == 4630
