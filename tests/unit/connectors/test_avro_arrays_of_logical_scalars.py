# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""An Avro array of a logical type or an enum is one JSON array per row
(docs/UUID_AND_NESTED_COLUMNS_DESIGN.md ND2). Draken's ARRAY has no child for a date, a decimal, a
uuid, an enum ..., and the reader used to refuse the column; it now renders it as the shared JSON
renderer renders the same value anywhere else (dates / times / timestamps quoted ISO, decimals bare
numbers, uuids quoted lowercase text, enum symbols quoted).

fastavro writes the files (an independent writer; test-only). Every expected value is the value the
test wrote.
"""

import datetime
import decimal
import json
import os
import sys
import uuid
from decimal import Decimal

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import fastavro

import opteryx

pytestmark = pytest.mark.usefixtures("allow_local_reads")

T0 = datetime.datetime(2026, 3, 1, 12, 30, 45, 123456, tzinfo=datetime.timezone.utc)
U1 = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
U2 = uuid.UUID("00000000-0000-0000-0000-00000000000f")


def _array(items):
    return ["null", {"type": "array", "items": ["null", items]}]


SCHEMA = {
    "type": "record",
    "name": "R",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "dates", "type": _array({"type": "int", "logicalType": "date"})},
        {"name": "times", "type": _array({"type": "long", "logicalType": "time-micros"})},
        {"name": "stamps", "type": _array({"type": "long", "logicalType": "timestamp-micros"})},
        {"name": "decs", "type": _array({"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2})},
        {"name": "uuids", "type": _array({"type": "string", "logicalType": "uuid"})},
        {"name": "enums", "type": _array({"type": "enum", "name": "Colour", "symbols": ["RED", "GREEN"]})},
        # a plain scalar array stays an ARRAY
        {"name": "plain", "type": _array("long")},
    ],
}
ROWS = [
    {
        "id": 1,
        "dates": [datetime.date(2026, 1, 2), None, datetime.date(1970, 1, 1)],
        "times": [datetime.time(12, 30, 45, 123456), None],
        "stamps": [T0, None],
        "decs": [Decimal("12.50"), Decimal("-0.01"), None],
        "uuids": [U1, None, U2],
        "enums": ["RED", None, "GREEN"],
        "plain": [1, None, 3],
    },
    {"id": 2, "dates": None, "times": None, "stamps": None, "decs": None, "uuids": None, "enums": None, "plain": None},
    {"id": 3, "dates": [], "times": [], "stamps": [], "decs": [], "uuids": [], "enums": [], "plain": []},
]
EXPECTED = {
    "dates": [["2026-01-02", None, "1970-01-01"], None, []],
    "times": [["12:30:45.123456", None], None, []],
    "stamps": [["2026-03-01T12:30:45.123456+00:00", None], None, []],
    "decs": [[Decimal("12.5"), Decimal("-0.01"), None], None, []],
    "uuids": [[str(U1), None, str(U2)], None, []],
    "enums": [["RED", None, "GREEN"], None, []],
}


@pytest.fixture(scope="module")
def path(tmp_path_factory):
    p = tmp_path_factory.mktemp("avro_arrays") / "a.avro"
    with open(p, "wb") as f:
        fastavro.writer(f, SCHEMA, ROWS)
    return p


def _read(path, column):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(f"SELECT id, {column} FROM READ_AVRO('{path}') ORDER BY id"):
        out.extend(morsel.column(column).to_pylist())
    return out


@pytest.mark.parametrize("column", list(EXPECTED))
def test_an_array_of_a_logical_type_is_a_json_array(path, column):
    got = [None if v is None else json.loads(v, parse_float=Decimal, parse_int=Decimal) for v in _read(path, column)]
    assert got == EXPECTED[column]


def test_an_array_of_a_plain_scalar_is_still_an_array(path):
    assert _read(path, "plain") == [[1, None, 3], None, []]


def test_the_folder_dataset_reads_it_too(path, tmp_path_factory):
    folder = tmp_path_factory.mktemp("folder")
    os.link(path, folder / "a.avro")
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(f"SELECT id, enums FROM '{folder}' ORDER BY id"):
        rows.extend(zip(*[morsel.column(n).to_pylist() for n in morsel.column_names]))
    assert [(i, None if v is None else json.loads(v)) for i, v in rows] == list(zip([1, 2, 3], EXPECTED["enums"]))
