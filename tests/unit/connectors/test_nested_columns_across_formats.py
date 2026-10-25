# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""One set of nested values, read five ways, must come back the same
(docs/UUID_AND_NESTED_COLUMNS_DESIGN.md §3.1, ND1/ND5): READ_PARQUET, READ_AVRO, READ_ORC, and a
folder of .orc / a folder of .avro read as a dataset.

A STRUCT or MAP is one JSON object per row (NVARCHAR); a LIST of a plain scalar (or of lists of one)
is an ARRAY. Every
expected value comes from one Python oracle over the values the test wrote, so a format cannot be
"right" by agreeing with another format's mistake. A LIST of lists of a plain scalar is a nested ARRAY
in every format, as the Parquet scan builds it; a LIST with a STRUCT / MAP at the bottom is a JSON array.

pyarrow writes the Parquet and ORC files, fastavro the Avro: independent writers (test-only,
CLAUDE.md §4).
"""

import base64
import datetime
import decimal
import json
import math
import os
import sys
import time
from decimal import Decimal

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

os.environ["TZ"] = "UTC"
time.tzset()

import fastavro
import pyarrow as pa
import pyarrow.orc as po
import pyarrow.parquet as pq

import opteryx

# ── the data ────────────────────────────────────────────────────────────────

STRUCT = pa.struct(
    [
        ("a", pa.int64()),
        ("b", pa.string()),
        ("c", pa.float64()),
        ("d", pa.date32()),
        ("p", pa.decimal128(9, 2)),
        ("bin", pa.binary()),
        ("ok", pa.bool_()),
        ("t", pa.timestamp("us")),
    ]
)
ITEM = pa.struct([("k", pa.int64()), ("v", pa.string())])
TYPES = {
    "id": pa.int64(),
    "s": STRUCT,
    "m": pa.map_(pa.string(), pa.int64()),
    "ls": pa.list_(pa.int64()),
    "ln": pa.list_(pa.string()),
    "lst": pa.list_(ITEM),
    "ll": pa.list_(pa.list_(pa.int64())),
}

T0 = datetime.datetime(2026, 3, 1, 12, 30, 45, 123456)
ROWS = [
    {
        "id": 1,
        "s": {"a": 1, "b": "x", "c": 1.5, "d": datetime.date(2026, 1, 2), "p": Decimal("12.50"), "bin": b"\x00\xffab", "ok": True, "t": T0},
        "m": [("a", 1), ("é", 2), ("日本", 3)],
        "ls": [1, 2, 3],
        "ln": ["a", "b"],
        "lst": [{"k": 1, "v": "one"}, {"k": 2, "v": None}],
        "ll": [[1, 2], [3]],
    },
    {"id": 2, "s": None, "m": None, "ls": None, "ln": None, "lst": None, "ll": None},
    {
        "id": 3,
        "s": {"a": None, "b": 'quo"te\n', "c": float("nan"), "d": None, "p": None, "bin": b"", "ok": False, "t": None},
        "m": [],
        "ls": [],
        "ln": [],
        "lst": [],
        "ll": [],
    },
    {
        "id": 4,
        "s": {"a": -4, "b": "", "c": -0.25, "d": datetime.date(1970, 1, 1), "p": Decimal("-0.01"), "bin": None, "ok": None, "t": T0},
        "m": [("z", None), ("y", 7)],
        "ls": [4, None, 6],
        "ln": [None, "bb", ""],
        "lst": [None, {"k": None, "v": "n"}],
        "ll": [None, [], [None, 5]],
    },
    {
        "id": 5,
        "s": {"a": 5, "b": "five", "c": float("inf"), "d": datetime.date(2000, 2, 29), "p": Decimal("0.00"), "bin": b"z", "ok": True, "t": datetime.datetime(1999, 12, 31, 23, 59, 59)},
        "m": [("k", 5)],
        "ls": [5],
        "ln": ["five"],
        "lst": [{"k": 5, "v": "five"}],
        "ll": [[5]],
    },
]


# ── the oracle ──────────────────────────────────────────────────────────────


def _ts_text(dt):
    text = dt.strftime("%Y-%m-%dT%H:%M:%S")
    if dt.microsecond:
        text += ".%06d" % dt.microsecond
    return text + "+00:00"


def tree(value, typ):
    """The Python structure the JSON text must parse to (numbers as Decimal)."""
    if value is None:
        return None
    if pa.types.is_struct(typ):
        return {typ.field(i).name: tree(value[typ.field(i).name], typ.field(i).type) for i in range(typ.num_fields)}
    if pa.types.is_map(typ):
        return {k: tree(v, typ.item_type) for k, v in value}
    if pa.types.is_list(typ):
        return [tree(v, typ.value_type) for v in value]
    if pa.types.is_boolean(typ):
        return bool(value)
    if pa.types.is_integer(typ):
        return Decimal(int(value))
    if pa.types.is_floating(typ):
        return None if math.isnan(value) or math.isinf(value) else Decimal(repr(float(value)))
    if pa.types.is_decimal(typ):
        return Decimal(value)
    if pa.types.is_string(typ):
        return value
    if pa.types.is_binary(typ):
        return base64.b64encode(value).decode()
    if pa.types.is_date32(typ):
        return value.isoformat()
    if pa.types.is_timestamp(typ):
        return _ts_text(value)
    raise AssertionError(f"oracle has no rule for {typ}")


def _holds_groups(typ):
    """A LIST whose elements, however deeply listed, are STRUCT / MAP: JSON. A LIST of lists of a plain
    scalar is a nested ARRAY, as the Parquet scan builds it."""
    while pa.types.is_list(typ):
        typ = typ.value_type
    return pa.types.is_struct(typ) or pa.types.is_map(typ)


def _is_json(typ):
    return pa.types.is_struct(typ) or pa.types.is_map(typ) or (pa.types.is_list(typ) and _holds_groups(typ))


def expected(column):
    """[(id, expected)] where expected is a JSON tree for a JSON column, a list for an ARRAY."""
    typ = TYPES[column]
    as_json = _is_json(typ)
    out = []
    for row in ROWS:
        value = row[column]
        if as_json:
            out.append((row["id"], tree(value, typ)))
        else:
            out.append((row["id"], value))
    return out


def _table():
    return pa.table({name: pa.array([r[name] for r in ROWS], typ) for name, typ in TYPES.items()})


# ── writers ─────────────────────────────────────────────────────────────────


def _avro_schema():
    def opt(t):
        return ["null", t]

    struct = {
        "type": "record",
        "name": "S",
        "fields": [
            {"name": "a", "type": opt("long")},
            {"name": "b", "type": opt("string")},
            {"name": "c", "type": opt("double")},
            {"name": "d", "type": opt({"type": "int", "logicalType": "date"})},
            {"name": "p", "type": opt({"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2})},
            {"name": "bin", "type": opt("bytes")},
            {"name": "ok", "type": opt("boolean")},
            {"name": "t", "type": opt({"type": "long", "logicalType": "timestamp-micros"})},
        ],
    }
    item = {"type": "record", "name": "I", "fields": [{"name": "k", "type": opt("long")}, {"name": "v", "type": opt("string")}]}
    return {
        "type": "record",
        "name": "R",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "s", "type": opt(struct)},
            {"name": "m", "type": opt({"type": "map", "values": opt("long")})},
            {"name": "ls", "type": opt({"type": "array", "items": opt("long")})},
            {"name": "ln", "type": opt({"type": "array", "items": opt("string")})},
            {"name": "lst", "type": opt({"type": "array", "items": opt(item)})},
            {"name": "ll", "type": opt({"type": "array", "items": opt({"type": "array", "items": opt("long")})})},
        ],
    }


def _avro_record(row):
    out = dict(row)
    if row["m"] is not None:
        out["m"] = dict(row["m"])
    if row["s"] is not None:
        out["s"] = dict(row["s"])
        c = out["s"]["c"]
        out["s"]["c"] = c
    return out


@pytest.fixture(scope="module")
def files(tmp_path_factory):
    root = tmp_path_factory.mktemp("nested")
    table = _table()
    pq.write_table(table, root / "t.parquet")
    with open(root / "t.orc", "wb") as f:
        po.write_table(table, f, compression="zlib")
    with open(root / "t.avro", "wb") as f:
        fastavro.writer(f, _avro_schema(), [_avro_record(r) for r in ROWS])
    for kind in ("orc", "avro"):
        (root / f"{kind}_dir").mkdir()
        os.link(root / f"t.{kind}", root / f"{kind}_dir" / f"t.{kind}")
    return root


# ── reading ─────────────────────────────────────────────────────────────────

SOURCES = {
    "READ_PARQUET": lambda root: f"READ_PARQUET('{root}/t.parquet')",
    "READ_AVRO": lambda root: f"READ_AVRO('{root}/t.avro')",
    "READ_ORC": lambda root: f"READ_ORC('{root}/t.orc')",
    "orc folder": lambda root: f"'{root}/orc_dir'",
    "avro folder": lambda root: f"'{root}/avro_dir'",
}
PARQUET_COLUMNS = set(c for c in TYPES if c != "id")


def _read(root, source, column):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(f"SELECT id, {column} FROM {SOURCES[source](root)} ORDER BY id"):
        rows.extend(zip(*[morsel.column(n).to_pylist() for n in morsel.column_names]))
    return rows


def _normalise(column, rows):
    if _is_json(TYPES[column]):
        return [(i, None if v is None else json.loads(v, parse_float=Decimal, parse_int=Decimal)) for i, v in rows]
    return [(i, v) for i, v in rows]


@pytest.mark.parametrize("column", [c for c in TYPES if c != "id"])
@pytest.mark.parametrize("source", list(SOURCES))
def test_every_format_reads_the_same_nested_value(files, source, column):
    got = _normalise(column, _read(files, source, column))
    assert got == expected(column)


def test_a_null_top_level_group_is_sql_null_and_a_null_inside_is_json_null(files):
    for source in ("READ_AVRO", "READ_ORC", "orc folder", "avro folder"):
        rows = dict(_read(files, source, "lst"))
        assert rows[2] is None                       # the NULL list is SQL NULL
        assert rows[3] == "[]"                       # an empty list is an empty array
        assert json.loads(rows[4]) == [None, {"k": None, "v": "n"}]   # NULLs nested inside are `null`


def test_an_orc_union_is_refused_naming_the_column(tmp_path):
    # pyarrow cannot write a union, so build the footer refusal through a MAP with non-string keys,
    # which a JSON object cannot hold.
    table = pa.table({"m": pa.array([[(1, 2)]], pa.map_(pa.int64(), pa.int64()))})
    with open(tmp_path / "k.orc", "wb") as f:
        po.write_table(table, f)
    with pytest.raises(Exception, match="column 'm'.*keys"):
        list(opteryx.session().execute_to_morsels(f"SELECT m FROM READ_ORC('{tmp_path}/k.orc')"))
