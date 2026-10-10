# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Rugo's Avro reader with a READER schema (docs/AVRO_READER_DESIGN.md §19.3), against
fastavro's schema resolution as the oracle wherever fastavro has the feature:

  - fields match by field-id when both sides carry one (Iceberg), else by name
  - a reader field the file lacks is its default, or NULL — as a constant
  - promotions int->long/float/double, long->float/double, float->double, string<->bytes
  - logical types follow the FILE (the fastavro / Apache convention)
  - enum symbols remapped to the reader's order
  - nested records inside JSON resolved: reordered, dropped, defaulted
  - a nullable file field read as a required one is refused when the file is opened
"""

import base64
import datetime
import decimal
import io
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import fastavro  # noqa: E402

from rugo.rugo_native import read_avro  # noqa: E402

UTC = datetime.timezone.utc


def _record(fields, name="r"):
    return {"type": "record", "name": name, "fields": fields}


def _write(schema, records, codec="null"):
    buf = io.BytesIO()
    fastavro.writer(buf, schema, records, codec=codec, sync_interval=2000)
    return buf.getvalue()


def _rugo(data, reader, columns=None):
    res = read_avro(data, columns, json.dumps(reader))
    out = {n: [] for n in res["column_names"]}
    for batch in res["batches"]:
        for n, v in zip(res["column_names"], batch):
            out[n].extend(v.to_pylist())
    return res, out


def _oracle(data, reader):
    return list(fastavro.reader(io.BytesIO(data), reader_schema=reader))


def _refuses(data, reader, match, columns=None):
    with pytest.raises(RuntimeError, match=match):
        read_avro(data, columns, json.dumps(reader))


# ── promotions ──

@pytest.mark.parametrize("w,r", [
    ("int", "long"), ("int", "float"), ("int", "double"),
    ("long", "float"), ("long", "double"), ("float", "double"),
    ("string", "bytes"), ("bytes", "string"),
])
@pytest.mark.parametrize("nullable", [False, True])
def test_promotions_match_fastavro(w, r, nullable):
    vals = {
        "int": [(-1) ** i * i * 7 for i in range(3000)],
        "long": [(-1) ** i * i * 2**33 for i in range(3000)],
        "float": [i / 8 for i in range(3000)],
        "string": ["s" * (i % 20) for i in range(3000)],
        "bytes": [bytes([65 + i % 26]) * (i % 20) for i in range(3000)],
    }[w]
    if nullable:
        vals = [None if i % 6 == 0 else v for i, v in enumerate(vals)]
    wt, rt = (["null", w], ["null", r]) if nullable else (w, r)
    data = _write(_record([{"name": "c", "type": wt}]), [{"c": v} for v in vals])
    reader = _record([{"name": "c", "type": rt}])
    _, got = _rugo(data, reader)
    assert got["c"] == [x["c"] for x in _oracle(data, reader)]


def test_refuses_non_promotion():
    data = _write(_record([{"name": "c", "type": "long"}]), [{"c": 1}])
    _refuses(data, _record([{"name": "c", "type": "int"}]), r"the file's long cannot be read as int")


# ── field matching ──

def test_fields_match_by_name_reordered_and_dropped():
    w = _record([{"name": "a", "type": "long"}, {"name": "b", "type": "string"}, {"name": "c", "type": "double"}])
    r = _record([{"name": "c", "type": "double"}, {"name": "a", "type": "long"}])
    recs = [{"a": i, "b": str(i), "c": i / 2} for i in range(500)]
    data = _write(w, recs)
    res, got = _rugo(data, r)
    assert res["column_names"] == ["c", "a"]
    oracle = _oracle(data, r)
    assert got["c"] == [x["c"] for x in oracle] and got["a"] == [x["a"] for x in oracle]


def test_fields_match_by_field_id_across_a_rename():
    # Iceberg renames keep the field-id; fastavro has no field-id resolution, so the
    # expected values are the written ones.
    w = _record([{"name": "old_name", "type": "long", "field-id": 7}, {"name": "x", "type": "long", "field-id": 8}])
    r = _record([{"name": "x", "type": "long", "field-id": 8}, {"name": "new_name", "type": "long", "field-id": 7}])
    data = _write(w, [{"old_name": i, "x": -i} for i in range(100)])
    _, got = _rugo(data, r)
    assert got["new_name"] == list(range(100))
    assert got["x"] == [-i for i in range(100)]


def test_field_id_wins_over_a_reused_name():
    # The reader's `a` is a different field (id 2) from the file's `a` (id 1).
    w = _record([{"name": "a", "type": "long", "field-id": 1}])
    r = _record([{"name": "a", "type": ["null", "long"], "field-id": 2, "default": None}])
    data = _write(w, [{"a": 5}])
    _, got = _rugo(data, r)
    assert got["a"] == [None]


# ── reader-only fields: defaults and NULL, as constants ──

DEFAULTS = _record([
    {"name": "id", "type": "long"},
    {"name": "i", "type": "int", "default": -3},
    {"name": "l", "type": "long", "default": 1 << 40},
    {"name": "d", "type": "double", "default": 2.5},
    {"name": "f", "type": "float", "default": 0.25},
    {"name": "b", "type": "boolean", "default": True},
    {"name": "s", "type": "string", "default": "a default longer than twelve"},
    {"name": "by", "type": "bytes", "default": "ÿ\u0000A"},
    {"name": "fx", "type": {"type": "fixed", "name": "F2", "size": 2}, "default": "\u0001\u0002"},
    {"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["x", "y"]}, "default": "y"},
    {"name": "dt", "type": {"type": "int", "logicalType": "date"}, "default": 18000},
    {"name": "n1", "type": ["null", "string"], "default": None},
    {"name": "n2", "type": ["long", "null"], "default": 9},
    {"name": "n3", "type": ["null", "long"]},
    {"name": "rec", "type": ["null", _record([{"name": "q", "type": "long"}], "Q")], "default": None},
])


def test_reader_only_fields_are_defaults_matching_fastavro():
    data = _write(_record([{"name": "id", "type": "long"}]), [{"id": i} for i in range(7000)])
    _, got = _rugo(data, DEFAULTS)
    # fastavro refuses a reader field with no default (n3); we read it as NULL (docs §19.3).
    oracle = _oracle(data, _record([f for f in DEFAULTS["fields"] if f["name"] != "n3"]))
    for name in got:
        if name == "n3":
            continue
        exp = [x[name] for x in oracle]
        if name in ("by", "fx"):
            # The spec: a bytes/fixed default's code points 0-255 ARE the bytes
            # (ISO-8859-1). fastavro returns the JSON string unconverted, and Apache's
            # Python package UTF-8-encodes it; both deviate, so assert the spec.
            exp = [v if type(v) is bytes else v.encode("latin-1") for v in exp]
        if name == "dt":
            # fastavro does not apply a logical type to a default: it returns the raw int.
            exp = [datetime.date(1970, 1, 1) + datetime.timedelta(days=v) for v in exp]
        if name == "rec":
            exp = [None] * len(exp)  # JSON text column: NULL
        assert got[name] == exp, name
    assert got["n3"] == [None] * 7000
    # Constant-shaped (CLAUDE.md §11): one value, every position 0.
    res = read_avro(data, None, json.dumps(DEFAULTS))
    for batch in res["batches"]:
        for name, vec in zip(res["column_names"], batch):
            if name != "id":
                assert vec.is_constant and vec.data_length == 1, name


def test_null_fill_through_an_absent_record():
    data = _write(_record([{"name": "id", "type": "long"}]), [{"id": i} for i in range(10)])
    r = _record([{"name": "id", "type": "long"},
                 {"name": "rec", "type": ["null", _record([{"name": "q", "type": "long"},
                                                           {"name": "z", "type": "string"}], "Q")], "default": None}])
    _, got = _rugo(data, r, ["id", "rec.q", "rec.z"])
    assert got["rec.q"] == [None] * 10 and got["rec.z"] == [None] * 10


def test_default_under_a_nullable_record_is_null_where_the_record_is():
    w = _record([{"name": "rec", "type": ["null", _record([{"name": "a", "type": "long"}], "A")]}])
    r = _record([{"name": "rec", "type": ["null", _record([{"name": "a", "type": "long"},
                                                           {"name": "added", "type": "long", "default": 42}], "A")]}])
    recs = [{"rec": None if i % 3 == 0 else {"a": i}} for i in range(300)]
    data = _write(w, recs)
    _, got = _rugo(data, r, ["rec.a", "rec.added"])
    oracle = _oracle(data, r)
    assert got["rec.added"] == [None if x["rec"] is None else x["rec"]["added"] for x in oracle]
    assert got["rec.a"] == [None if x["rec"] is None else x["rec"]["a"] for x in oracle]


@pytest.mark.parametrize("t", [
    {"type": "long", "logicalType": "timestamp-micros"},
    {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2},
])
def test_refuses_constant_with_a_logical_type_until_draken_can_carry_it(t):
    data = _write(_record([{"name": "id", "type": "long"}]), [{"id": 1}])
    r = _record([{"name": "id", "type": "long"}, {"name": "c", "type": ["null", t], "default": None}])
    _refuses(data, r, r"no draken producer that carries its logical type")


# ── nullability (1a) and logical types (1b) ──

def test_refuses_nullable_file_field_read_as_required():
    data = _write(_record([{"name": "c", "type": ["null", "long"]}]), [{"c": 1}])
    _refuses(data, _record([{"name": "c", "type": "long"}]), r"nullable in the file but required in the reader schema")


def test_required_file_field_read_as_nullable():
    data = _write(_record([{"name": "c", "type": "long"}]), [{"c": i} for i in range(10)])
    _, got = _rugo(data, _record([{"name": "c", "type": ["null", "long"]}]))
    assert got["c"] == list(range(10))


def test_logical_types_follow_the_file():
    w = _record([
        {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "dec", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
        {"name": "dt", "type": {"type": "int", "logicalType": "date"}},
    ])
    r = _record([
        {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "dec", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 4}},
        {"name": "dt", "type": "int"},
    ])
    recs = [{"ts": datetime.datetime(2020, 1, 1, tzinfo=UTC) + datetime.timedelta(milliseconds=i),
             "dec": decimal.Decimal(i).scaleb(-2), "dt": datetime.date(2020, 1, 1)} for i in range(100)]
    data = _write(w, recs)
    _, got = _rugo(data, r)
    oracle = _oracle(data, r)
    for name in ("ts", "dec", "dt"):
        assert got[name] == [x[name] for x in oracle], name


# ── enums ──

def test_enum_symbols_remap_to_the_reader_order():
    w = _record([{"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["a", "b", "c"]}}])
    r = _record([{"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["c", "z", "a", "b"]}}])
    data = _write(w, [{"e": "abc"[i % 3]} for i in range(1000)])
    _, got = _rugo(data, r)
    assert got["e"] == [x["e"] for x in _oracle(data, r)]


def test_refuses_enum_symbol_the_reader_lacks():
    w = _record([{"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["a", "b"]}}])
    r = _record([{"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["a"]}}])
    _refuses(_write(w, [{"e": "a"}]), r, r"enum symbol 'b' is not in the reader's enum")


# ── nested records inside JSON (1c) ──

def _json_norm(v):
    if v is None or v is True or v is False:
        return v
    t = type(v)
    if t is dict:
        return {k: _json_norm(x) for k, x in v.items()}
    if t is list:
        return [_json_norm(x) for x in v]
    if t is bytes:
        return base64.b64encode(v).decode()
    if t is float:
        return decimal.Decimal(repr(v))
    if t is datetime.date:
        return v.isoformat()
    return v


@pytest.mark.parametrize("reorder", [False, True])
def test_json_record_resolved_like_fastavro(reorder):
    inner_w = _record([
        {"name": "a", "type": "long"},
        {"name": "dropped", "type": {"type": "array", "items": "string"}},
        {"name": "b", "type": "string"},
        {"name": "deep", "type": ["null", _record([{"name": "x", "type": "int"}, {"name": "y", "type": "string"}], "D")]},
    ], "I")
    deep_r = _record([{"name": "y", "type": "string"}, {"name": "x", "type": "long"},
                      {"name": "z", "type": "boolean", "default": False}], "D")
    fields_r = [
        {"name": "a", "type": "double"},
        {"name": "added", "type": "string", "default": "dflt"},
        {"name": "b", "type": "bytes"},
        {"name": "deep", "type": ["null", deep_r]},
        {"name": "dt", "type": ["null", {"type": "int", "logicalType": "date"}], "default": None},
        {"name": "e", "type": {"type": "enum", "name": "EE", "symbols": ["p", "q"]}, "default": "q"},
    ]
    if reorder:
        fields_r = [fields_r[2], fields_r[3], fields_r[0], fields_r[1], fields_r[5], fields_r[4]]
    w = _record([{"name": "id", "type": "long"}, {"name": "rec", "type": inner_w}])
    r = _record([{"name": "rec", "type": _record(fields_r, "I")}])
    recs = [{"id": i, "rec": {"a": i, "dropped": ["x"] * (i % 3), "b": "b" * (i % 5),
                              "deep": None if i % 4 == 0 else {"x": i, "y": f"y{i}"}}} for i in range(800)]
    data = _write(w, recs)
    _, got = _rugo(data, r)
    oracle = _oracle(data, r)
    assert [json.loads(x, parse_float=decimal.Decimal) for x in got["rec"]] == [_json_norm(x["rec"]) for x in oracle]
    # key order follows the reader schema
    assert list(json.loads(got["rec"][1]).keys()) == [f["name"] for f in fields_r]


# ── Iceberg: one reader schema for v1 and v2 manifests ──

def test_one_reader_schema_reads_v1_and_v2_manifests(tmp_path):
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    from rugo.rugo_native import read_avro_metadata

    manifests = {}
    for version in ("1", "2"):
        cat = SqlCatalog(f"t{version}", uri=f"sqlite:///{tmp_path}/c{version}.db", warehouse=f"file://{tmp_path}/wh{version}")
        cat.create_namespace("n")
        t = cat.create_table("n.t", schema=Schema(NestedField(1, "id", LongType(), required=False)),
                             properties={"format-version": version})
        t.append(pa.table({"id": pa.array([1, 2, 3], pa.int64())}))
        m = t.current_snapshot().manifests(t.io)[0]
        manifests[version] = Path(m.manifest_path[len("file://"):]).read_bytes()

    # The v2 manifest's own schema, with `content` defaulted to 0 (DATA) — what the
    # Iceberg spec says a v1 data file is.
    reader = json.loads(read_avro_metadata(manifests["2"])["schema"])
    data_file = next(f for f in reader["fields"] if f["name"] == "data_file")
    content = next(f for f in data_file["type"]["fields"] if f["name"] == "content")
    content["default"] = 0
    cols = ["status", "data_file.content", "data_file.file_path", "data_file.record_count"]
    for version, data in manifests.items():
        _, got = _rugo(data, reader, cols)
        assert got["data_file.content"] == [0], version
        assert got["data_file.record_count"] == [3], version
        assert got["status"] == [1], version
