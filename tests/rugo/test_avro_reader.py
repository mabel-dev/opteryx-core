# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Rugo's Avro reader (docs/AVRO_READER_DESIGN.md, scope A) against two independent
oracles: fastavro writes every fixture and decodes it, and Apache's own `avro`
package (the reference implementation) decodes it too. Rugo must agree with both.
Both are test-only dependencies (tests/requirements.txt), never imported by rugo.

PyArrow has no Avro reader, so it is not an oracle here.
"""

import base64
import datetime
import decimal
import io
import json
import math
import struct
import sys
import zlib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import fastavro  # noqa: E402
from avro.datafile import DataFileReader  # noqa: E402
from avro.io import DatumReader  # noqa: E402

from rugo.rugo_native import read_avro, read_avro_metadata  # noqa: E402

UTC = datetime.timezone.utc
CODECS = ["null", "deflate", "snappy", "zstandard"]


def _write(schema, records, codec="null", sync_interval=16000):
    buf = io.BytesIO()
    fastavro.writer(buf, schema, records, codec=codec, sync_interval=sync_interval)
    return buf.getvalue()


def _rugo_rows(data, columns=None):
    res = read_avro(data, columns)
    out = {name: [] for name in res["column_names"]}
    for batch in res["batches"]:
        for name, vec in zip(res["column_names"], batch):
            out[name].extend(vec.to_pylist())
    return res, out


def _fastavro_rows(data):
    return list(fastavro.reader(io.BytesIO(data)))


def _apache_rows(data):
    reader = DataFileReader(io.BytesIO(data), DatumReader())
    rows = list(reader)
    reader.close()
    return rows


def _record(schema_fields):
    return {"type": "record", "name": "r", "fields": schema_fields}


# ── primitives and logical types, every codec, dense / nullable both orders ──

def _values(kind, n):
    if kind == "boolean":
        return [i % 3 == 0 for i in range(n)]
    if kind == "int":
        return [(-1) ** i * i * 7919 for i in range(n)]
    if kind == "long":
        return [(-1) ** i * i * 2**40 for i in range(n)]
    if kind == "float":
        return [i / 4 for i in range(n)]  # exactly representable in float32
    if kind == "double":
        return [i / 3 for i in range(n)]
    if kind == "string":
        return ["v" * (i % 30) for i in range(n)]  # inline and long-form slots
    if kind == "bytes":
        return [bytes([i % 256]) * (i % 20) for i in range(n)]
    raise AssertionError(kind)


PRIMITIVES = ["boolean", "int", "long", "float", "double", "string", "bytes"]


@pytest.mark.parametrize("codec", CODECS)
@pytest.mark.parametrize("kind", PRIMITIVES)
@pytest.mark.parametrize("shape", ["dense", "null_first", "null_second"])
def test_primitive_matches_both_oracles(codec, kind, shape):
    n = 2500
    vals = _values(kind, n)
    if shape == "dense":
        t = kind
    else:
        t = ["null", kind] if shape == "null_first" else [kind, "null"]
        vals = [None if i % 7 == 0 else v for i, v in enumerate(vals)]
    data = _write(_record([{"name": "c", "type": t}]), [{"c": v} for v in vals], codec, sync_interval=4000)

    _, got = _rugo_rows(data)
    assert got["c"] == [r["c"] for r in _fastavro_rows(data)]
    assert got["c"] == [r["c"] for r in _apache_rows(data)]


def test_logical_types_match_both_oracles():
    schema = _record([
        {"name": "date", "type": {"type": "int", "logicalType": "date"}},
        {"name": "tms", "type": {"type": "int", "logicalType": "time-millis"}},
        {"name": "tus", "type": {"type": "long", "logicalType": "time-micros"}},
        {"name": "tsms", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "tsus", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "dec", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 3}]},
        {"name": "decf", "type": {"type": "fixed", "name": "F8", "size": 8, "logicalType": "decimal", "precision": 18, "scale": 0}},
        # 128-bit path; values kept to 28 significant digits — see test note below.
        {"name": "dec128", "type": {"type": "bytes", "logicalType": "decimal", "precision": 30, "scale": 2}},
        {"name": "dec128f", "type": {"type": "fixed", "name": "F16", "size": 16, "logicalType": "decimal", "precision": 38, "scale": 6}},
    ])
    base = datetime.datetime(2001, 2, 3, 4, 5, 6, tzinfo=UTC)
    recs = []
    for i in range(3000):
        recs.append({
            "date": datetime.date(1960, 1, 1) + datetime.timedelta(days=i * 11),
            "tms": datetime.time(i % 24, i % 60, i % 60, (i % 1000) * 1000),
            "tus": datetime.time(i % 24, i % 60, i % 60, i % 1000000),
            "tsms": base + datetime.timedelta(milliseconds=i * 1234567),
            "tsus": base - datetime.timedelta(microseconds=i * 987654321),
            "dec": None if i % 5 == 0 else decimal.Decimal(i * 1000003 * (-1) ** i).scaleb(-3),
            "decf": decimal.Decimal((-1) ** i * i * 10**14),
            "dec128": decimal.Decimal((-1) ** i * (10**27 + i)).scaleb(-2),
            "dec128f": decimal.Decimal((-1) ** i * (10**27 + i)).scaleb(-6),
        })
    for codec in ["null", "zstandard"]:
        data = _write(schema, recs, codec, sync_interval=8000)
        _, got = _rugo_rows(data)
        for oracle in (_fastavro_rows(data), _apache_rows(data)):
            for name in got:
                assert got[name] == [r[name] for r in oracle], name


def test_enum_is_dictionary_shaped():
    symbols = ["A", "BB", "A_LONG_SYMBOL_PAST_INLINE"]
    schema = _record([{"name": "e", "type": ["null", {"type": "enum", "name": "E", "symbols": symbols}]}])
    vals = [None if i % 4 == 0 else symbols[i % 3] for i in range(5000)]
    data = _write(schema, [{"e": v} for v in vals])
    _, got = _rugo_rows(data)
    assert got["e"] == vals == [r["e"] for r in _apache_rows(data)]


# ── projection ──

_DEEP = {"type": "record", "name": "D", "fields": [
    {"name": "y", "type": ["null", "string"]},
    {"name": "z", "type": {"type": "array", "items": {"type": "array", "items": "int"}}},
]}
_INNER = {"type": "record", "name": "I", "fields": [
    {"name": "x", "type": "long"},
    {"name": "deep", "type": _DEEP},
]}
NESTED = _record([
    {"name": "id", "type": "long"},
    {"name": "tags", "type": {"type": "array", "items": "string"}},
    {"name": "attrs", "type": {"type": "map", "values": ["null", "double"]}},
    {"name": "inner", "type": ["null", _INNER]},
    {"name": "tail", "type": "string"},
])


def _nested_records(n):
    out = []
    for i in range(n):
        out.append({
            "id": i,
            "tags": ["t" * j for j in range(i % 4)],
            "attrs": {f"k{j}": (None if j % 2 else j / 2) for j in range(i % 3)},
            "inner": None if i % 3 == 0 else {
                "x": i * 10,
                "deep": {"y": None if i % 5 == 0 else f"y{i}", "z": [[j] * j for j in range(i % 3)]},
            },
            "tail": f"tail-{i}",
        })
    return out


@pytest.mark.parametrize("codec", ["null", "snappy"])
def test_dotted_projection_through_nullable_records(codec):
    recs = _nested_records(4000)
    data = _write(NESTED, recs, codec)
    cols = ["tail", "inner.deep.y", "id", "inner.x"]
    res, got = _rugo_rows(data, cols)
    assert res["column_names"] == cols
    assert got["id"] == [r["id"] for r in recs]
    assert got["tail"] == [r["tail"] for r in recs]
    assert got["inner.x"] == [None if r["inner"] is None else r["inner"]["x"] for r in recs]
    assert got["inner.deep.y"] == [None if r["inner"] is None else r["inner"]["deep"]["y"] for r in recs]


def _zz(v):
    # zigzag varint
    v = (v << 1) ^ (v >> 63)
    out = bytearray()
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _container(schema_json, records_bytes, count, sync=b"S" * 16):
    meta = _zz(1) + _zz(11) + b"avro.schema" + _zz(len(schema_json)) + schema_json + _zz(0)
    return b"Obj\x01" + meta + sync + _zz(count) + _zz(len(records_bytes)) + records_bytes + sync


def test_skip_array_block_with_byte_size():
    # The negative-count array block form ("-n items, then their byte size") is legal
    # and lets a reader skip without decoding. fastavro never writes it, so hand-build.
    schema = b'{"type":"record","name":"r","fields":[{"name":"a","type":{"type":"array","items":"long"}},{"name":"b","type":"long"}]}'
    items = _zz(5) + _zz(-6) + _zz(7)
    rec = _zz(-3) + _zz(len(items)) + items + _zz(0) + _zz(42)
    data = _container(schema, rec + rec, 2)
    _, got = _rugo_rows(data, ["b"])
    assert got["b"] == [42, 42]
    assert [r["b"] for r in _apache_rows(data)] == [42, 42]


# ── nested output (docs §17.2): JSON text, or ARRAY for arrays of plain scalars ──

def _json_norm(v):
    """A fastavro value as parquet's NESTEDJSON rules render it, parsed back."""
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
        return None if math.isnan(v) or math.isinf(v) else decimal.Decimal(repr(v))
    if t is decimal.Decimal:
        return v
    if t is datetime.datetime:
        return v.isoformat()
    if t is datetime.date or t is datetime.time:
        return v.isoformat()
    return v  # int, str


def _json_cell(text):
    return None if text is None else json.loads(text, parse_float=decimal.Decimal)


@pytest.mark.parametrize("codec", ["null", "zstandard"])
def test_whole_record_map_and_nested_arrays_are_json(codec):
    recs = _nested_records(3000)
    data = _write(NESTED, recs, codec)
    _, got = _rugo_rows(data, ["inner", "attrs", "id"])
    assert [_json_cell(x) for x in got["inner"]] == [_json_norm(r["inner"]) for r in recs]
    assert [_json_cell(x) for x in got["attrs"]] == [_json_norm(r["attrs"]) for r in recs]
    # array<array<int>> inside a record, by path: JSON
    _, got = _rugo_rows(data, ["inner.deep.z"])
    assert [_json_cell(x) for x in got["inner.deep.z"]] == [
        None if r["inner"] is None else r["inner"]["deep"]["z"] for r in recs]


def test_json_renders_every_value_kind():
    schema = _record([{"name": "rec", "type": ["null", {"type": "record", "name": "V", "fields": [
        {"name": "b", "type": "boolean"},
        {"name": "f", "type": "float"},
        {"name": "d", "type": ["null", "double"]},
        {"name": "s", "type": "string"},
        {"name": "by", "type": "bytes"},
        {"name": "fx", "type": {"type": "fixed", "name": "F3", "size": 3}},
        {"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["red", "green"]}},
        {"name": "dt", "type": {"type": "int", "logicalType": "date"}},
        {"name": "tm", "type": {"type": "int", "logicalType": "time-millis"}},
        {"name": "tu", "type": {"type": "long", "logicalType": "time-micros"}},
        {"name": "tsm", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "tsu", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "dec", "type": {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 3}},
        {"name": "decf", "type": {"type": "fixed", "name": "F9", "size": 9, "logicalType": "decimal", "precision": 20, "scale": 2}},
        {"name": "empty", "type": {"type": "record", "name": "Z", "fields": []}},
        {"name": "esc", "type": "string"},
    ]}]}])
    base = datetime.datetime(1999, 12, 31, 23, 59, 58, tzinfo=UTC)
    recs = []
    for i in range(500):
        recs.append({"rec": None if i % 6 == 0 else {
            "b": i % 2 == 0,
            "f": [1.5, float("nan"), float("inf"), -0.0][i % 4],
            "d": None if i % 5 == 0 else i / 7,
            "s": "s" * (i % 9),
            "by": bytes(range(i % 7)),
            "fx": bytes([i % 256, 1, 2]),
            "e": ["red", "green"][i % 2],
            "dt": datetime.date(1970, 1, 1) + datetime.timedelta(days=i * 37 - 5000),
            "tm": datetime.time(i % 24, i % 60, 7, (i % 1000) * 1000),
            "tu": datetime.time(i % 24, 3, i % 60, i),
            "tsm": base + datetime.timedelta(milliseconds=i * 1001),
            "tsu": base - datetime.timedelta(microseconds=i * 1234567),
            "dec": decimal.Decimal((-1) ** i * i * 1001).scaleb(-3),
            "decf": decimal.Decimal((-1) ** i * (10**17 + i)).scaleb(-2),
            "empty": {},
            "esc": 'quote" back\\ nl\n tab\t ctl\x01 ü',
        }})
    data = _write(schema, recs)
    _, got = _rugo_rows(data, ["rec"])
    assert [_json_cell(x) for x in got["rec"]] == [_json_norm(r["rec"]) for r in recs]


ARRAYS = _record([
    {"name": "longs", "type": {"type": "array", "items": "long"}},
    {"name": "nlongs", "type": ["null", {"type": "array", "items": ["null", "long"]}]},
    {"name": "ints", "type": {"type": "array", "items": "int"}},
    {"name": "bools", "type": {"type": "array", "items": "boolean"}},
    {"name": "dbls", "type": {"type": "array", "items": "double"}},
    {"name": "flts", "type": {"type": "array", "items": "float"}},
    {"name": "strs", "type": {"type": "array", "items": ["string", "null"]}},
    {"name": "blobs", "type": {"type": "array", "items": "bytes"}},
])


@pytest.mark.parametrize("codec", ["null", "snappy"])
def test_arrays_of_scalars_are_array_vectors(codec):
    recs = []
    for i in range(70000):  # crosses a batch boundary
        k = i % 5
        recs.append({
            "longs": [i * 1000 + j for j in range(k)],
            "nlongs": None if i % 7 == 0 else [None if j % 2 else j for j in range(k)],
            "ints": [-j for j in range(k)],
            "bools": [j % 2 == 0 for j in range(k + i % 11)],
            "dbls": [j / 3 for j in range(k)],
            "flts": [j / 4 for j in range(k)],
            "strs": [None if j == 1 else "x" * (j * 7) for j in range(k)],
            "blobs": [bytes([j]) * j for j in range(k)],
        })
    data = _write(ARRAYS, recs, codec)
    res, got = _rugo_rows(data)
    assert len(res["batches"]) > 1
    for name in got:
        assert got[name] == [r[name] for r in recs], name


def test_all_columns_on_a_nested_schema():
    recs = _nested_records(50)
    data = _write(NESTED, recs)
    res, got = _rugo_rows(data)
    assert res["column_names"] == ["id", "tags", "attrs", "inner", "tail"]
    assert got["tags"] == [r["tags"] for r in recs]
    assert [_json_cell(x) for x in got["inner"]] == [_json_norm(r["inner"]) for r in recs]


def test_refuses_array_of_logical_type():
    schema = _record([{"name": "a", "type": {"type": "array", "items": {"type": "int", "logicalType": "date"}}}])
    data = _write(schema, [{"a": [datetime.date(2020, 1, 1)]}])
    _refuses(data, r"array of a logical type, which ARRAY cannot hold")


# ── batching ──

def test_whole_blocks_pack_into_batches_up_to_65536_rows():
    recs = [{"c": i} for i in range(200_000)]
    data = _write(_record([{"name": "c", "type": "long"}]), recs, sync_interval=100_000)
    res, got = _rugo_rows(data)
    assert got["c"] == list(range(200_000))
    assert res["num_rows"] == 200_000
    assert len(res["batches"]) > 1
    for batch in res["batches"]:
        assert len(batch[0]) <= 65536


def test_metadata_and_schema():
    data = _write(_record([{"name": "c", "type": "long"}]), [{"c": 1}], codec="zstandard")
    meta = read_avro_metadata(data)
    assert meta["metadata"]["avro.codec"] == b"zstandard"
    assert '"name": "c"' in meta["schema"] or '"name":"c"' in meta["schema"]


def test_empty_file_has_no_batches():
    data = _write(_record([{"name": "c", "type": "long"}]), [])
    res = read_avro(data)
    assert res["num_rows"] == 0 and res["batches"] == []


# ── deflate as written by Python writers ──

def test_strict_raw_deflate():
    # A raw RFC 1951 stream with nothing after it (what the Java writer produces).
    schema = b'{"type":"record","name":"r","fields":[{"name":"c","type":"long"}]}'
    raw = b"".join(_zz(i) for i in range(1000))
    co = zlib.compressobj(wbits=-15)
    body = co.compress(raw) + co.flush()
    meta = (_zz(2) + _zz(11) + b"avro.schema" + _zz(len(schema)) + schema
            + _zz(10) + b"avro.codec" + _zz(7) + b"deflate" + _zz(0))
    sync = b"Q" * 16
    data = b"Obj\x01" + meta + sync + _zz(1000) + _zz(len(body)) + body + sync
    _, got = _rugo_rows(data)
    assert got["c"] == list(range(1000))


def test_deflate_refuses_more_than_the_zlib_trailer():
    # D10 (a): up to 4 bytes may follow the end-of-stream marker; 5 is corrupt.
    schema = b'{"type":"record","name":"r","fields":[{"name":"c","type":"long"}]}'
    raw = b"".join(_zz(i) for i in range(10))
    meta = (_zz(2) + _zz(11) + b"avro.schema" + _zz(len(schema)) + schema
            + _zz(10) + b"avro.codec" + _zz(7) + b"deflate" + _zz(0))
    sync = b"Q" * 16
    for extra, ok in ((b"\x00" * 4, True), (b"\x00" * 5, False)):
        c = zlib.compressobj(wbits=-15)
        body = c.compress(raw) + c.flush() + extra
        data = b"Obj\x01" + meta + sync + _zz(10) + _zz(len(body)) + body + sync
        if ok:
            assert _rugo_rows(data)[1]["c"] == list(range(10))
        else:
            _refuses(data, r"trailing bytes after the compressed block")


def test_deflate_block_needing_output_growth():
    # A block that inflates far beyond 4x its compressed size: the first output buffer
    # is too small, so the decoder must grow it and inflate the block again.
    recs = [{"s": "x" * 100_000} for _ in range(30)]
    data = _write(_record([{"name": "s", "type": "string"}]), recs, "deflate", sync_interval=10_000_000)
    _, got = _rugo_rows(data)
    assert got["s"] == [r["s"] for r in recs]


def test_refuses_corrupt_deflate_stream():
    schema = b'{"type":"record","name":"r","fields":[{"name":"c","type":"long"}]}'
    meta = (_zz(2) + _zz(11) + b"avro.schema" + _zz(len(schema)) + schema
            + _zz(10) + b"avro.codec" + _zz(7) + b"deflate" + _zz(0))
    body = b"\xff\xff\xff\xff\xff\xff"  # not a valid DEFLATE stream
    sync = b"Q" * 16
    _refuses(b"Obj\x01" + meta + sync + _zz(1) + _zz(len(body)) + body + sync, r"deflate: the compressed block is corrupt")


# ── refusals (docs §8) ──

def _refuses(data, match, columns=None):
    with pytest.raises(RuntimeError, match=match):
        read_avro(data, columns)


def test_refuses_general_union():
    data = _write(_record([{"name": "c", "type": ["int", "string"]}]), [{"c": 1}])
    _refuses(data, r"union without a null branch")


def test_refuses_three_branch_union():
    data = _write(_record([{"name": "c", "type": ["null", "int", "string"]}]), [{"c": 1}])
    _refuses(data, r"union of 3 branches")


def test_refuses_recursive_type():
    schema = {"type": "record", "name": "Node", "fields": [
        {"name": "v", "type": "long"}, {"name": "next", "type": ["null", "Node"]}]}
    data = _write(schema, [{"v": 1, "next": None}])
    _refuses(data, r"recursive type 'Node'")


def test_refuses_uuid():
    data = _write(_record([{"name": "u", "type": {"type": "string", "logicalType": "uuid"}}]),
                  [{"u": "6b8c5d4e-0000-4000-8000-000000000000"}])
    _refuses(data, r"uuid, which is not supported")


@pytest.mark.parametrize("lt", ["timestamp-nanos", "local-timestamp-micros"])
def test_refuses_unsupported_logical(lt):
    schema = b'{"type":"record","name":"r","fields":[{"name":"c","type":{"type":"long","logicalType":"%s"}}]}' % lt.encode()
    _refuses(_container(schema, _zz(1), 1), lt)


def test_refuses_decimal_wider_than_38():
    schema = b'{"type":"record","name":"r","fields":[{"name":"c","type":{"type":"bytes","logicalType":"decimal","precision":39,"scale":0}}]}'
    _refuses(_container(schema, _zz(1) + b"\x01", 1), r"wider than 38")


def test_refuses_missing_column():
    data = _write(NESTED, _nested_records(10))
    _refuses(data, r"'inner.nope' is not in the file's schema", ["inner.nope"])


def test_refuses_column_selected_twice():
    data = _write(NESTED, _nested_records(10))
    _refuses(data, r"selected twice", ["id", "id"])


def test_refuses_path_through_array():
    data = _write(NESTED, _nested_records(10))
    _refuses(data, r"is a array", ["tags.x"])


def test_refuses_bzip2_codec():
    schema = b'{"type":"record","name":"r","fields":[{"name":"c","type":"long"}]}'
    meta = (_zz(2) + _zz(11) + b"avro.schema" + _zz(len(schema)) + schema
            + _zz(10) + b"avro.codec" + _zz(5) + b"bzip2" + _zz(0))
    _refuses(b"Obj\x01" + meta + b"S" * 16, r"codec 'bzip2' is not supported")


def test_refuses_undecodable_header_bytes_with_runtime_error():
    # Found by mutation: header bytes reach error messages and Python strings, and
    # must surface as RuntimeError, never UnicodeDecodeError.
    schema = b'{"type":"record","name":"r","fields":[{"name":"c","type":"long"}]}'
    meta = (_zz(2) + _zz(11) + b"avro.schema" + _zz(len(schema)) + schema
            + _zz(10) + b"avro.codec" + _zz(6) + b"\x8deflat" + _zz(0))
    _refuses(b"Obj\x01" + meta + b"S" * 16, r"codec '\\x8deflat' is not supported")
    meta = (_zz(2) + _zz(11) + b"avro.schema" + _zz(len(schema)) + schema
            + _zz(3) + b"k\xed\xa0" + _zz(1) + b"v" + _zz(0))
    _refuses(b"Obj\x01" + meta + b"S" * 16, r"metadata key is not valid UTF-8")


def test_refuses_bad_magic():
    _refuses(b"PAR1" + b"\x00" * 40, r"bad magic")


def test_refuses_sync_mismatch():
    schema = b'{"type":"record","name":"r","fields":[{"name":"c","type":"long"}]}'
    data = bytearray(_container(schema, _zz(1), 1))
    data[-1] ^= 0xFF
    _refuses(bytes(data), r"sync marker")


def test_refuses_truncated_block():
    schema = b'{"type":"record","name":"r","fields":[{"name":"c","type":"string"}]}'
    _refuses(_container(schema, _zz(10) + b"abc", 1), r"truncated")


def test_refuses_trailing_bytes_in_block():
    schema = b'{"type":"record","name":"r","fields":[{"name":"c","type":"long"}]}'
    _refuses(_container(schema, _zz(1) + _zz(2), 1), r"trailing bytes")


def test_refuses_overlong_varint():
    schema = b'{"type":"record","name":"r","fields":[{"name":"c","type":"long"}]}'
    _refuses(_container(schema, b"\xff" * 11 + b"\x01", 1), r"longer than 10 bytes")


def test_refuses_snappy_crc_mismatch():
    data = bytearray(_write(_record([{"name": "c", "type": "long"}]), [{"c": i} for i in range(100)], "snappy"))
    # The block's 4-byte CRC sits just before the trailing 16-byte sync marker.
    data[-17] ^= 0xFF
    _refuses(bytes(data), r"CRC32 mismatch")


def test_refuses_enum_index_out_of_range():
    schema = b'{"type":"record","name":"r","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["a"]}}]}'
    _refuses(_container(schema, _zz(3), 1), r"enum index is out of range")


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
