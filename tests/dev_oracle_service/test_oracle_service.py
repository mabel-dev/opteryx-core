"""
Oracle service tests — the HTTP service in dev/oracle_service that the rugo riscv64
release floor will post rugo-written files to.

PyArrow, fastavro and Apache avro are the files' WRITERS here and the service's
READERS; every expected value is a literal or is computed in this file with
struct/int arithmetic, never by the service's own encoder. Nothing here imports
rugo, draken or opteryx, and nothing needs the native build.
"""

import base64
import datetime
import decimal
import hashlib
import http.client
import io
import json
import socket
import struct
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SERVICE_DIR = REPO_ROOT / "dev"
sys.path.insert(0, str(SERVICE_DIR))

import fastavro  # test writer
import pyarrow as pa  # test writer
import pyarrow.orc as orc  # test writer
import pyarrow.parquet as pq  # test writer

from oracle_service import SCHEMA_VERSION, client, service

NAN = float("nan")
INF = float("inf")


# ─────────────────────────────────────────────────────────────────────────────
# Plumbing
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def oracle():
    server = service.start_in_thread("127.0.0.1", 0, 8 * 1024 * 1024)
    yield "http://127.0.0.1:%d" % server.server_port
    server.shutdown()
    server.server_close()


def pq_bytes(table, **kwargs) -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf, **kwargs)
    return buf.getvalue()


def orc_bytes(table, **kwargs) -> bytes:
    buf = io.BytesIO()
    orc.write_table(table, buf, **kwargs)
    return buf.getvalue()


def avro_bytes(schema, records, **kwargs) -> bytes:
    buf = io.BytesIO()
    fastavro.writer(buf, schema, records, **kwargs)
    return buf.getvalue()


def columns(doc: dict) -> dict:
    return {c["name"]: c["values"] for c in doc["columns"]}


def fields(doc: dict) -> dict:
    return {f["name"]: f for f in doc["schema"]["fields"]}


def f64(value: float, text: str) -> str:
    return "f64:%s:%s" % (struct.pack(">d", value).hex(), text)


def f32(value: float, text: str) -> str:
    return "f32:%s:%s" % (struct.pack(">f", value).hex(), text)


def b64(raw: bytes) -> str:
    return "b64:" + base64.b64encode(raw).decode()


# ─────────────────────────────────────────────────────────────────────────────
# Parquet
# ─────────────────────────────────────────────────────────────────────────────

PARQUET_SCALARS = pa.table(
    {
        "i64": pa.array([2**53 + 1, -(2**63), 2**63 - 1, None], pa.int64()),
        "u64": pa.array([2**64 - 1, 2**63, 0, None], pa.uint64()),
        "i8": pa.array([-128, 127, 0, None], pa.int8()),
        "u32": pa.array([2**32 - 1, 0, 1, None], pa.uint32()),
        "flag": pa.array([True, False, None, True], pa.bool_()),
        "f32": pa.array([NAN, -0.0, INF, -INF], pa.float32()),
        "f32b": pa.array([1.1, 0.0, 3.4028234663852886e38, None], pa.float32()),
        "f64": pa.array([NAN, -0.0, INF, -INF], pa.float64()),
        "f64b": pa.array([0.1, 5e-324, 1.7976931348623157e308, None], pa.float64()),
        "dec": pa.array(
            [decimal.Decimal("1.50"), decimal.Decimal("-0.01"), decimal.Decimal("99999999.99"), None],
            pa.decimal128(10, 2),
        ),
        "dec38": pa.array(
            [decimal.Decimal("1234567890123456789012345678.1234567890"), None, decimal.Decimal("0E-10"), None],
            pa.decimal128(38, 10),
        ),
        "ts_ms": pa.array([0, -1, 1577836800123, None], pa.timestamp("ms")),
        "ts_us_utc": pa.array([0, -1, 1577836800123456, None], pa.timestamp("us", tz="UTC")),
        "ts_ns": pa.array([1, 2**62, -(2**62), None], pa.timestamp("ns")),
        "d32": pa.array([0, -1, 18263, None], pa.date32()),
        "t_us": pa.array([0, 86399999999, 1, None], pa.time64("us")),
        "t_ms": pa.array([0, 86399999, 1, None], pa.time32("ms")),
        "bin": pa.array([b"\x00\xff", b"", b"abc", None], pa.binary()),
        "txt": pa.array(["héllo", "", "\u0000x", None], pa.string()),
        "emoji": pa.array(["😀", "a\nb", "\"q\"", "\\"], pa.string()),
    }
)

PARQUET_SCALARS_EXPECTED = {
    "i64": ["i64:9007199254740993", "i64:-9223372036854775808", "i64:9223372036854775807", None],
    "u64": ["u64:18446744073709551615", "u64:9223372036854775808", "u64:0", None],
    "i8": [-128, 127, 0, None],
    "u32": [4294967295, 0, 1, None],
    "flag": [True, False, None, True],
    "f32": ["f32:7fc00000:nan", "f32:80000000:-0.0", "f32:7f800000:inf", "f32:ff800000:-inf"],
    "f32b": ["f32:3f8ccccd:1.1", "f32:00000000:0.0", "f32:7f7fffff:3.4028235e+38", None],
    "f64": [
        "f64:7ff8000000000000:nan",
        "f64:8000000000000000:-0.0",
        "f64:7ff0000000000000:inf",
        "f64:fff0000000000000:-inf",
    ],
    "f64b": [
        "f64:3fb999999999999a:0.1",
        "f64:0000000000000001:5e-324",
        "f64:7fefffffffffffff:1.7976931348623157e+308",
        None,
    ],
    "dec": ["dec:1.50", "dec:-0.01", "dec:99999999.99", None],
    "dec38": ["dec:1234567890123456789012345678.1234567890", None, "dec:0.0000000000", None],
    "ts_ms": ["ts[ms]:0", "ts[ms]:-1", "ts[ms]:1577836800123", None],
    "ts_us_utc": ["ts[us]:0", "ts[us]:-1", "ts[us]:1577836800123456", None],
    "ts_ns": ["ts[ns]:1", "ts[ns]:4611686018427387904", "ts[ns]:-4611686018427387904", None],
    "d32": ["date32:0", "date32:-1", "date32:18263", None],
    "t_us": ["time[us]:0", "time[us]:86399999999", "time[us]:1", None],
    "t_ms": ["time[ms]:0", "time[ms]:86399999", "time[ms]:1", None],
    "bin": ["b64:AP8=", "b64:", "b64:YWJj", None],
    "txt": ["héllo", "", "\u0000x", None],
    "emoji": ["😀", "a\nb", '"q"', "\\"],
}


def test_parquet_scalar_values_are_exact(oracle):
    data = pq_bytes(PARQUET_SCALARS)
    doc = client.inspect_bytes(data, "parquet", oracle)
    assert doc["schema_version"] == SCHEMA_VERSION
    assert doc["format"] == "parquet"
    assert doc["input"] == {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
    assert columns(doc) == PARQUET_SCALARS_EXPECTED
    assert [c["name"] for c in doc["columns"]] == list(PARQUET_SCALARS_EXPECTED)


def test_parquet_float_hex_matches_independent_struct_packing(oracle):
    doc = client.inspect_bytes(pq_bytes(PARQUET_SCALARS), "parquet", oracle)
    cols = columns(doc)
    assert cols["f64"][1] == f64(-0.0, "-0.0")
    assert cols["f64b"][0] == f64(0.1, "0.1")
    assert cols["f32b"][0] == f32(1.1, "1.1")
    # NaN, +inf, -inf and -0.0 are distinct, explicit, and never JSON numbers
    assert len({cols["f64"][0], cols["f64"][1], cols["f64"][2], cols["f64"][3]}) == 4


def test_parquet_response_is_strict_json_with_no_bare_numbers_for_wide_types(oracle):
    raw = client.inspect_bytes(pq_bytes(PARQUET_SCALARS), "parquet", oracle)
    text = json.dumps(raw)
    json.loads(text, parse_constant=lambda c: pytest.fail("non-finite JSON constant %s" % c))
    prefixes = {"i64": "i64:", "u64": "u64:", "f32": "f32:", "f64": "f64:", "dec": "dec:", "ts_ns": "ts[ns]:", "bin": "b64:"}
    for name, prefix in prefixes.items():
        for v in columns(raw)[name]:
            assert v is None or v.startswith(prefix), (name, v)


def test_parquet_schema_physical_and_logical_types(oracle):
    table = pa.table(
        {
            "req": pa.array([1, 2], pa.int32()),
            "dec": pa.array([decimal.Decimal("1.50"), None], pa.decimal128(10, 2)),
            "ts": pa.array([1, None], pa.timestamp("ns", tz="UTC")),
            "u64": pa.array([1, None], pa.uint64()),
            "lst": pa.array([[1], None], pa.list_(pa.int32())),
        },
        schema=pa.schema(
            [
                pa.field("req", pa.int32(), nullable=False),
                pa.field("dec", pa.decimal128(10, 2)),
                pa.field("ts", pa.timestamp("ns", tz="UTC")),
                pa.field("u64", pa.uint64()),
                pa.field("lst", pa.list_(pa.int32())),
            ]
        ),
    )
    doc = client.inspect_bytes(pq_bytes(table), "parquet", oracle)
    f = fields(doc)
    assert f["req"]["nullable"] is False
    assert f["dec"]["nullable"] is True
    assert (f["dec"]["type"]["kind"], f["dec"]["type"]["precision"], f["dec"]["type"]["scale"]) == ("decimal128", 10, 2)
    assert (f["ts"]["type"]["unit"], f["ts"]["type"]["timezone"]) == ("ns", "UTC")
    assert f["lst"]["type"]["kind"] == "list"
    assert f["lst"]["type"]["element"]["type"]["kind"] == "int32"

    leaves = {leaf["path"]: leaf for leaf in doc["schema"]["parquet_leaves"]}
    assert leaves["req"]["physical_type"] == "INT32"
    assert leaves["req"]["max_definition_level"] == 0
    assert leaves["dec"]["physical_type"] == "FIXED_LEN_BYTE_ARRAY"
    assert leaves["dec"]["logical_type"]["Type"] == "Decimal"
    assert (leaves["dec"]["logical_type"]["precision"], leaves["dec"]["logical_type"]["scale"]) == (10, 2)
    assert leaves["ts"]["physical_type"] == "INT64"
    assert leaves["ts"]["logical_type"]["Type"] == "Timestamp"
    assert leaves["ts"]["logical_type"]["isAdjustedToUTC"] is True
    assert leaves["ts"]["logical_type"]["timeUnit"] == "nanoseconds"
    assert leaves["u64"]["logical_type"]["Type"] == "Int"
    assert leaves["u64"]["logical_type"]["isSigned"] is False
    assert leaves["u64"]["logical_type"]["bitWidth"] == 64
    assert leaves["lst.list.element"]["max_repetition_level"] == 1
    assert "optional" in doc["file"]["parquet_schema"] and "required" in doc["file"]["parquet_schema"]


@pytest.mark.parametrize("version", ["1.0", "2.0"])
def test_parquet_row_groups_stats_and_page_version(oracle, version):
    n = 25
    table = pa.table(
        {
            "k": pa.array(range(n), pa.int64()),
            "v": pa.array([None if i % 5 == 0 else i * 10 for i in range(n)], pa.int64()),
            "dec": pa.array([decimal.Decimal(i) / 100 if i % 7 else None for i in range(n)], pa.decimal128(10, 2)),
            "u": pa.array([2**64 - 1 - i for i in range(n)], pa.uint64()),
            "s": pa.array(["s%02d" % i for i in range(n)], pa.string()),
        }
    )
    doc = client.inspect_bytes(pq_bytes(table, row_group_size=10, data_page_version=version), "parquet", oracle)
    assert doc["file"]["num_rows"] == n
    assert doc["file"]["num_row_groups"] == 3
    assert [rg["num_rows"] for rg in doc["row_groups"]] == [10, 10, 5]
    expected_page = "DATA_PAGE" if version == "1.0" else "DATA_PAGE_V2"
    for g, rg in enumerate(doc["row_groups"]):
        lo, hi = g * 10, min(n, g * 10 + 10)
        by_path = {c["path"]: c for c in rg["columns"]}
        for c in rg["columns"]:
            assert c["data_page_versions"] == [1 if version == "1.0" else 2]
            assert [p["type"] for p in c["pages"] if p["type"] != "DICTIONARY_PAGE"] == [expected_page]
        k = by_path["k"]["statistics"]
        assert (k["min"], k["max"], k["null_count"]) == ("i64:%d" % lo, "i64:%d" % (hi - 1), 0)
        v_vals = [i * 10 for i in range(lo, hi) if i % 5]
        v = by_path["v"]["statistics"]
        assert (v["min"], v["max"]) == ("i64:%d" % min(v_vals), "i64:%d" % max(v_vals))
        assert v["null_count"] == sum(1 for i in range(lo, hi) if i % 5 == 0)
        d_vals = [i for i in range(lo, hi) if i % 7]
        d = by_path["dec"]["statistics"]
        assert d["min"] == "dec:%d.%02d" % (min(d_vals) // 100, min(d_vals) % 100)
        assert d["max"] == "dec:%d.%02d" % (max(d_vals) // 100, max(d_vals) % 100)
        u = by_path["u"]["statistics"]
        assert (u["min"], u["max"]) == ("u64:%d" % (2**64 - hi), "u64:%d" % (2**64 - 1 - lo))
        s = by_path["s"]["statistics"]
        assert (s["min"], s["max"]) == ("s%02d" % lo, "s%02d" % (hi - 1))
    assert columns(doc)["k"] == ["i64:%d" % i for i in range(n)]


def test_parquet_multiple_data_pages_are_walked(oracle):
    n = 5000
    table = pa.table({"a": pa.array(range(n), pa.int64())})
    doc = client.inspect_bytes(
        pq_bytes(table, use_dictionary=False, data_page_size=1024, compression="none"), "parquet", oracle
    )
    chunk = doc["row_groups"][0]["columns"][0]
    pages = chunk["pages"]
    assert len(pages) > 3
    assert all(p["type"] == "DATA_PAGE" and p["encoding"] == "PLAIN" for p in pages)
    assert sum(p["num_values"] for p in pages) == n
    assert chunk["encodings"] == ["PLAIN", "RLE"]
    assert columns(doc)["a"][-1] == "i64:4999"


@pytest.mark.parametrize(
    "codec,expected",
    [
        ("NONE", "UNCOMPRESSED"),
        ("snappy", "SNAPPY"),
        ("gzip", "GZIP"),
        ("zstd", "ZSTD"),
        ("lz4", "LZ4"),
        ("brotli", "BROTLI"),
    ],
)
def test_parquet_compression_codecs(oracle, codec, expected):
    if codec != "NONE":
        assert pa.Codec.is_available(codec), "this PyArrow build lacks %s; the oracle host needs it" % codec
    table = pa.table({"a": pa.array(range(200), pa.int64()), "s": pa.array(["x%d" % i for i in range(200)])})
    doc = client.inspect_bytes(pq_bytes(table, compression=codec), "parquet", oracle)
    assert {c["compression"] for c in doc["row_groups"][0]["columns"]} == {expected}
    assert columns(doc)["s"][199] == "x199"


def test_parquet_nested_types(oracle):
    table = pa.table(
        {
            "l": pa.array([[1, None, 3], None, [], [4]], pa.list_(pa.int32())),
            "ll": pa.array([[["a"], None], None, [], [[], ["b", "c"]]], pa.list_(pa.list_(pa.string()))),
            "st": pa.array(
                [{"x": 1, "y": "a"}, None, {"x": None, "y": None}, {"x": 2**40, "y": "d"}],
                pa.struct([("x", pa.int64()), ("y", pa.string())]),
            ),
            "m": pa.array([[("a", 1)], None, [], [("b", 2), ("c", None)]], pa.map_(pa.string(), pa.int32())),
            "ld": pa.array([[decimal.Decimal("1.5")], None, [], [None]], pa.list_(pa.decimal128(5, 1))),
        }
    )
    doc = client.inspect_bytes(pq_bytes(table), "parquet", oracle)
    cols = columns(doc)
    assert cols["l"] == [[1, None, 3], None, [], [4]]
    assert cols["ll"] == [[["a"], None], None, [], [[], ["b", "c"]]]
    assert cols["st"] == [{"x": "i64:1", "y": "a"}, None, {"x": None, "y": None}, {"x": "i64:1099511627776", "y": "d"}]
    assert cols["m"] == [[["a", 1]], None, [], [["b", 2], ["c", None]]]
    assert cols["ld"] == [["dec:1.5"], None, [], [None]]
    f = fields(doc)
    assert f["m"]["type"]["kind"] == "map"
    assert f["m"]["type"]["key"]["type"]["kind"] == "string"
    assert f["st"]["type"]["fields"][0]["type"]["kind"] == "int64"
    assert f["ll"]["type"]["element"]["type"]["element"]["type"]["kind"] == "string"
    assert f["ld"]["type"]["element"]["type"]["scale"] == 1


def test_parquet_empty_and_all_null(oracle):
    empty = client.inspect_bytes(pq_bytes(pa.table({"a": pa.array([], pa.int64())})), "parquet", oracle)
    assert empty["file"]["num_rows"] == 0
    assert columns(empty) == {"a": []}
    nulls = client.inspect_bytes(
        pq_bytes(pa.table({"a": pa.array([None, None, None], pa.int64()), "b": pa.array([None] * 3, pa.string())})),
        "parquet",
        oracle,
    )
    assert columns(nulls) == {"a": [None] * 3, "b": [None] * 3}
    assert nulls["row_groups"][0]["columns"][0]["statistics"]["null_count"] == 3
    assert nulls["row_groups"][0]["columns"][0]["statistics"]["has_min_max"] is False


def test_parquet_dictionary_column_is_decoded(oracle):
    table = pa.table({"d": pa.array(["a", "b", "a", None]).dictionary_encode()})
    doc = client.inspect_bytes(pq_bytes(table), "parquet", oracle)
    assert columns(doc)["d"] == ["a", "b", "a", None]


def test_parquet_key_value_metadata_and_created_by(oracle):
    table = pa.table({"a": pa.array([1])}).replace_schema_metadata({"owner": "rugo-floor"})
    doc = client.inspect_bytes(pq_bytes(table), "parquet", oracle)
    assert doc["file"]["key_value_metadata"]["owner"] == "rugo-floor"
    assert doc["file"]["created_by"].startswith("parquet-cpp-arrow")


# ─────────────────────────────────────────────────────────────────────────────
# ORC
# ─────────────────────────────────────────────────────────────────────────────


def test_orc_scalar_values_are_exact(oracle):
    table = pa.table(
        {
            "i64": pa.array([2**53 + 1, -(2**63), None], pa.int64()),
            "f32": pa.array([NAN, -0.0, 1.1], pa.float32()),
            "f64": pa.array([INF, -INF, 0.1], pa.float64()),
            "dec": pa.array([decimal.Decimal("1.50"), None, decimal.Decimal("-0.01")], pa.decimal128(10, 2)),
            "dec38": pa.array([decimal.Decimal("-1234567890123456789012345678.1234567890"), None, None], pa.decimal128(38, 10)),
            "ts": pa.array([1, 2**40, None], pa.timestamp("ns")),
            "d": pa.array([0, -1, None], pa.date32()),
            "bin": pa.array([b"\x00\xff", b"", None], pa.binary()),
            "txt": pa.array(["héllo", None, "\u0000x"], pa.string()),
            "flag": pa.array([True, None, False], pa.bool_()),
            "i8": pa.array([-128, 127, None], pa.int8()),
        }
    )
    data = orc_bytes(table)
    doc = client.inspect_bytes(data, "orc", oracle)
    assert doc["format"] == "orc"
    assert doc["input"]["sha256"] == hashlib.sha256(data).hexdigest()
    assert columns(doc) == {
        "i64": ["i64:9007199254740993", "i64:-9223372036854775808", None],
        "f32": ["f32:7fc00000:nan", "f32:80000000:-0.0", "f32:3f8ccccd:1.1"],
        "f64": ["f64:7ff0000000000000:inf", "f64:fff0000000000000:-inf", "f64:3fb999999999999a:0.1"],
        "dec": ["dec:1.50", None, "dec:-0.01"],
        "dec38": ["dec:-1234567890123456789012345678.1234567890", None, None],
        "ts": ["ts[ns]:1", "ts[ns]:1099511627776", None],
        "d": ["date32:0", "date32:-1", None],
        "bin": ["b64:AP8=", "b64:", None],
        "txt": ["héllo", None, "\u0000x"],
        "flag": [True, None, False],
        "i8": [-128, 127, None],
    }
    f = fields(doc)
    assert (f["dec"]["type"]["precision"], f["dec"]["type"]["scale"]) == (10, 2)
    assert (f["dec38"]["type"]["precision"], f["dec38"]["type"]["scale"]) == (38, 10)
    assert f["ts"]["type"]["unit"] == "ns"


def test_orc_nested_types(oracle):
    table = pa.table(
        {
            "l": pa.array([[1, None, 3], None, [], [4]], pa.list_(pa.int32())),
            "st": pa.array(
                [{"x": 1, "y": "a"}, None, {"x": None, "y": None}, {"x": 2**40, "y": "d"}],
                pa.struct([("x", pa.int64()), ("y", pa.string())]),
            ),
            "m": pa.array([[("a", 1)], None, [], [("b", 2)]], pa.map_(pa.string(), pa.int32())),
        }
    )
    doc = client.inspect_bytes(orc_bytes(table), "orc", oracle)
    cols = columns(doc)
    assert cols["l"] == [[1, None, 3], None, [], [4]]
    assert cols["st"] == [{"x": "i64:1", "y": "a"}, None, {"x": None, "y": None}, {"x": "i64:1099511627776", "y": "d"}]
    assert cols["m"] == [[["a", 1]], None, [], [["b", 2]]]
    assert fields(doc)["l"]["type"]["kind"] == "list"


@pytest.mark.parametrize(
    "codec,expected",
    [
        ("uncompressed", "UNCOMPRESSED"),
        ("snappy", "SNAPPY"),
        ("zlib", "ZLIB"),
        ("zstd", "ZSTD"),
        ("lz4", "LZ4"),
    ],
)
def test_orc_compression_codecs(oracle, codec, expected):
    table = pa.table({"a": pa.array(range(300), pa.int64())})
    doc = client.inspect_bytes(orc_bytes(table, compression=codec), "orc", oracle)
    assert doc["file"]["compression"] == expected
    assert doc["file"]["num_rows"] == 300
    assert columns(doc)["a"][299] == "i64:299"


def test_orc_multiple_stripes(oracle):
    n = 300000
    table = pa.table({"a": pa.array(range(n), pa.int64()), "s": pa.array([str(i) * 3 for i in range(n)])})
    doc = client.inspect_bytes(orc_bytes(table, stripe_size=64 * 1024, compression="zstd"), "orc", oracle)
    assert doc["file"]["num_stripes"] > 1
    assert len(doc["stripes"]) == doc["file"]["num_stripes"]
    assert sum(s["num_rows"] for s in doc["stripes"]) == n
    assert columns(doc)["a"][n - 1] == "i64:%d" % (n - 1)
    assert columns(doc)["s"][n - 1] == str(n - 1) * 3


def test_orc_empty(oracle):
    doc = client.inspect_bytes(orc_bytes(pa.table({"a": pa.array([], pa.int64())})), "orc", oracle)
    assert doc["file"]["num_rows"] == 0
    assert columns(doc) == {"a": []}


# ─────────────────────────────────────────────────────────────────────────────
# Avro
# ─────────────────────────────────────────────────────────────────────────────

AVRO_SCHEMA = {
    "type": "record",
    "name": "Row",
    "namespace": "floor",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "small", "type": "int"},
        {"name": "flag", "type": "boolean"},
        {"name": "f", "type": "float"},
        {"name": "d", "type": "double"},
        {"name": "txt", "type": ["null", "string"]},
        {"name": "raw", "type": "bytes"},
        {"name": "dec", "type": [{"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}, "null"]},
        {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "tsms", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "day", "type": {"type": "int", "logicalType": "date"}},
        {"name": "tm", "type": {"type": "long", "logicalType": "time-micros"}},
        {"name": "fx", "type": {"type": "fixed", "name": "Quad", "size": 4}},
        {"name": "fx2", "type": "Quad"},
        {"name": "color", "type": {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN"]}},
        {"name": "xs", "type": {"type": "array", "items": ["null", "long"]}},
        {"name": "kv", "type": {"type": "map", "values": "double"}},
        {
            "name": "inner",
            "type": [
                "null",
                {"type": "record", "name": "Inner", "fields": [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]},
            ],
        },
    ],
}


def _utc(*args):
    return datetime.datetime(*args, tzinfo=datetime.timezone.utc)


def _avro_records():
    base = {
        "id": 2**53 + 1,
        "small": -(2**31),
        "flag": True,
        "f": 1.1,
        "d": 0.1,
        "txt": "héllo",
        "raw": b"\x00\xff",
        "dec": decimal.Decimal("-1.50"),
        "ts": _utc(2020, 1, 1, 0, 0, 0, 123456),
        "tsms": _utc(2020, 1, 1, 0, 0, 0, 123000),
        "day": datetime.date(2020, 1, 2),
        "tm": datetime.time(23, 59, 59, 999999),
        "fx": b"abcd",
        "fx2": b"wxyz",
        "color": "GREEN",
        "xs": [1, None, 2**62],
        "kv": {"k": float("inf")},
        "inner": {"a": -(2**63), "b": "z"},
    }
    nulls = dict(
        base,
        id=-(2**63),
        flag=False,
        f=float("nan"),
        d=-0.0,
        txt=None,
        dec=None,
        color="RED",
        xs=[],
        kv={},
        inner=None,
    )
    return [base, nulls]


AVRO_EXPECTED = {
    "id": ["i64:9007199254740993", "i64:-9223372036854775808"],
    "small": [-2147483648, -2147483648],
    "flag": [True, False],
    "f": ["f32:3f8ccccd:1.1", "f32:7fc00000:nan"],
    "d": ["f64:3fb999999999999a:0.1", "f64:8000000000000000:-0.0"],
    "txt": ["héllo", None],
    "raw": ["b64:AP8=", "b64:AP8="],
    "dec": ["dec:-1.50", None],
    "ts": ["ts[us]:1577836800123456", "ts[us]:1577836800123456"],
    "tsms": ["ts[ms]:1577836800123", "ts[ms]:1577836800123"],
    "day": ["date32:18263", "date32:18263"],
    "tm": ["time[us]:86399999999", "time[us]:86399999999"],
    "fx": ["b64:YWJjZA==", "b64:YWJjZA=="],
    "fx2": ["b64:d3h5eg==", "b64:d3h5eg=="],
    "color": ["GREEN", "RED"],
    "xs": [[ "i64:1", None, "i64:4611686018427387904"], []],
    "kv": [[["k", "f64:7ff0000000000000:inf"]], []],
    "inner": [{"a": "i64:-9223372036854775808", "b": "z"}, None],
}


@pytest.mark.parametrize("codec", ["null", "deflate", "bzip2", "snappy", "zstandard"])
def test_avro_values_and_facts(oracle, codec):
    data = avro_bytes(AVRO_SCHEMA, _avro_records(), codec=codec, metadata={"owner": "rugo-floor"})
    doc = client.inspect_bytes(data, "avro", oracle)
    assert doc["format"] == "avro"
    assert doc["reader"].keys() == {"fastavro", "avro"}
    assert doc["file"]["num_rows"] == 2
    assert doc["file"]["codec"] == codec
    assert doc["file"]["user_metadata"] == {"owner": "rugo-floor"}
    assert json.loads(doc["file"]["schema_json"]) == AVRO_SCHEMA
    assert json.loads(doc["file"]["schema_json"])["name"] == "Row"
    assert columns(doc) == AVRO_EXPECTED


def test_avro_schema_description_is_logical_and_nullable(oracle):
    doc = client.inspect_bytes(avro_bytes(AVRO_SCHEMA, _avro_records()), "avro", oracle)
    t = {f["name"]: f["type"] for f in doc["schema"]["fields"]}
    assert doc["schema"]["record"] == "floor.Row"
    assert (t["dec"]["kind"], t["dec"]["logical_type"], t["dec"]["precision"], t["dec"]["scale"]) == ("bytes", "decimal", 10, 2)
    assert t["dec"]["nullable"] is True
    assert t["id"]["nullable"] is False
    assert (t["ts"]["logical_type"], t["ts"]["unit"], t["ts"]["timezone"]) == ("timestamp-micros", "us", "UTC")
    assert (t["tsms"]["unit"], t["day"]["logical_type"], t["tm"]["unit"]) == ("ms", "date", "us")
    assert t["xs"]["kind"] == "array" and t["xs"]["element"]["kind"] == "long" and t["xs"]["element"]["nullable"] is True
    assert t["kv"]["values"]["kind"] == "double"
    assert t["fx2"]["kind"] == "fixed" and t["fx2"]["name"] == "floor.Quad" and t["fx2"]["size"] == 4
    assert t["color"]["symbols"] == ["RED", "GREEN"]
    assert [f["name"] for f in t["inner"]["fields"]] == ["a", "b"]


def test_avro_many_blocks_and_recursive_schema(oracle):
    schema = {
        "type": "record",
        "name": "Node",
        "fields": [{"name": "v", "type": "long"}, {"name": "next", "type": ["null", "Node"]}],
    }
    records = [{"v": i, "next": {"v": -i, "next": None}} for i in range(500)]
    data = avro_bytes(schema, records, sync_interval=64, codec="deflate")
    doc = client.inspect_bytes(data, "avro", oracle)
    assert doc["file"]["num_rows"] == 500
    assert columns(doc)["v"][499] == "i64:499"
    assert columns(doc)["next"][499] == {"v": "i64:-499", "next": None}
    nxt = doc["schema"]["fields"][1]["type"]
    # Node is being defined by the top-level record, so the self-reference is a ref, not an expansion
    assert nxt == {"kind": "ref", "name": "Node", "nullable": True}


def test_avro_multi_branch_union_is_refused_not_guessed(oracle):
    schema = {"type": "record", "name": "R", "fields": [{"name": "u", "type": ["int", "string"]}]}
    with pytest.raises(client.OracleClientError) as err:
        client.inspect_bytes(avro_bytes(schema, [{"u": 1}]), "avro", oracle)
    assert err.value.status == 422
    assert json.loads(str(err.value).split(": ", 1)[1])["error"]["kind"] == "unsupported"


def test_avro_codec_the_reference_reader_lacks_is_an_error_not_a_fallback(oracle):
    # fastavro writes and reads xz; Apache avro does not, so the cross-check cannot be made
    with pytest.raises(client.OracleClientError) as err:
        client.inspect_bytes(avro_bytes(AVRO_SCHEMA, _avro_records(), codec="xz"), "avro", oracle)
    assert err.value.status == 422
    assert "xz" in str(err.value)


def test_avro_non_record_top_level_is_refused(oracle):
    with pytest.raises(client.OracleClientError) as err:
        client.inspect_bytes(avro_bytes("long", [1, 2, 3]), "avro", oracle)
    assert err.value.status == 422


def test_avro_empty_file(oracle):
    doc = client.inspect_bytes(avro_bytes(AVRO_SCHEMA, []), "avro", oracle)
    assert doc["file"]["num_rows"] == 0
    assert columns(doc)["id"] == []


# ─────────────────────────────────────────────────────────────────────────────
# Errors: loud, non-2xx, never partial
# ─────────────────────────────────────────────────────────────────────────────


def raw_post(base_url: str, path: str, body: bytes):
    conn = http.client.HTTPConnection(base_url.split("//")[1])
    conn.request("POST", path, body=body)
    response = conn.getresponse()
    return response.status, json.loads(response.read())


def test_healthz(oracle):
    doc = client.healthz(oracle)
    assert doc["status"] == "ok"
    assert doc["schema_version"] == SCHEMA_VERSION
    assert doc["readers"]["parquet"]["pyarrow"] == pa.__version__
    assert doc["readers"]["avro"]["fastavro"] == fastavro.__version__


@pytest.mark.parametrize("fmt,garbage", [("parquet", b"PAR1" + b"\x00" * 64), ("orc", b"ORC" + b"\x00" * 64), ("avro", b"Obj\x01" + b"\x00" * 64)])
def test_corrupt_file_returns_an_error_status(oracle, fmt, garbage):
    status, body = raw_post(oracle, "/v1/inspect?format=" + fmt, garbage)
    assert status == 422
    assert body["schema_version"] == SCHEMA_VERSION
    assert "columns" not in body
    assert body["error"]["message"]


def test_truncated_real_files_are_rejected(oracle):
    parquet = pq_bytes(pa.table({"a": pa.array(range(100))}))
    orc_file = orc_bytes(pa.table({"a": pa.array(range(100))}))
    avro_file = avro_bytes(AVRO_SCHEMA, _avro_records())
    for fmt, data in (("parquet", parquet), ("orc", orc_file), ("avro", avro_file)):
        for cut in (len(data) // 2, len(data) - 3):
            status, body = raw_post(oracle, "/v1/inspect?format=" + fmt, data[:cut])
            assert status >= 400 and "error" in body and "columns" not in body, (fmt, cut, status)


def test_corrupted_parquet_page_bytes_are_rejected(oracle):
    data = bytearray(pq_bytes(pa.table({"a": pa.array(range(1000))}), compression="zstd"))
    for i in range(8, 40):
        data[i] ^= 0xFF
    status, body = raw_post(oracle, "/v1/inspect?format=parquet", bytes(data))
    assert status >= 400 and "error" in body


def test_wrong_format_parameter_fails(oracle):
    parquet = pq_bytes(pa.table({"a": pa.array([1, 2, 3])}))
    for fmt in ("orc", "avro"):
        status, body = raw_post(oracle, "/v1/inspect?format=" + fmt, parquet)
        assert status == 422 and "error" in body, fmt
    status, body = raw_post(oracle, "/v1/inspect?format=parquet", orc_bytes(pa.table({"a": pa.array([1])})))
    assert status == 422


@pytest.mark.parametrize("query", ["", "?format=csv", "?format=", "?format=parquet&format=orc", "?fmt=parquet"])
def test_bad_format_parameter_is_a_400(oracle, query):
    status, body = raw_post(oracle, "/v1/inspect" + query, b"x")
    assert status == 400
    assert body["error"]["kind"] == "bad_request"


def test_empty_body_is_a_400(oracle):
    status, body = raw_post(oracle, "/v1/inspect?format=parquet", b"")
    assert status == 400


def test_missing_content_length_is_a_411(oracle):
    host, port = oracle.split("//")[1].split(":")
    with socket.create_connection((host, int(port)), timeout=10) as s:
        s.sendall(b"POST /v1/inspect?format=parquet HTTP/1.0\r\n\r\n")
        reply = s.recv(4096)
    assert reply.startswith(b"HTTP/1.0 411") or reply.startswith(b"HTTP/1.1 411")


def test_unknown_paths_and_methods(oracle):
    status, body = raw_post(oracle, "/v1/other", b"x")
    assert status == 404
    conn = http.client.HTTPConnection(oracle.split("//")[1])
    conn.request("GET", "/nope")
    assert conn.getresponse().status == 404


def test_oversize_body_is_rejected():
    server = service.start_in_thread("127.0.0.1", 0, 1000)
    try:
        base = "http://127.0.0.1:%d" % server.server_port
        with pytest.raises(client.OracleClientError) as err:
            client.inspect_bytes(b"\x00" * 2000, "parquet", base)
        assert err.value.status == 413
        assert json.loads(str(err.value).split(": ", 1)[1])["error"]["kind"] == "too_large"
        assert client.healthz(base)["max_body_bytes"] == 1000
        # a body exactly at the limit is accepted by the limit check and judged by the reader
        status, body = raw_post(base, "/v1/inspect?format=parquet", b"\x00" * 1000)
        assert status == 422
    finally:
        server.shutdown()
        server.server_close()


def test_unreachable_service_makes_the_client_raise():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    with pytest.raises(client.OracleClientError) as err:
        client.inspect_bytes(b"PAR1", "parquet", "http://127.0.0.1:%d" % port, timeout=5)
    assert err.value.status is None
    assert "unreachable" in str(err.value)
    with pytest.raises(client.OracleClientError):
        client.healthz("http://127.0.0.1:%d" % port, timeout=5)


def test_client_rejects_unknown_format_before_any_network_call():
    with pytest.raises(client.OracleClientError):
        client.inspect_bytes(b"x", "csv", "http://127.0.0.1:1")


def test_client_inspect_path(oracle, tmp_path):
    path = tmp_path / "f.parquet"
    path.write_bytes(pq_bytes(pa.table({"a": pa.array([1, 2])})))
    doc = client.inspect_path(str(path), "parquet", oracle)
    assert columns(doc) == {"a": ["i64:1", "i64:2"]}


# ─────────────────────────────────────────────────────────────────────────────
# Independence
# ─────────────────────────────────────────────────────────────────────────────


def test_service_imports_no_rugo_draken_opteryx_or_numpy():
    code = (
        "import sys; sys.path.insert(0, %r); import oracle_service.service, oracle_service.client; "
        "bad = sorted(m for m in sys.modules if m.split('.')[0] in ('rugo', 'draken', 'opteryx')); "
        "assert not bad, bad"
    ) % str(SERVICE_DIR)
    subprocess.run([sys.executable, "-I", "-c", code], check=True)
    for source in (SERVICE_DIR / "oracle_service").glob("*.py"):
        imports = [l.split()[1].split(".")[0] for l in source.read_text().splitlines() if l.startswith(("import ", "from "))]
        for banned in ("rugo", "draken", "opteryx", "numpy"):
            assert banned not in imports, "%s imports %s" % (source.name, banned)


def test_client_module_needs_no_third_party_packages():
    code = (
        "import sys; sys.path.insert(0, %r); import oracle_service.client; "
        "bad = sorted(m for m in sys.modules if m.split('.')[0] in ('pyarrow', 'fastavro', 'avro', 'numpy')); "
        "assert not bad, bad"
    ) % str(SERVICE_DIR)
    subprocess.run([sys.executable, "-I", "-c", code], check=True)
