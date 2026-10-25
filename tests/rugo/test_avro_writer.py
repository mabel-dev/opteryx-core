# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The native Avro writer (rugo.avro.write_avro), docs/AVRO_WRITER_DESIGN.md.

Three oracles read every file written here: fastavro, the Apache `avro` package, and
rugo's own reader (a round trip). Phase 1 scope: scalars without a logical type, the
`null` and `deflate` codecs, one-shot writes.
"""

import io
import json
import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import fastavro  # noqa: E402
from avro.datafile import DataFileReader  # noqa: E402
from avro.io import DatumReader  # noqa: E402

import opteryx  # noqa: E402
from draken.interop.vector_sequence import vector_from_sequence  # noqa: E402
from draken.morsels.morsel import Morsel  # noqa: E402
from rugo import avro, parquet  # noqa: E402

REPO_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
MISSIONS = os.path.join(REPO_ROOT, "testdata", "missions", "space_missions.parquet")
SYNC = bytes(range(16))


def _sql_morsel(sql):
    return Morsel.combine(list(opteryx.session().execute_to_morsels(sql)))


def _morsel(**columns):
    """{name: (dtype, values)} -> Morsel."""
    names, vectors = [], []
    for name, (dtype, values) in columns.items():
        names.append(name)
        vectors.append(vector_from_sequence(values, dtype=dtype))
    return Morsel.from_vectors(names, vectors)


def _fastavro_rows(data):
    return list(fastavro.reader(io.BytesIO(data)))


def _apache_rows(data):
    with DataFileReader(io.BytesIO(data), DatumReader()) as r:
        return list(r)


def _rugo_columns(data):
    out = {}
    with avro.read_avro(data) as r:
        for morsel in r:
            for name in morsel.column_names:
                out.setdefault(name.decode(), []).extend(morsel.column(name).to_pylist())
    return out


def _columns_of(rows, names):
    return {n: [r[n] for r in rows] for n in names}


def _check_all_oracles(data, expected):
    """expected: {name: [python values]} — what every reader must see."""
    names = list(expected)
    assert _columns_of(_fastavro_rows(data), names) == expected
    assert _columns_of(_apache_rows(data), names) == expected
    got = _rugo_columns(data)
    assert set(got) == set(expected)
    for n in names:
        assert got[n] == expected[n], n


# ---------------- types ----------------

def test_scalars_with_nulls_every_oracle():
    m = _sql_morsel(
        "SELECT * FROM (VALUES "
        "(1, 1.5, true, 'short', 'a string longer than twelve bytes'),"
        "(-9000000000, -3.25, false, 'x', ''),"
        "(NULL, NULL, NULL, NULL, NULL)) AS t(i, d, b, s, l)"
    )
    data = avro.write_avro(m, codec="null")
    _check_all_oracles(data, {
        "i": [1, -9000000000, None],
        "d": [1.5, -3.25, None],
        "b": [True, False, None],
        "s": ["short", "x", None],
        "l": ["a string longer than twelve bytes", "", None],
    })


def test_narrow_ints_and_float32_and_bytes():
    m = _morsel(
        i8=("INT8", [-128, 127, None, 0]),
        i32=("INT32", [-2147483648, 2147483647, None, 7]),
        f32=("FLOAT32", [1.5, -0.25, None, 1024.0]),
        blob=("VARBINARY", [b"\x00\xff\x10", b"", None, b"bytes"]),
    )
    data = avro.write_avro(m, codec="null")
    _check_all_oracles(data, {
        "i8": [-128, 127, None, 0],
        "i32": [-2147483648, 2147483647, None, 7],
        "f32": [1.5, -0.25, None, 1024.0],
        "blob": [b"\x00\xff\x10", b"", None, b"bytes"],
    })
    schema = avro.read_metadata(data).schema
    types = {f["name"]: f["type"] for f in schema["fields"]}
    assert types == {
        "i8": ["null", "int"],
        "i32": ["null", "int"],
        "f32": ["null", "float"],
        "blob": ["null", "bytes"],
    }


def test_int64_extremes_and_varint_lengths():
    values = [0, 1, -1, 63, 64, -64, -65, 2**31 - 1, -2**31, 2**63 - 1, -2**63, None]
    data = avro.write_avro(_morsel(v=("INT64", values)), codec="null")
    _check_all_oracles(data, {"v": values})


def test_schema_text_and_header_metadata():
    m = _morsel(a=("INT64", [1]), b=("VARCHAR", ["x"]))
    data = avro.write_avro(m, codec="null", record_name="launch")
    meta = avro.read_metadata(data)
    assert meta.codec == "null"
    assert meta.schema == {
        "type": "record",
        "name": "launch",
        "namespace": "rugo",
        "fields": [
            {"name": "a", "type": "long"},     # no nulls: required (a one-shot write)
            {"name": "b", "type": "string"},   # one 1-byte value: an enum would be larger
        ],
    }
    import rugo
    assert meta.metadata["rugo.created_by"].decode() == rugo.__writer_id__


def test_dict_and_constant_shaped_input_write_logical_rows():
    """Dictionary- and constant-shaped columns are written through the uniform
    data[selection[i]] path: logical rows, not the dictionary."""
    from draken.vectors.vector import Vector

    values = ["b", "a", None, "b", "a long value well over twelve bytes", "b", None, "a"]
    dense = vector_from_sequence(values, dtype="VARCHAR")
    dict_vec = Vector(dense.dictionary_encode())
    assert dict_vec.is_dict and dict_vec.to_pylist() == values
    ints = vector_from_sequence([7, 7, 7, 7, 7, 7, 7, 7], dtype="INT64")
    const_vec = Vector(ints.dictionary_encode())
    assert const_vec.is_constant
    m = Morsel.from_vectors(["s", "k"], [dict_vec, const_vec])
    data = avro.write_avro(m, codec="null")
    _check_all_oracles(data, {"s": values, "k": [7] * 8})


def test_zero_rows_is_a_valid_empty_file():
    m = _morsel(a=("INT64", []), b=("VARCHAR", []))
    data = avro.write_avro(m, codec="null")
    assert _fastavro_rows(data) == []
    assert _apache_rows(data) == []
    assert avro.read_metadata(data).schema["fields"][1]["name"] == "b"
    assert _rugo_columns(data) == {}  # a file with no records yields no morsel


# ---------------- container ----------------

def test_deflate_default_compresses_and_reads_everywhere():
    m = _sql_morsel("SELECT 'the same string again and again' AS s, 42 AS i FROM GENERATE_SERIES(1, 2000) AS gx")
    z = avro.write_avro(m)  # deflate is the default
    u = avro.write_avro(m, codec="null")
    assert len(z) < len(u) / 10
    assert avro.read_metadata(z).codec == "deflate"
    expected = {"s": ["the same string again and again"] * 2000, "i": [42] * 2000}
    _check_all_oracles(z, expected)
    _check_all_oracles(u, expected)


def test_block_bytes_cuts_blocks_and_sync_marker_is_honoured():
    n = 5000
    m = _morsel(v=("INT64", list(range(n))))
    data = avro.write_avro(m, codec="null", block_bytes=1024, sync_marker=SYNC)
    blocks = list(fastavro.block_reader(io.BytesIO(data)))
    assert len(blocks) > 1
    assert sum(b.num_records for b in blocks) == n
    # Every row is 1 (branch) + varint(zigzag(v)) bytes: block_bytes is a floor, so each
    # block but the last holds at least 1024 bytes of payload.
    assert all(len(b.bytes_.getvalue()) >= 1024 for b in blocks[:-1])
    assert data.count(SYNC) == len(blocks) + 1  # header + one per block
    assert data.endswith(SYNC)
    assert _rugo_columns(data) == {"v": list(range(n))}


def test_same_rows_same_sync_same_bytes():
    m = _morsel(v=("INT64", [1, 2, 3]), s=("VARCHAR", ["a", None, "c"]))
    assert avro.write_avro(m, sync_marker=SYNC) == avro.write_avro(m, sync_marker=SYNC)
    assert avro.write_avro(m) != avro.write_avro(m)  # random markers differ


def test_space_missions_round_trip_through_parquet():
    with parquet.read_parquet(MISSIONS, columns=["Company", "Location", "Mission"]) as r:
        m = Morsel.combine(list(r))
    data = avro.write_avro(m)
    expected = {c: m.column(c.encode()).to_pylist() for c in ("Company", "Location", "Mission")}
    _check_all_oracles(data, expected)


# ---------------- refusals ----------------

def test_invalid_and_duplicate_names_are_refused():
    with pytest.raises(ValueError, match=r"column name 'count\(\*\)' is not a valid Avro name"):
        avro.write_avro(_morsel(**{"count(*)": ("INT64", [1])}))
    with pytest.raises(ValueError, match=r"column name 'Rocket\.Name' is not a valid Avro name"):
        avro.write_avro(_morsel(**{"Rocket.Name": ("VARCHAR", ["x"])}))
    with pytest.raises(ValueError, match="column name 'a' appears more than once"):
        avro.write_avro(Morsel.from_vectors(["a", "a"], [vector_from_sequence([1], dtype="INT64")] * 2))
    with pytest.raises(ValueError, match="record name '9x' is not a valid Avro name"):
        avro.write_avro(_morsel(a=("INT64", [1])), record_name="9x")


def test_no_columns_is_refused():
    with pytest.raises(ValueError, match="no columns"):
        avro.write_avro(Morsel.from_vectors([], []))


def test_unknown_codec_is_refused():
    m = _morsel(a=("INT64", [1]))
    with pytest.raises(ValueError, match="codec 'lzma' is not an Avro codec"):
        avro.write_avro(m, codec="lzma")
    with pytest.raises(ValueError, match="codec 'xz' is not an Avro codec"):
        avro.write_avro(m, codec="xz")


def test_refused_types_fail_loud():
    with pytest.raises(ValueError, match="column 'u' is UINT64, which Avro has no type for"):
        avro.write_avro(_sql_morsel("SELECT CAST(1 AS UINT64) AS u"))
    with pytest.raises(ValueError, match="column 'iv' is INTERVAL"):
        avro.write_avro(_sql_morsel("SELECT INTERVAL '1' DAY AS iv"))


def test_bad_sync_marker_and_block_bytes():
    m = _morsel(a=("INT64", [1]))
    with pytest.raises(ValueError, match="sync_marker must be exactly 16 bytes"):
        avro.write_avro(m, sync_marker=b"short")
    with pytest.raises(ValueError, match="block_bytes must be positive"):
        avro.write_avro(m, block_bytes=0)


# ---------------- phase 2: logical types ----------------

def _native_morsel(**columns):
    """{name: native draken Vector} -> Morsel."""
    from draken.vectors.vector import Vector

    return Morsel.from_vectors(list(columns), [Vector(v) for v in columns.values()])


def _field_types(data):
    """Each field's value type: T of a `["null", T]` union, or the plain type of a
    required field (see _field_unions for which is which)."""
    return {f["name"]: (f["type"][1] if type(f["type"]) is list else f["type"])
            for f in avro.read_metadata(data).schema["fields"]}


def _field_unions(data):
    return {f["name"]: type(f["type"]) is list for f in avro.read_metadata(data).schema["fields"]}


def test_date_time_timestamp_every_unit():
    import datetime as dt
    import draken.draken_native as dn

    utc = dt.timezone.utc
    ts = dt.datetime(2020, 1, 2, 3, 4, 5, 123000, tzinfo=utc)
    ts_s = dt.datetime(2020, 1, 2, 3, 4, 5, tzinfo=utc)
    t = dt.time(1, 2, 3, 123000)
    t_s = dt.time(1, 2, 3)
    t_us = dt.time(1, 2, 3, 123456)
    m = _native_morsel(
        d=vector_from_sequence([dt.date(2020, 1, 2), dt.date(1969, 12, 31), None], dtype="DATE"),
        ts_s=dn.vector_timestamp_from_sequence([ts_s, None, ts_s], "s", 0),
        ts_ms=dn.vector_timestamp_from_sequence([ts, None, ts], "ms", 0),
        ts_us=dn.vector_timestamp_from_sequence([ts, None, ts], "us", 0),
        t32_s=dn.vector_time32_from_sequence([t_s, None, t_s], "s"),
        t32_ms=dn.vector_time32_from_sequence([t, None, t], "ms"),
        t64_us=dn.vector_time64_from_sequence([t_us, None, t_us], "us"),
    )
    data = avro.write_avro(m, codec="null")
    assert _field_types(data) == {
        "d": {"type": "int", "logicalType": "date"},
        "ts_s": {"type": "long", "logicalType": "timestamp-millis"},
        "ts_ms": {"type": "long", "logicalType": "timestamp-millis"},
        "ts_us": {"type": "long", "logicalType": "timestamp-micros"},
        "t32_s": {"type": "int", "logicalType": "time-millis"},
        "t32_ms": {"type": "int", "logicalType": "time-millis"},
        "t64_us": {"type": "long", "logicalType": "time-micros"},
    }
    # fastavro and Apache avro both hand back datetime objects (UTC-aware timestamps).
    want = {
        "d": [dt.date(2020, 1, 2), dt.date(1969, 12, 31), None],
        "ts_s": [ts_s, None, ts_s],
        "ts_ms": [ts, None, ts],
        "ts_us": [ts, None, ts],
        "t32_s": [t_s, None, t_s],
        "t32_ms": [t, None, t],
        "t64_us": [t_us, None, t_us],
    }
    assert _columns_of(_fastavro_rows(data), list(want)) == want
    apache = _columns_of(_apache_rows(data), list(want))
    assert apache == want
    # rugo reads every temporal as microseconds (TIME / TIMESTAMP) or DATE.
    got = _rugo_columns(data)
    assert got == want
    with avro.read_avro(data) as r:
        morsel = next(iter(r))
    assert morsel.column(b"ts_ms").type_name == "TIMESTAMP[us]"
    assert morsel.column(b"d").type_name == "DATE"


def test_sql_time_on_int64_storage():
    """CAST(... AS TIME) in the engine yields INT64 storage with the TIME descriptor."""
    import datetime as dt

    m = _sql_morsel("SELECT CAST('03:04:05.123456' AS TIME) AS tm")
    assert m.column(b"tm").type_name == "INT64"
    data = avro.write_avro(m, codec="null")
    assert _field_types(data)["tm"] == {"type": "long", "logicalType": "time-micros"}
    assert _columns_of(_fastavro_rows(data), ["tm"]) == {"tm": [dt.time(3, 4, 5, 123456)]}


def test_decimal_minimal_twos_complement():
    import decimal
    import draken.draken_native as dn

    D = decimal.Decimal
    small = [D("12.345"), D("-12.345"), D("0.000"), D("0.127"), D("0.128"), D("-0.128"), D("-0.129"),
             D("9999999.999"), D("-9999999.999"), None]
    wide = [D("1234567890123456789012345.67890"), D("-1234567890123456789012345.67890"), D("0.00000"),
            D("-0.00001"), None, D("0.00127"), D("0.00128"), D("-0.00128"), D("9999999999999999999999999.99999"), None]
    m = _native_morsel(
        d=dn.vector_decimal_from_sequence(small, 10, 3),
        w=dn.vector_decimal128_from_sequence(wide, 30, 5),
    )
    data = avro.write_avro(m, codec="null")
    assert _field_types(data) == {
        "d": {"type": "fixed", "name": "decimal_10_3", "namespace": "rugo", "size": 5,
              "logicalType": "decimal", "precision": 10, "scale": 3},
        "w": {"type": "fixed", "name": "decimal_30_5", "namespace": "rugo", "size": 13,
              "logicalType": "decimal", "precision": 30, "scale": 5},
    }
    want = {"d": small, "w": wide}
    # Both oracles AND draken's to_pylist build Decimals through the thread's context,
    # which rounds a 30-digit value at the default precision of 28; widen it for the
    # comparison (the bytes in the file are exact either way — see the payload check).
    with decimal.localcontext() as ctx:
        ctx.prec = 40
        assert _columns_of(_fastavro_rows(data), ["d", "w"]) == want
        assert _columns_of(_apache_rows(data), ["d", "w"]) == want
        assert _rugo_columns(data) == want
    # Fixed width, big-endian two's complement, sign-extended: DECIMAL(10, 3) is 5 bytes.
    blocks = list(fastavro.block_reader(io.BytesIO(data)))
    payload = blocks[0].bytes_.getvalue()
    assert b"\x02\x00\x00\x00\x00\x7f" in payload   # branch 1, 0.127 (unscaled 127)
    assert b"\x02\x00\x00\x00\x00\x80" in payload   # branch 1, 0.128 (128)
    assert b"\x02\xff\xff\xff\xff\x80" in payload   # branch 1, -0.128 (-128)


def test_uuid_fixed16_and_null_column_and_ipv4():
    import uuid
    import draken.draken_native as dn

    u = uuid.UUID("123E4567-E89B-12D3-A456-426614174000")
    m = _sql_morsel(
        "SELECT CAST('123E4567-E89B-12D3-A456-426614174000' AS UUID) AS u, NULL AS n, "
        "CAST('10.0.0.1' AS IPV4) AS ip, CAST('255.255.255.255' AS IPV4) AS top"
    )
    assert m.column(b"n").type_name == "NULL"
    data = avro.write_avro(m, codec="null")
    assert _field_types(data) == {
        "u": {"type": "fixed", "name": "uuid", "namespace": "rugo", "size": 16, "logicalType": "uuid"},
        "n": "int",
        "ip": {"type": "long", "opteryx.type": "IPV4"},
        "top": {"type": "long", "opteryx.type": "IPV4"},
    }
    # The typeless NULL column keeps its union even in a one-shot write; the rest are required.
    assert _field_unions(data) == {"u": False, "n": True, "ip": False, "top": False}
    # Neither Python oracle applies `uuid` to a fixed (Avro 1.12 allows it; both hand back
    # the 16 bytes, Apache with a warning); the bytes are the UUID's own.
    want = [{"u": u.bytes, "n": None, "ip": 167772161, "top": 4294967295}]
    assert _fastavro_rows(data) == want
    assert _apache_rows(data) == want
    # rugo honours the side channel: the column comes back as IPV4, the uuid as UUID.
    with avro.read_avro(data) as r:
        morsel = next(iter(r))
    assert morsel.column(b"u").type_name == "UUID"
    assert morsel.column(b"u").to_pylist() == [u]
    assert morsel.column(b"ip").type_name == "IPV4"
    assert morsel.column(b"ip").to_pylist() == ["10.0.0.1"]
    assert morsel.column(b"top").to_pylist() == ["255.255.255.255"]
    assert morsel.column(b"n").to_pylist() == [None]


def test_ipv4_round_trips_through_read_avro_in_the_engine(tmp_path):
    m = _sql_morsel("SELECT CAST('192.168.1.9' AS IPV4) AS ip, 1 AS k")
    path = tmp_path / "ip.avro"
    path.write_bytes(avro.write_avro(m))
    got = _sql_morsel(f"SELECT ip, k FROM READ_AVRO('{path}')")
    assert got.column(b"ip").type_name == "IPV4"
    assert got.column(b"ip").to_pylist() == ["192.168.1.9"]


def test_reader_refuses_an_out_of_range_ipv4_long():
    """A foreign file carrying the attribute with a value no IPv4 can hold is corrupt."""
    schema = {"type": "record", "name": "r", "fields": [
        {"name": "ip", "type": {"type": "long", "opteryx.type": "IPV4"}}]}
    buf = io.BytesIO()
    fastavro.writer(buf, schema, [{"ip": 1}, {"ip": 2**32}])
    with pytest.raises(RuntimeError, match="IPV4 value \\(4294967296\\) is outside 0..4294967295"):
        _rugo_columns(buf.getvalue())
    # The same attribute on another type is simply ignored, as the spec says.
    schema["fields"][0]["type"] = {"type": "string", "opteryx.type": "IPV4"}
    buf = io.BytesIO()
    fastavro.writer(buf, schema, [{"ip": "x"}])
    assert _rugo_columns(buf.getvalue()) == {"ip": ["x"]}


def test_nanoseconds_and_missing_descriptors_are_refused():
    import datetime as dt
    import draken.draken_native as dn

    ts = dt.datetime(2020, 1, 2, tzinfo=dt.timezone.utc)
    with pytest.raises(ValueError, match="column 'ts' is in nanoseconds"):
        avro.write_avro(_native_morsel(ts=dn.vector_timestamp_from_sequence([ts], "ns", 0)))
    with pytest.raises(ValueError, match="column 't' is in nanoseconds"):
        avro.write_avro(_native_morsel(t=dn.vector_time64_from_sequence([dt.time(1, 2)], "ns")))


# ---------------- phase 3: codecs ----------------

def _codec_morsel():
    import datetime as dt
    import decimal

    n = 3000
    return _native_morsel(
        i=vector_from_sequence([None if k % 7 == 0 else k * 1000003 for k in range(n)], dtype="INT64"),
        s=vector_from_sequence([f"row {k % 50} of the same few strings" for k in range(n)], dtype="VARCHAR"),
        f=vector_from_sequence([k / 8 for k in range(n)], dtype="FLOAT64"),
        d=vector_from_sequence([dt.date(2020, 1, 1 + k % 28) for k in range(n)], dtype="DATE"),
    )


@pytest.mark.parametrize("codec", ["null", "deflate", "snappy", "zstandard", "bzip2"])
def test_every_codec_reads_in_every_oracle(codec):
    m = _codec_morsel()
    data = avro.write_avro(m, codec=codec, block_bytes=16 * 1024)
    assert avro.read_metadata(data).codec == codec
    blocks = list(fastavro.block_reader(io.BytesIO(data)))
    assert len(blocks) > 1 and sum(b.num_records for b in blocks) == 3000
    want = {c: m.column(c.encode()).to_pylist() for c in ("i", "s", "f", "d")}
    _check_all_oracles(data, want)
    if codec != "null":
        assert len(data) < len(avro.write_avro(m, codec="null")) / 2


def test_snappy_crc_is_the_crc_of_the_uncompressed_block():
    """A one-byte change to a snappy block's payload is caught by the reader's CRC check
    (so the trailer written is the CRC the readers expect, not a constant)."""
    m = _morsel(s=("VARCHAR", ["abc"] * 64))
    data = bytearray(avro.write_avro(m, codec="snappy", sync_marker=SYNC))
    header_end = data.index(SYNC) + 16
    # block: varint count, varint size, payload (snappy bytes + 4-byte CRC), sync
    pos = header_end + 2  # count=64 -> 0x80 0x01 (two bytes); size follows
    # Flip a byte in the middle of the compressed payload, not in the CRC trailer.
    data[pos + 4] ^= 0x01
    with pytest.raises(RuntimeError, match="snappy: (CRC32 mismatch|the compressed block is corrupt)"):
        _rugo_columns(bytes(data))
    # and python-snappy / fastavro refuse it too, or see different values
    try:
        rows = _fastavro_rows(bytes(data))
    except Exception:  # noqa: BLE001 — the oracle's own error type is not ours to pin
        rows = None
    assert rows != [{"s": "abc"}] * 64


def test_bzip2_block_is_one_stream_and_zstandard_is_one_frame():
    import bz2
    import zstandard

    m = _morsel(v=("INT64", list(range(500))))
    for codec, check in (("bzip2", lambda b: bz2.decompress(b)),
                         ("zstandard", lambda b: zstandard.ZstdDecompressor().decompress(b))):
        data = avro.write_avro(m, codec=codec, sync_marker=SYNC)
        block = list(fastavro.block_reader(io.BytesIO(data)))[0]
        # fastavro has already decompressed; the raw stream is what sits in the file.
        raw_null = avro.write_avro(m, codec="null", sync_marker=SYNC)
        null_block = list(fastavro.block_reader(io.BytesIO(raw_null)))[0]
        assert block.bytes_.getvalue() == null_block.bytes_.getvalue()
        # The compressed bytes in the file decompress with the stock library in one call.
        start = data.index(SYNC) + 16
        # skip varint count and varint size
        p = start
        while data[p] & 0x80: p += 1
        p += 1
        size = 0; shift = 0
        while True:
            b = data[p]; p += 1
            size |= (b & 0x7F) << shift
            if not b & 0x80: break
            shift += 7
        size >>= 1
        assert check(data[p:p + size]) == null_block.bytes_.getvalue()


# ---------------- phase 4: ARRAY ----------------

def test_flat_arrays_of_every_plain_scalar():
    import draken.draken_native as dn

    ints = [[1, 2, 3], [], None, [4, None, 6], [-2**63, 2**63 - 1]]
    floats = [[1.5, None], [], None, [0.0, -0.25], [1e300]]
    bools = [[True, False, None], None, [], [True], [False]]
    strs = [["a", "a long string over twelve bytes"], None, [], ["", None], ["z"]]
    bins = [[b"\x00\xff", None], None, [], [b""], [b"xyz"]]
    m = _native_morsel(
        i=dn.vector_array_from_sequence(ints),
        f=dn.vector_array_from_sequence(floats),
        b=dn.vector_array_from_sequence(bools),
        s=dn.vector_array_from_sequence(strs),
        x=dn.vector_array_from_sequence(bins),
    )
    data = avro.write_avro(m, codec="null")
    assert _field_types(data) == {
        "i": {"type": "array", "items": ["null", "long"]},
        "f": {"type": "array", "items": ["null", "double"]},
        "b": {"type": "array", "items": ["null", "boolean"]},
        "s": {"type": "array", "items": ["null", "string"]},
        "x": {"type": "array", "items": ["null", "bytes"]},
    }
    want = {"i": ints, "f": floats, "b": bools, "s": strs, "x": bins}
    # rugo reads an array of a plain scalar back as ARRAY, value for value.
    _check_all_oracles(data, want)
    # Byte-level: [] is a single 0 byte after the branch; null list is the null branch.
    payload = list(fastavro.block_reader(io.BytesIO(data)))[0].bytes_.getvalue()
    assert payload.startswith(b"\x02\x06\x02\x02\x02\x04\x02\x06\x00")  # i row 0: branch, count 3, items 1 2 3, end


def test_nested_arrays_write_and_read_back_as_a_nested_array():
    import draken.draken_native as dn

    rows = [[[1, 2], [3]], None, [[], [None]], [None, [4, 5, 6]], []]
    m = _native_morsel(n=dn.vector_array_from_sequence(rows))
    data = avro.write_avro(m, codec="deflate")
    assert _field_types(data)["n"] == {
        "type": "array",
        "items": ["null", {"type": "array", "items": ["null", "long"]}],
    }
    assert _columns_of(_fastavro_rows(data), ["n"]) == {"n": rows}
    assert _columns_of(_apache_rows(data), ["n"]) == {"n": rows}
    # rugo reads an array of arrays of a plain scalar as a nested ARRAY, as the parquet scan does.
    assert _rugo_columns(data)["n"] == rows


def test_array_of_timestamps_is_written_with_its_logical_type():
    """ARRAY<TIMESTAMP> from the engine (array_child retagged as TIMESTAMP64): the items
    carry the logical type; fastavro and Apache avro read datetimes. rugo's reader gives an
    array of a logical type back as a JSON array (no Draken ARRAY child for it)."""
    import datetime as dt

    m = _sql_morsel(
        "SELECT ARRAY_AGG(CAST(ts AS TIMESTAMP)) AS tss FROM "
        "(VALUES (1, '2020-01-02 03:04:05'), (1, '2021-06-07 08:09:10')) AS t(g, ts) GROUP BY g"
    )
    child = m.column(b"tss")._nb.array_child
    assert child.type.name == "TIMESTAMP64", child.type
    data = avro.write_avro(m, codec="null")
    assert _field_types(data)["tss"] == {
        "type": "array", "items": ["null", {"type": "long", "logicalType": "timestamp-micros"}]}
    utc = dt.timezone.utc
    want = [[dt.datetime(2020, 1, 2, 3, 4, 5, tzinfo=utc), dt.datetime(2021, 6, 7, 8, 9, 10, tzinfo=utc)]]
    assert _columns_of(_fastavro_rows(data), ["tss"]) == {"tss": want}
    assert _columns_of(_apache_rows(data), ["tss"]) == {"tss": want}
    assert [json.loads(x) for x in _rugo_columns(data)["tss"]] == [["2020-01-02T03:04:05+00:00", "2021-06-07T08:09:10+00:00"]]


def test_array_column_with_no_elements_anywhere():
    """Every row null or empty: the child (INT64 here, zero elements) still types the
    items; only `0` terminators and null branches are written."""
    import draken.draken_native as dn

    v = dn.vector_array_from_sequence([[], None, []])
    assert v.array_child.length == 0
    data = avro.write_avro(_native_morsel(a=v), codec="null")
    assert _field_types(data)["a"] == {"type": "array", "items": ["null", "long"]}
    _check_all_oracles(data, {"a": [[], None, []]})


def test_array_in_a_multi_block_file_with_other_columns():
    import draken.draken_native as dn

    n = 4000
    arrays = [None if k % 5 == 0 else list(range(k % 7)) for k in range(n)]
    m = _native_morsel(
        a=dn.vector_array_from_sequence(arrays),
        k=vector_from_sequence(list(range(n)), dtype="INT64"),
    )
    data = avro.write_avro(m, codec="zstandard", block_bytes=4096)
    assert len(list(fastavro.block_reader(io.BytesIO(data)))) > 1
    _check_all_oracles(data, {"a": arrays, "k": list(range(n))})


# ---------------- phase 5: parallel blocks, streaming ----------------

def _wide_morsel(n=20000):
    """Every encoder family in one morsel: ints, floats, strings, arrays, logical types."""
    import datetime as dt
    import decimal
    import draken.draken_native as dn

    utc = dt.timezone.utc
    return _native_morsel(
        i=vector_from_sequence([None if k % 7 == 0 else k * 1000003 for k in range(n)], dtype="INT64"),
        s=vector_from_sequence([f"value {k % 97} " * (1 + k % 3) for k in range(n)], dtype="VARCHAR"),
        f=vector_from_sequence([k / 8 for k in range(n)], dtype="FLOAT64"),
        ts=dn.vector_timestamp_from_sequence(
            [dt.datetime(2020, 1, 1, tzinfo=utc) + dt.timedelta(seconds=k) for k in range(n)], "us", 0),
        dec=dn.vector_decimal_from_sequence([decimal.Decimal(k) / 100 for k in range(n)], 12, 2),
        a=dn.vector_array_from_sequence([None if k % 11 == 0 else list(range(k % 5)) for k in range(n)]),
    )


@pytest.mark.parametrize("codec", ["null", "deflate", "snappy", "zstandard", "bzip2"])
def test_parallel_blocks_are_byte_identical_to_serial(codec):
    m = _wide_morsel()
    one = avro.write_avro(m, codec=codec, block_bytes=8192, sync_marker=SYNC, max_threads=1)
    many = avro.write_avro(m, codec=codec, block_bytes=8192, sync_marker=SYNC, max_threads=8)
    every = avro.write_avro(m, codec=codec, block_bytes=8192, sync_marker=SYNC)  # 0 = every core
    assert len(list(fastavro.block_reader(io.BytesIO(one)))) > 8
    assert one == many == every
    assert _rugo_columns(many)["i"] == m.column(b"i").to_pylist()


def test_parallel_failure_is_the_one_a_serial_run_raises():
    """Two out-of-range time-millis values, in different blocks and length-pass ranges:
    every thread count reports the FIRST one, as a serial run does."""
    import draken.draken_native as dn

    values = [1000] * 300000
    values[70000] = 3_000_000_000
    values[250000] = 4_000_000_000
    t = dn.vector_reinterpret_as_time64(dn.vector_int64_from_sequence(values), "ms")
    m = _native_morsel(t=t)
    for threads in (1, 2, 8, 0):
        with pytest.raises(RuntimeError, match=r"time-millis value \(3000000000\)"):
            avro.write_avro(m, block_bytes=4096, max_threads=threads)


def test_one_batch_stream_reads_like_one_shot_with_the_stream_schema():
    """A stream cannot see later batches: every field keeps `["null", T]` and strings
    stay strings, where the one-shot write of the same rows makes no-null columns
    plain. The values read back the same."""
    m = _wide_morsel(5000)
    chunks = []
    with avro.open_avro_writer(chunks.append, codec="zstandard", block_bytes=8192, sync_marker=SYNC) as w:
        w.write_batch(m)
        assert w.rows_written == 5000
        blocks = w.blocks_written
    assert len(chunks) == 1 + blocks  # the header, then one call per block
    streamed = b"".join(chunks)
    one_shot = avro.write_avro(m, codec="zstandard", block_bytes=8192, sync_marker=SYNC)
    assert all(_field_unions(streamed).values())
    assert _field_unions(one_shot) == {"i": True, "s": False, "f": False, "ts": False, "dec": False, "a": True}
    assert _rugo_columns(streamed) == _rugo_columns(one_shot)


def test_stream_of_batches_reads_as_their_concatenation():
    import decimal

    m = _wide_morsel(9000)
    batches = [m.slice(0, 4000), m.slice(4000, 1), m.slice(4001, 0), m.slice(4001, 4999)]
    chunks = []
    rows = avro.write_avro_stream(batches, chunks.append, codec="deflate", block_bytes=16384, sync_marker=SYNC)
    assert rows == 9000
    data = b"".join(chunks)
    blocks = list(fastavro.block_reader(io.BytesIO(data)))
    assert sum(b.num_records for b in blocks) == 9000
    assert len(blocks) >= 3  # blocks never span batches; the zero-row batch adds none
    want = {c: m.column(c.encode()).to_pylist() for c in ("i", "s", "f", "dec")}
    with decimal.localcontext() as ctx:
        ctx.prec = 40
        assert _columns_of(_fastavro_rows(data), list(want)) == want
        assert _columns_of(_apache_rows(data), list(want)) == want
        got = _rugo_columns(data)
        assert {c: got[c] for c in want} == want


def test_stream_refuses_a_batch_with_another_schema_and_writes_nothing_for_it():
    chunks = []
    with avro.open_avro_writer(chunks.append, codec="null") as w:
        w.write_batch(_morsel(a=("INT64", [1, 2]), b=("VARCHAR", ["x", "y"])))
        calls = len(chunks)
        with pytest.raises(ValueError, match="column 'a' is INT32 written as \"int\" in this batch but INT64 written as \"long\""):
            w.write_batch(_morsel(a=("INT32", [3]), b=("VARCHAR", ["z"])))
        with pytest.raises(ValueError, match="column 1 is 'c' in this batch but 'b' in the first batch"):
            w.write_batch(_morsel(a=("INT64", [3]), c=("VARCHAR", ["z"])))
        with pytest.raises(ValueError, match="this batch has 1 columns, the first batch had 2"):
            w.write_batch(_morsel(a=("INT64", [3])))
        assert len(chunks) == calls  # nothing reached the sink
        w.write_batch(_morsel(a=("INT64", [3]), b=("VARCHAR", [None])))  # still usable
    _check_all_oracles(b"".join(chunks), {"a": [1, 2, 3], "b": ["x", "y", None]})


def test_stream_with_no_batch_is_refused_and_zero_rows_is_a_file():
    with pytest.raises(ValueError, match="no batch was written"):
        with avro.open_avro_writer(lambda b: None):
            pass
    with pytest.raises(ValueError, match="no batch was written"):
        avro.write_avro_stream(iter([]), lambda b: None)
    chunks = []
    assert avro.write_avro_stream([_morsel(a=("INT64", []))], chunks.append, codec="null") == 0
    assert len(chunks) == 1  # the header alone
    assert _fastavro_rows(b"".join(chunks)) == []


def test_exception_inside_the_block_leaves_a_valid_file_of_what_was_written():
    chunks = []
    with pytest.raises(KeyError):
        with avro.open_avro_writer(chunks.append, codec="deflate") as w:
            w.write_batch(_morsel(a=("INT64", [1, 2])))
            raise KeyError("caller failure")
    with pytest.raises(ValueError, match="closed"):
        w.write_batch(_morsel(a=("INT64", [3])))
    assert _fastavro_rows(b"".join(chunks)) == [{"a": 1}, {"a": 2}]


def test_a_failing_sink_closes_the_writer():
    seen = []

    def sink(b):
        seen.append(b)
        if len(seen) == 2:
            raise OSError("disk full")

    w = avro.open_avro_writer(sink, codec="null")
    with pytest.raises(OSError, match="disk full"):
        w.write_batch(_morsel(a=("INT64", [1])))
    with pytest.raises(ValueError, match="the Avro writer is closed"):
        w.write_batch(_morsel(a=("INT64", [2])))


def test_open_avro_writer_validates_eagerly():
    with pytest.raises(TypeError, match="sink must be a callable"):
        avro.open_avro_writer(b"not callable")
    with pytest.raises(ValueError, match="codec 'lz4' is not an Avro codec"):
        avro.open_avro_writer(lambda b: None, codec="lz4")
    with pytest.raises(ValueError, match="sync_marker must be exactly 16 bytes"):
        avro.open_avro_writer(lambda b: None, sync_marker=b"x")


# ---------------- 2026-10-16 rulings: required fields, fixed types, enums ----------------

def test_no_null_columns_are_required_and_null_columns_keep_the_union():
    import draken.draken_native as dn

    m = _native_morsel(
        dense=vector_from_sequence([1, 2, 3, 4], dtype="INT64"),
        nulls=vector_from_sequence([1, None, 3, 4], dtype="INT64"),
        arr=dn.vector_array_from_sequence([[1], [], [2, None], [3]]),
        arr_nulls=dn.vector_array_from_sequence([[1], None, [2], [3]]),
    )
    data = avro.write_avro(m, codec="null")
    assert _field_unions(data) == {"dense": False, "nulls": True, "arr": False, "arr_nulls": True}
    # Array ITEMS keep their union: element nulls are allowed whatever the rows hold.
    assert _field_types(data)["arr"] == {"type": "array", "items": ["null", "long"]}
    _check_all_oracles(data, {"dense": [1, 2, 3, 4], "nulls": [1, None, 3, 4],
                              "arr": [[1], [], [2, None], [3]], "arr_nulls": [[1], None, [2], [3]]})
    # A required value carries no union byte: four 1-byte varints, then the next column.
    payload = list(fastavro.block_reader(io.BytesIO(data)))[0].bytes_.getvalue()
    assert payload.startswith(b"\x02\x02\x02")  # dense=1 (no branch), nulls: branch 1 + 1


def test_a_sliced_column_without_nulls_is_required():
    """Required is decided from the rows present, not from whether a validity buffer
    exists: the null-free tail of a column with nulls is written plain."""
    m = _morsel(v=("INT64", [None, 2, 3]))
    tail = m.slice(1, 2)
    data = avro.write_avro(tail, codec="null")
    assert _field_unions(data) == {"v": False}
    _check_all_oracles(data, {"v": [2, 3]})


def test_zero_rows_writes_required_fields():
    m = _morsel(a=("INT64", []), b=("VARCHAR", []))
    data = avro.write_avro(m, codec="null")
    assert _field_unions(data) == {"a": False, "b": False}
    assert _fastavro_rows(data) == []


def test_uuid_fixed_is_defined_once_and_referenced_after():
    import uuid
    import draken.draken_native as dn

    us = [uuid.UUID(int=k * 0x0123456789ABCDEF) for k in range(4)]
    m = _native_morsel(
        a=dn.vector_uuid_from_sequence(us),
        b=dn.vector_uuid_from_sequence([us[1], None, us[2], us[3]]),
    )
    data = avro.write_avro(m, codec="null")
    assert _field_types(data) == {
        "a": {"type": "fixed", "name": "uuid", "namespace": "rugo", "size": 16, "logicalType": "uuid"},
        "b": "rugo.uuid",
    }
    want_bytes = {"a": [u.bytes for u in us], "b": [us[1].bytes, None, us[2].bytes, us[3].bytes]}
    assert _columns_of(_fastavro_rows(data), ["a", "b"]) == want_bytes
    assert _columns_of(_apache_rows(data), ["a", "b"]) == want_bytes
    got = _rugo_columns(data)
    assert got == {"a": us, "b": [us[1], None, us[2], us[3]]}


@pytest.mark.parametrize("precision,width", [(1, 1), (2, 1), (3, 2), (9, 4), (10, 5), (18, 8), (19, 9), (38, 16)])
def test_decimal_fixed_width_is_the_smallest_for_the_precision(precision, width):
    import decimal
    import draken.draken_native as dn

    D = decimal.Decimal
    with decimal.localcontext() as ctx:
        ctx.prec = 50  # 10**38 - 1 has 38 digits; the default context (28) would round it
        top = D(10) ** precision - 1
        values = [top, -top, D(0), D(1), D(-1), None]  # unary minus rounds to the context too
    build = dn.vector_decimal_from_sequence if precision <= 18 else dn.vector_decimal128_from_sequence
    m = _native_morsel(d=build(values, precision, 0), e=build(values, precision, 0))
    data = avro.write_avro(m, codec="null")
    t = _field_types(data)
    assert t["d"]["type"] == "fixed" and t["d"]["size"] == width
    assert t["e"] == f"rugo.decimal_{precision}_0"  # the same (p, s) is one named type
    with decimal.localcontext() as ctx:
        ctx.prec = 40
        _check_all_oracles(data, {"d": values, "e": values})


def test_decimal_outside_its_precision_is_refused():
    """A DECIMAL(5, 0) column holding 10**12 (outside its declared precision) does not fit
    the 3-byte fixed DECIMAL(5, 0) is written as: refused, never truncated."""
    import draken.draken_native as dn

    v = dn.vector_reinterpret_as_decimal(dn.vector_int64_from_sequence([1, 10 ** 12]), 5, 0)
    with pytest.raises(RuntimeError, match="does not fit its declared precision"):
        avro.write_avro(_native_morsel(d=v), codec="null")


def _countries(n):
    codes = ["US", "GB", "DE", "FR", "JP", "BR", "IN", "CA"]
    return [codes[(k * 7) % len(codes)] for k in range(n)]


def test_low_cardinality_strings_are_enums_and_read_back_dictionary_encoded():
    values = _countries(5000)
    with_nulls = [None if k % 9 == 0 else v for k, v in enumerate(values)]
    m = _morsel(c=("VARCHAR", values), n=("VARCHAR", with_nulls), w=("NVARCHAR", values))
    data = avro.write_avro(m, codec="null")
    symbols = sorted(set(values))
    t = _field_types(data)
    assert t["c"] == {"type": "enum", "name": "c_enum", "namespace": "rugo", "symbols": symbols}
    assert t["n"]["type"] == "enum" and t["w"]["type"] == "enum"
    assert _field_unions(data) == {"c": False, "n": True, "w": False}
    _check_all_oracles(data, {"c": values, "n": with_nulls, "w": values})
    with avro.read_avro(data) as r:
        morsel = next(iter(r))
    for name in (b"c", b"n", b"w"):
        assert morsel.column(name).is_dict, name
        assert morsel.column(name).data_length == len(symbols)
    # An enum value is its index: one byte per row here, not the 3-byte string.
    assert len(data) < len(avro.write_avro(m, codec="null", dictionary=False)) * 0.6


def test_enum_columns_through_read_avro_in_the_engine(tmp_path):
    values = _countries(3000)
    path = tmp_path / "e.avro"
    path.write_bytes(avro.write_avro(_morsel(c=("VARCHAR", values), k=("INT64", list(range(3000))))))
    got = _sql_morsel(f"SELECT c, COUNT(*) AS n FROM READ_AVRO('{path}') GROUP BY c ORDER BY c")
    assert got.column(b"c").to_pylist() == sorted(set(values))
    assert sum(got.column(b"n").to_pylist()) == 3000
    scanned = _sql_morsel(f"SELECT c FROM READ_AVRO('{path}')")
    assert scanned.column(b"c").is_dict


def test_strings_stay_strings_when_an_enum_does_not_apply():
    values = _countries(2000)
    spaced = [v if k % 100 else "New York" for k, v in enumerate(values)]  # not an Avro name
    high = [f"id_{k}" for k in range(5000)]                                # over 4,096 symbols
    cases = [
        ("spaced", "VARCHAR", spaced, {}, "string"),
        ("high", "VARCHAR", high, {}, "string"),
        ("bin", "VARBINARY", [b"US"] * 5000, {}, "bytes"),                 # only VARCHAR / NVARCHAR
        ("ok", "VARCHAR", _countries(5000), {"dictionary": False}, "string"),
    ]
    for name, dtype, vals, kw, want in cases:
        data = avro.write_avro(_morsel(**{name: (dtype, vals)}), codec="null", **kw)
        assert _field_types(data)[name] == want, name
        _check_all_oracles(data, {name: vals})
    # A stream never writes enums.
    chunks = []
    avro.write_avro_stream([_morsel(ok=("VARCHAR", _countries(5000)))], chunks.append, codec="null")
    assert _field_types(b"".join(chunks))["ok"] == "string"


# ---------------- enum analysis: slot-keyed, parallel (2026-10-16) ----------------

def _enum_column(n):
    """A column over several 64k-row ranges: short (inline) and long (> 12 byte) symbols,
    with nulls."""
    syms = ["US", "GB", "DE", "a_rather_long_symbol_name", "another_long_symbol_xyz", "Z"]
    return [None if k % 13 == 0 else syms[(k * 7) % len(syms)] for k in range(n)]


def test_enum_analysis_is_the_same_on_any_thread_count():
    values = _enum_column(300_000)
    m = _morsel(c=("VARCHAR", values))
    outs = [avro.write_avro(m, codec="null", sync_marker=SYNC, max_threads=t) for t in (1, 2, 8, 0)]
    assert all(o == outs[0] for o in outs)
    t = _field_types(outs[0])["c"]
    assert t["type"] == "enum"
    assert t["symbols"] == sorted({v for v in values if v is not None})
    assert _rugo_columns(outs[0]) == {"c": values}


def test_a_non_name_after_the_precheck_keeps_the_column_a_string():
    values = _enum_column(300_000)
    values[250_000] = "New York"  # far past the serial precheck, in a later range
    data = avro.write_avro(_morsel(c=("VARCHAR", values)), codec="null")
    assert _field_types(data)["c"] == "string"
    assert _rugo_columns(data) == {"c": values}


def test_the_symbol_cap_is_checked_across_ranges():
    """Each 64k-row range holds 3,000 distinct values (under the cap on its own); the
    column holds 15,000 (over it): a string."""
    values = [f"s{(i // 65536) * 3000 + i % 3000}" for i in range(5 * 65536)]
    data = avro.write_avro(_morsel(c=("VARCHAR", values)), codec="null")
    assert _field_types(data)["c"] == "string"


def test_an_unused_dictionary_entry_does_not_decide_the_enum():
    """A dictionary-shaped column whose dictionary holds a non-name no row uses: still an
    enum, and the unused entry is not a symbol."""
    import draken.draken_native as dn
    from draken.vectors.vector import Vector

    base = dn.vector_from_string_sequence([b"US", b"New York", b"GB", b"US"]).dictionary_encode()
    rows = [0, 2, 3] * 2000
    v = Vector(base.take(rows))
    assert v.is_dict and v.data_length == 3
    data = avro.write_avro(Morsel.from_vectors([b"c"], [v]), codec="null")
    assert _field_types(data)["c"]["symbols"] == ["GB", "US"]
    _check_all_oracles(data, {"c": ["US", "GB", "US"] * 2000})
