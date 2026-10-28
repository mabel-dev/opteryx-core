# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""ORC PRESENT streams from the independent Apache ORC writer, through to Draken validity."""

import datetime
import io
import json
import sys
import zlib
from decimal import Decimal
from pathlib import Path

import pytest
import pyarrow as pa
import pyarrow.orc as arrow_orc

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from rugo import orc  # noqa: E402


BOUNDARIES = [0, 1, 7, 8, 9, 63, 64, 65, 127, 128, 129, 511, 512, 513]


def _write(table, **options):
    out = io.BytesIO()
    arrow_orc.write_table(table, out, **options)
    return out.getvalue()


def _lsb_validity(values):
    bits = bytearray((len(values) + 7) // 8)
    for i, value in enumerate(values):
        if value is not None:
            bits[i // 8] |= 1 << (i % 8)
    return bytes(bits)


def _check_vector(vector, expected):
    actual = vector.to_pylist()
    assert actual == expected
    bitmap = vector.null_bitmap()
    if bitmap is None:
        assert all(value is not None for value in expected)
        return
    want = _lsb_validity(expected)
    assert len(bitmap) == len(want)
    if len(expected) % 8:
        # Padding bits are not logical rows and need not have a particular value.
        last_mask = (1 << (len(expected) % 8)) - 1
        assert bitmap[:-1] == want[:-1]
        assert bitmap[-1] & last_mask == want[-1]
    else:
        assert bitmap == want


def _check_columns(data, expected, json_columns=()):
    positions = dict.fromkeys(expected, 0)
    for morsel in orc.read_orc(data, columns=list(expected)):
        for name, values in expected.items():
            vector = morsel.column(name.encode())
            pos = positions[name]
            part = values[pos : pos + vector.length]
            if name in json_columns:
                actual = vector.to_pylist()
                assert [None if value is None else json.loads(value) for value in actual] == part
                # The JSON representation changes values, but not their validity.
                part = [None if value is None else actual[i] for i, value in enumerate(part)]
            _check_vector(vector, part)
            positions[name] += vector.length
    assert positions == {name: len(values) for name, values in expected.items()}


def _varint(data, pos):
    value = 0
    shift = 0
    while True:
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7f) << shift
        if byte < 0x80:
            return value, pos
        shift += 7


def _fields(data):
    """The protobuf wire types needed to inspect an uncompressed ORC footer."""
    pos = 0
    fields = []
    while pos < len(data):
        key, pos = _varint(data, pos)
        field, wire = key >> 3, key & 7
        if wire == 0:
            value, pos = _varint(data, pos)
        elif wire == 2:
            size, pos = _varint(data, pos)
            value = data[pos : pos + size]
            assert len(value) == size
            pos += size
        elif wire in (1, 5):
            size = 8 if wire == 1 else 4
            value = data[pos : pos + size]
            assert len(value) == size
            pos += size
        else:
            raise AssertionError(f"unexpected protobuf wire type {wire}")
        fields.append((field, value))
    return fields


def _one(fields, field, default=None):
    values = [value for number, value in fields if number == field]
    assert len(values) <= 1
    return values[0] if values else default


def _zlib_blocks(data):
    """Decode ORC's three-byte compression framing, including original blocks."""
    out = bytearray()
    original_flags = []
    pos = 0
    while pos < len(data):
        assert pos + 3 <= len(data)
        header = int.from_bytes(data[pos : pos + 3], "little")
        pos += 3
        size = header >> 1
        assert pos + size <= len(data)
        block = data[pos : pos + size]
        pos += size
        original = bool(header & 1)
        original_flags.append(original)
        out.extend(block if original else zlib.decompress(block, -15))
    return bytes(out), original_flags


def _stripe_streams(data):
    """Return raw streams and encodings of one uncompressed or zlib stripe."""
    ps_length = data[-1]
    ps = _fields(data[-1 - ps_length : -1])
    compression = _one(ps, 2, 0)
    assert compression in (0, 1)
    footer_length = _one(ps, 1)
    footer_end = len(data) - 1 - ps_length
    footer_bytes = data[footer_end - footer_length : footer_end]
    if compression == 1:
        footer_bytes = _zlib_blocks(footer_bytes)[0]
    footer = _fields(footer_bytes)
    stripes = [value for field, value in footer if field == 3]
    assert len(stripes) == 1
    stripe = _fields(stripes[0])
    offset = _one(stripe, 1)
    index_length = _one(stripe, 2, 0)
    data_length = _one(stripe, 3, 0)
    stripe_footer_length = _one(stripe, 4)
    footer_at = offset + index_length + data_length
    stripe_footer_bytes = data[footer_at : footer_at + stripe_footer_length]
    if compression == 1:
        stripe_footer_bytes = _zlib_blocks(stripe_footer_bytes)[0]
    stripe_footer = _fields(stripe_footer_bytes)
    streams = {}
    at = offset
    for field, value in stripe_footer:
        if field != 1:
            continue
        stream = _fields(value)
        kind = _one(stream, 1, 0)
        column = _one(stream, 2, 0)
        size = _one(stream, 3, 0)
        streams[column, kind] = data[at : at + size]
        at += size
    assert at == footer_at
    encodings = [_one(_fields(value), 1, 0) for field, value in stripe_footer if field == 2]
    return streams, encodings


def _decode_byte_rle(data):
    """Decode the writer's PRESENT byte-RLE and report the run forms it used."""
    out = bytearray()
    forms = set()
    pos = 0
    while pos < len(data):
        header = data[pos]
        pos += 1
        if header < 128:
            assert pos < len(data)
            out.extend([data[pos]] * (header + 3))
            pos += 1
            forms.add("repeat")
        else:
            length = 256 - header
            assert pos + length <= len(data)
            out.extend(data[pos : pos + length])
            pos += length
            forms.add("literal")
    return bytes(out), forms


def _scalars(keep):
    n = len(keep)
    specs = {
        "byte": (pa.int8(), [i % 101 - 50 for i in range(n)]),
        "long": (pa.int64(), [i * 17 - 101 for i in range(n)]),
        "double": (pa.float64(), [(i % 19 - 9) / 4 for i in range(n)]),
        "date": (pa.date32(), [datetime.date(1970, 1, 1) + datetime.timedelta(days=i - 256) for i in range(n)]),
        "bool": (pa.bool_(), [i % 3 == 0 for i in range(n)]),
        "string": (pa.string(), ["" if i % 4 == 0 else f"v{i % 11}" for i in range(n)]),
        "binary": (pa.binary(), [b"" if i % 4 == 0 else bytes([i % 256]) for i in range(n)]),
        "decimal": (pa.decimal128(12, 2), [Decimal(i - 256) / 100 for i in range(n)]),
    }
    expected = {
        name: [value if keep[i] else None for i, value in enumerate(values)]
        for name, (_, values) in specs.items()
    }
    table = pa.table({name: pa.array(expected[name], typ) for name, (typ, _) in specs.items()})
    return table, expected


@pytest.mark.parametrize("n", BOUNDARIES)
@pytest.mark.parametrize("layout", ["all-valid", "all-null", "single-valid-per-byte", "single-null-per-byte"])
def test_partial_validity_bytes_and_boundaries(n, layout):
    keep = []
    for i in range(n):
        if layout == "all-valid":
            present = True
        elif layout == "all-null":
            present = False
        elif layout == "single-valid-per-byte":
            present = i % 8 == (i // 8) % 8
        else:
            present = i % 8 != (i // 8) % 8
        keep.append(present)
    table, expected = _scalars(keep)
    data = _write(table, file_version="0.12", compression="uncompressed")
    assert orc.read_metadata(data).num_rows == n
    _check_columns(data, expected)


@pytest.mark.parametrize("version", ["0.11", "0.12"])
def test_omitted_present_and_all_null_present(version):
    valid = list(range(129))
    nulls = [None] * 129
    table = pa.table({"valid": pa.array(valid, pa.int64()), "nulls": pa.array(nulls, pa.int64())})
    data = _write(table, file_version=version, compression="uncompressed")
    streams, _ = _stripe_streams(data)
    assert (1, 0) not in streams
    packed, _ = _decode_byte_rle(streams[2, 0])
    assert packed[:16] == bytes(16)
    assert packed[16] & 0x80 == 0
    _check_columns(data, {"valid": valid, "nulls": nulls})


@pytest.mark.parametrize("version", ["0.11", "0.12"])
def test_orc_msb_present_becomes_draken_lsb_validity(version):
    first = [101] + [None] * 7
    last = [None] * 7 + [202]
    table = pa.table({"first": pa.array(first, pa.int64()), "last": pa.array(last, pa.int64())})
    data = _write(table, file_version=version, compression="uncompressed")
    streams, encodings = _stripe_streams(data)
    assert encodings[1] == (0 if version == "0.11" else 2)
    assert _decode_byte_rle(streams[1, 0])[0] == b"\x80"
    assert _decode_byte_rle(streams[2, 0])[0] == b"\x01"
    assert _lsb_validity(first) == b"\x01"
    assert _lsb_validity(last) == b"\x80"
    _check_columns(data, {"first": first, "last": last})


@pytest.mark.parametrize("version", ["0.11", "0.12"])
def test_writer_present_contains_repeated_and_literal_byte_runs(version):
    packed = bytes([0x81] * 137 + list(range(1, 129)) + [0x42] * 133)
    keep = [bool(byte & (0x80 >> bit)) for byte in packed for bit in range(8)]
    values = [i * 3 - 10 if present else None for i, present in enumerate(keep)]
    table = pa.table({"x": pa.array(values, pa.int64())})
    data = _write(table, file_version=version, compression="uncompressed", stripe_size=1024 * 1024)
    streams, encodings = _stripe_streams(data)
    assert encodings[1] == (0 if version == "0.11" else 2)
    decoded, forms = _decode_byte_rle(streams[1, 0])
    assert decoded == packed
    assert forms == {"repeat", "literal"}
    _check_columns(data, {"x": values})


@pytest.mark.parametrize("version", ["0.11", "0.12"])
def test_present_stream_is_actually_zlib_compressed(version):
    packed = bytes(range(1, 65)) * 64
    keep = [bool(byte & (0x80 >> bit)) for byte in packed for bit in range(8)]
    values = [i if present else None for i, present in enumerate(keep)]
    data = _write(
        pa.table({"x": pa.array(values, pa.int64())}),
        file_version=version,
        compression="zlib",
        compression_block_size=65536,
        stripe_size=4 * 1024 * 1024,
    )
    streams, encodings = _stripe_streams(data)
    assert encodings[1] == (0 if version == "0.11" else 2)
    present, original = _zlib_blocks(streams[1, 0])
    assert original and not all(original)
    assert _decode_byte_rle(present)[0] == packed
    _check_columns(data, {"x": values})


@pytest.mark.parametrize("version", ["0.11", "0.12"])
@pytest.mark.parametrize("codec", ["uncompressed", "zlib", "snappy", "lz4", "zstd"])
@pytest.mark.parametrize("dictionary", [0.0, 1.0], ids=["direct-strings", "dictionary-strings"])
def test_mixed_presence_across_kinds_and_compression(version, codec, dictionary):
    n = 1027
    boundaries = set(BOUNDARIES)
    keep = [i not in boundaries and i % 11 not in (2, 7) for i in range(n)]
    table, expected = _scalars(keep)
    data = _write(
        table,
        file_version=version,
        compression=codec,
        dictionary_key_size_threshold=dictionary,
        stripe_size=1024 * 1024,
        # pyarrow's default batch_size (1024) cuts a stripe per batch; keep one stripe.
        batch_size=n,
    )
    assert orc.read_metadata(data).compression == ("none" if codec == "uncompressed" else codec)
    if codec == "uncompressed":
        _, encodings = _stripe_streams(data)
        assert encodings[2] == (0 if version == "0.11" else 2)
        assert encodings[6] == (0 if version == "0.11" else 2) + int(dictionary == 1.0)
    _check_columns(data, expected)


def test_presence_is_restarted_for_each_stripe():
    n = 40000
    keep = [i % 13 not in (0, 3, 8) for i in range(n)]
    integers = [i - 123 if keep[i] else None for i in range(n)]
    strings = [f"row-{i}-" + "x" * 40 if keep[i] else None for i in range(n)]
    table = pa.table({"i": pa.array(integers, pa.int64()), "s": pa.array(strings, pa.string())})
    data = _write(table, compression="uncompressed", stripe_size=64 * 1024)
    assert orc.read_metadata(data).stripes > 1
    _check_columns(data, {"i": integers, "s": strings})


@pytest.mark.parametrize("version", ["0.11", "0.12"])
@pytest.mark.parametrize("codec", ["uncompressed", "zlib", "snappy"])
def test_nested_parent_and_child_presence_have_independent_row_domains(version, codec):
    lists = []
    structs = []
    maps = []
    map_expected = []
    for i in range(513):
        lists.append(
            None if i % 7 in (0, 3) else
            ([] if i % 13 == 0 else [None if (i * 3 + j) % 5 == 1 else i * 3 + j for j in range(3)])
        )
        structs.append(
            None if i % 9 in (1, 5) else {
                "number": None if i % 4 == 0 else i - 250,
                "items": None if i % 6 == 0 else [None, i] if i % 3 == 0 else [i, None, -i],
                "flag": None if i % 5 == 0 else i % 2 == 0,
            }
        )
        entries = None if i % 8 in (2, 6) else (
            [] if i % 11 == 0 else [("a", None if i % 3 == 0 else i), ("b", None if i % 4 == 0 else -i)]
        )
        maps.append(entries)
        map_expected.append(None if entries is None else dict(entries))
    table = pa.table({
        "lists": pa.array(lists, pa.list_(pa.int64())),
        "structs": pa.array(structs, pa.struct([
            ("number", pa.int32()), ("items", pa.list_(pa.int64())), ("flag", pa.bool_()),
        ])),
        "maps": pa.array(maps, pa.map_(pa.string(), pa.int64())),
    })
    data = _write(table, file_version=version, compression=codec, stripe_size=1024 * 1024)
    _check_columns(data, {"lists": lists, "structs": structs, "maps": map_expected}, {"structs", "maps"})
