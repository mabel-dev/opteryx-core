# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
rugo's ORC reader and the Iceberg type attributes (docs/UUID_AND_NESTED_COLUMNS_DESIGN.md UD1-UD7):
`iceberg.binary-type=UUID` makes a BINARY column a UUID, `FIXED` (+ `iceberg.length`) a
fixed-length VARBINARY, and `iceberg.long-type=TIME` a TIME. A plain BINARY stays VARBINARY.

pyarrow.orc writes the data but cannot set type attributes, so the footer of an uncompressed
file is rewritten to add them (the same technique as opteryx-iceberg's tests/_orc_data_files.py).
PyArrow is test-only here (CLAUDE.md §4).
"""

import io
import os
import sys
import time
import uuid
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

os.environ["TZ"] = "UTC"
time.tzset()

import pyarrow as pa  # noqa: E402
import pyarrow.orc as po  # noqa: E402
from draken.draken_native import DrakenType  # noqa: E402

from rugo import orc  # noqa: E402

UUIDS = [
    uuid.UUID("550e8400-e29b-41d4-a716-446655440000"),
    uuid.UUID("00000000-0000-0000-0000-000000000001"),
    uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
]
FIXED = [b"abcd", b"\x00\x01\x02\x03", b"wxyz"]


def _varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        out.append(byte | 0x80 if value else byte)
        if not value:
            return bytes(out)


def _read_varint(buf, i):
    value, shift = 0, 0
    while True:
        c = buf[i]
        i += 1
        value |= (c & 0x7F) << shift
        shift += 7
        if not c & 0x80:
            return value, i


def _walk(buf):
    """[(field, wire, value)] for one protobuf message; value is an int or bytes."""
    out, i = [], 0
    while i < len(buf):
        key, i = _read_varint(buf, i)
        field, wire = key >> 3, key & 7
        if wire == 0:
            value, i = _read_varint(buf, i)
        elif wire == 2:
            n, i = _read_varint(buf, i)
            value = bytes(buf[i : i + n])
            i += n
        else:
            raise ValueError(f"unexpected wire type {wire}")
        out.append((field, wire, value))
    return out


def _emit(fields):
    out = bytearray()
    for field, wire, value in fields:
        out += _varint((field << 3) | wire)
        out += _varint(value) if wire == 0 else _varint(len(value)) + value
    return bytes(out)


def _with_attributes(data, attributes):
    """`data` (an uncompressed ORC file) with {column: {key: value}} type attributes added."""
    ps_len = data[-1]
    ps = _walk(data[-1 - ps_len : -1])
    footer_len = next(v for f, _, v in ps if f == 1)
    body_end = len(data) - 1 - ps_len - footer_len
    footer = _walk(data[body_end : body_end + footer_len])
    types = [f for f in footer if f[0] == 4]
    names = [v.decode() for f, _, v in _walk(types[0][2]) if f == 3]
    rewritten = [types[0]]
    for name, (_, _, raw) in zip(names, types[1:]):
        fields = _walk(raw)
        for key, value in attributes.get(name, {}).items():
            fields.append((7, 2, _emit([(1, 2, key.encode()), (2, 2, value.encode())])))
        rewritten.append((4, 2, _emit(fields)))
    out_footer, placed = [], False
    for f in footer:
        if f[0] == 4:
            if not placed:
                out_footer.extend(rewritten)
                placed = True
            continue
        out_footer.append(f)
    footer_bytes = _emit(out_footer)
    new_ps = _emit([(1, 0, len(footer_bytes)) if f == 1 else (f, w, v) for f, w, v in ps])
    return data[:body_end] + footer_bytes + new_ps + bytes([len(new_ps)])


def _file(table, attributes):
    buf = io.BytesIO()
    po.write_table(table, buf, compression="uncompressed")
    return _with_attributes(buf.getvalue(), attributes)


def _read(data, **kwargs):
    out = {}
    for morsel in orc.read_orc(data, **kwargs):
        for name in morsel.column_names:
            key = name.decode() if isinstance(name, bytes) else name
            out.setdefault(key, []).extend(morsel.column(name).to_pylist())
    return out


def _types(data):
    return {c["name"]: c for c in orc.read_metadata(data).columns}


def _table(uuid_values=None):
    return pa.table(
        {
            "u": pa.array(uuid_values or [u.bytes for u in UUIDS], pa.binary()),
            "f": pa.array(FIXED, pa.binary()),
            "plain": pa.array(FIXED, pa.binary()),
        }
    )


ATTRIBUTES = {
    "u": {"iceberg.binary-type": "UUID"},
    "f": {"iceberg.binary-type": "FIXED", "iceberg.length": "4"},
}


def test_uuid_and_fixed_attributes_type_the_columns():
    data = _file(_table(), ATTRIBUTES)
    assert _read(data) == {"u": UUIDS, "f": FIXED, "plain": FIXED}


def test_a_plain_binary_stays_varbinary_and_an_attribute_less_file_is_unchanged():
    data = _file(_table(), {})
    assert _read(data) == {"u": [u.bytes for u in UUIDS], "f": FIXED, "plain": FIXED}


def test_nulls_in_a_uuid_column_read_as_null():
    table = pa.table({"u": pa.array([UUIDS[0].bytes, None, UUIDS[2].bytes], pa.binary())})
    data = _file(table, {"u": {"iceberg.binary-type": "UUID"}})
    assert _read(data) == {"u": [UUIDS[0], None, UUIDS[2]]}


def test_a_uuid_value_that_is_not_16_bytes_names_the_column_and_length():
    data = _file(_table([UUIDS[0].bytes, b"short", UUIDS[2].bytes]), ATTRIBUTES)
    with pytest.raises(RuntimeError, match="a uuid value is 5 bytes, not 16"):
        _read(data, columns=["u"])
    assert _read(data, columns=["f"]) == {"f": FIXED}


def test_a_fixed_value_of_the_wrong_length_fails():
    table = pa.table({"f": pa.array([b"abcd", b"abc", b"wxyz"], pa.binary())})
    data = _file(table, {"f": ATTRIBUTES["f"]})
    with pytest.raises(RuntimeError, match="f"):
        _read(data)


def test_fixed_without_a_valid_length_is_refused():
    table = pa.table({"f": pa.array(FIXED, pa.binary())})
    data = _file(table, {"f": {"iceberg.binary-type": "FIXED"}})
    with pytest.raises(RuntimeError, match="iceberg.length"):
        _read(data)


def test_an_unknown_binary_type_attribute_is_refused_naming_it():
    table = pa.table({"f": pa.array(FIXED, pa.binary())})
    data = _file(table, {"f": {"iceberg.binary-type": "GEOMETRY"}})
    with pytest.raises(RuntimeError, match="GEOMETRY"):
        _read(data)


def test_a_uuid_column_is_a_uuid_type_in_the_metadata():
    columns = _types(_file(_table(), ATTRIBUTES))
    assert columns["u"]["readable"] is True
    assert columns["u"]["type"] == DrakenType.UUID.value
    assert columns["plain"]["type"] == DrakenType.VARBINARY.value
