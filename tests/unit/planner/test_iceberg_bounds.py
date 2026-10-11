# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
`native_manifest.iceberg_bounds` (src/cpp/planner/iceberg_bounds.hpp): an Iceberg
manifest entry's bounds maps - JSON text as rugo's Avro reader renders them, binary
values base64 - into the opteryx manifest's field_ids / ordinal min / max / null
counts. Expected keys come from `DrakenType.ordinalize` over the value the bytes
encode (Iceberg spec, Appendix D). opteryx-iceberg's tests compare the same function
against pyiceberg's `from_bytes` on real manifests.
"""

import base64
import json
import os
import struct
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from draken import vector_from_sequence  # noqa: E402
from draken.draken_native import DrakenType  # noqa: E402
from draken.morsels.morsel import Morsel  # noqa: E402

from opteryx.compiled.planner.native_manifest import iceberg_bounds  # noqa: E402


def _map(entries):
    if entries is None:
        return None
    return json.dumps(
        [{"key": k, "value": base64.b64encode(v).decode() if type(v) is bytes else v} for k, v in entries]
    )


def _run(rows, types):
    """rows: [(lower entries, upper entries, null entries)] - each a list of
    (key, value) or None."""
    names = [b"lower", b"upper", b"nulls"]
    vectors = [
        vector_from_sequence([_map(r[i]) for r in rows], DrakenType.NVARCHAR) for i in range(3)
    ]
    morsel = Morsel.from_vectors(names, vectors)
    return iceberg_bounds(
        len(rows), morsel.column(b"lower"), morsel.column(b"upper"), morsel.column(b"nulls"), types
    )


def test_each_type_decodes_to_its_ordinal_key():
    cases = [
        (1, DrakenType.BOOL, b"\x01", True),
        (2, DrakenType.INT32, struct.pack("<i", -5), -5),
        (3, DrakenType.DATE32, struct.pack("<i", -25567), -25567),
        (4, DrakenType.INT64, struct.pack("<q", -(2**62)), -(2**62)),
        (5, DrakenType.TIMESTAMP64, struct.pack("<q", 1_700_000_000_000_000), 1_700_000_000_000_000),
        (6, DrakenType.FLOAT32, struct.pack("<f", -1.5), -1.5),
        (7, DrakenType.FLOAT64, struct.pack("<d", 2.5e-300), 2.5e-300),
        (8, DrakenType.VARCHAR, "héllo wörld".encode(), "héllo wörld"),
        (9, DrakenType.VARBINARY, b"\xff\x00\x10", b"\xff\x00\x10"),
    ]
    types = {fid: t for fid, t, _, _ in cases}
    lower = [(fid, raw) for fid, _, raw, _ in cases]
    ((field_ids, mins, maxs, nulls),) = _run([(lower, lower, [(2, 3)])], types)
    assert field_ids == [fid for fid, _, _, _ in cases]
    for (fid, t, _, value), got in zip(cases, mins):
        if t in (DrakenType.DATE32, DrakenType.TIMESTAMP64):
            assert got == value, fid  # identity: ColumnType.ordinalize passes raw ints
        else:
            assert got == t.ordinalize(value), fid
    assert maxs == mins
    assert nulls == [None, 3] + [None] * 7


@pytest.mark.parametrize(
    "raw, unscaled",
    [(b"", 0), (b"\x01", 1), (b"\xff", -1), (b"\x00\xff", 255), (b"\xff\x00", -256),
     ((10**18 - 1).to_bytes(8, "big", signed=True), 10**18 - 1),
     ((-(10**18) + 1).to_bytes(8, "big", signed=True), -(10**18) + 1)],
)
def test_decimal_is_its_unscaled_big_endian_value(raw, unscaled):
    ((_, mins, _, _),) = _run([([(1, raw)], None, None)], {1: DrakenType.DECIMAL})
    assert mins == [unscaled]


def test_field_ids_are_the_sorted_union_and_untyped_fields_have_no_bound():
    rows = [
        ([(5, struct.pack("<q", 1)), (2, struct.pack("<q", 7))], [(3, struct.pack("<q", 9))], [(5, 0), (4, 1)]),
        (None, None, None),
        ([(1, struct.pack("<q", 1)), (1, struct.pack("<q", 2))], [], None),  # last entry wins
    ]
    types = {1: DrakenType.INT64, 2: DrakenType.INT64, 5: DrakenType.INT64}
    got = _run(rows, types)
    assert got[0] == ([2, 3, 5], [7, None, 1], [None, None, None], [None, None, 0])
    assert got[1] == ([], [], [], [])
    assert got[2] == ([1], [2], [None], [None])


@pytest.mark.parametrize(
    "lower, types, message",
    [
        ([(1, b"\x00\x00")], {1: DrakenType.INT32}, "is 2 bytes; its type takes 4"),
        ([(1, b"\x00" * 9)], {1: DrakenType.DECIMAL}, "does not fit in int64"),
        ([(1, b"\x00")], {1: DrakenType.DECIMAL128}, "DECIMAL128"),
        ([(1, "not base64!")], {1: DrakenType.INT64}, "not valid base64"),
        ([(1, 5)], {1: DrakenType.INT64}, "not base64 text"),
    ],
)
def test_malformed_bounds_are_refused(lower, types, message):
    with pytest.raises(ValueError, match=message):
        _run([(lower, None, None)], types)


@pytest.mark.parametrize(
    "raw",
    [b"\xff", b"\xc0\xaf", b"\xed\xa0\x80", b"ab\xe2\x82", b"\xf4\x90\x80\x80"],
    ids=["invalid-byte", "overlong", "surrogate", "truncated", "past-10FFFF"],
)
def test_a_string_bound_that_is_not_utf8_is_refused(raw):
    # What pyiceberg's from_bytes did: bytes.decode("utf-8") raised.
    with pytest.raises(UnicodeDecodeError):
        raw.decode("utf-8")
    with pytest.raises(ValueError, match="not valid UTF-8"):
        _run([([(1, raw)], None, None)], {1: DrakenType.VARCHAR})
    # The same bytes are a fine binary bound.
    ((_, mins, _, _),) = _run([([(1, raw)], None, None)], {1: DrakenType.VARBINARY})
    assert mins == [DrakenType.VARBINARY.ordinalize(raw)]


def test_a_utf8_string_bound_with_every_sequence_length_decodes():
    text = "a\u00e9\u20ac\U0001F600\U0010FFFF"
    ((_, mins, _, _),) = _run([([(1, text.encode())], None, None)], {1: DrakenType.VARCHAR})
    assert mins == [DrakenType.VARCHAR.ordinalize(text)]


def test_text_that_is_not_a_json_array_is_refused():
    vectors = [vector_from_sequence(["{", None], DrakenType.NVARCHAR)] + [
        vector_from_sequence([None, None], DrakenType.NVARCHAR) for _ in range(2)
    ]
    morsel = Morsel.from_vectors([b"l", b"u", b"n"], vectors)
    with pytest.raises(ValueError, match="not valid JSON"):
        iceberg_bounds(2, morsel.column(b"l"), morsel.column(b"u"), morsel.column(b"n"), {})


def test_absent_columns_mean_no_bounds():
    assert iceberg_bounds(2, None, None, None, {}) == [([], [], [], []), ([], [], [], [])]
