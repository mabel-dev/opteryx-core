# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The text renderers read a column's LOGICAL type from one per-column descriptor.

Both renderers -- draken's render_json_scalar / render_json_column
(interop/value_format.hpp, behind Vector._to_json) and rugo's writer-selection
switch (rugo/src/_text_render.hpp, behind write_csv / write_jsonl) -- used to
take the logical-type fields as a fan of parallel `const int*` arrays: unit,
scale, child unit, child scale, dimension. They now take one ColumnDesc per
column carrying those parameters AND the logical KIND.

Two things are asserted here:

1. Every descriptor-carrying type still renders exactly as it did when the
   parameters travelled as loose arrays -- TIMESTAMP/TIME unit, DECIMAL and
   DECIMAL128 scale, VECTOR_FP16 dimension, and an ARRAY element's own unit.
   Each of those parameters is the difference between a right answer and a
   plausible wrong one (a dropped unit turns microseconds into seconds), so a
   value that survives the round trip proves the descriptor arrived.

2. The kind, which is new. An IPv4 column is physically DRAKEN_UINT32 carrying
   LogicalKind::IPV4 -- the physical tag is IDENTICAL to a plain unsigned
   column's, so the kind is the only thing that can select dotted-decimal
   rendering. A UINT32 with no descriptor must keep rendering as a number.
"""

import csv as _csv
import datetime
import io
import json
import os
import sys
from decimal import Decimal

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import draken.draken_native as dn
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector
from rugo.csv import write_csv
from rugo.jsonl import write_jsonl

NAME = "c"


def _morsel(nb):
    return Morsel.from_vectors([NAME], [Vector(nb)])


def _jsonl_values(nb):
    return [json.loads(line)[NAME] for line in write_jsonl(_morsel(nb)).decode().splitlines()]


def _csv_cells(nb):
    """CSV body cells, nulls as None (a null renders as an empty field).

    Read back with the stdlib csv reader, so RFC-4180 quoting of the fields
    that embed a delimiter (arrays, fp16 vectors) is the reader's problem.
    """
    body = write_csv(_morsel(nb)).decode()
    rows = list(_csv.reader(io.StringIO(body)))[1:]  # drop the header row
    # A null cell in a single-column file is a line with nothing on it, which
    # the reader hands back as an empty row rather than a one-empty-field row.
    return [None if not row or row[0] == "" else row[0] for row in rows]


def _json_values(nb):
    return json.loads(Vector(nb)._to_json())


# ---------------------------------------------------------------------------
# Parameters that used to travel as parallel arrays
# ---------------------------------------------------------------------------

# One instant, stored at four resolutions. Dropping the unit renders every one
# of them as if it were seconds, so agreement across all four is the proof the
# per-column unit arrived.
INSTANT = datetime.datetime(2024, 5, 6, 7, 8, 9, 123456)


@pytest.mark.parametrize(
    "unit,expected",
    [
        ("s", "2024-05-06T07:08:09+00:00"),
        ("ms", "2024-05-06T07:08:09.123000+00:00"),
        ("us", "2024-05-06T07:08:09.123456+00:00"),
        ("ns", "2024-05-06T07:08:09.123456+00:00"),
    ],
)
def test_timestamp_unit_reaches_every_renderer(unit, expected):
    nb = dn.vector_timestamp_from_sequence([INSTANT, None], unit=unit)
    assert _json_values(nb) == [expected, None]
    assert _jsonl_values(nb) == [expected, None]
    assert _csv_cells(nb) == [expected, None]


@pytest.mark.parametrize(
    "unit,expected",
    [("us", "01:02:03.456789"), ("ns", "01:02:03.456789")],
)
def test_time_unit_reaches_every_renderer(unit, expected):
    nb = dn.vector_time64_from_sequence([datetime.time(1, 2, 3, 456789), None], unit=unit)
    assert _json_values(nb) == [expected, None]
    assert _jsonl_values(nb) == [expected, None]
    assert _csv_cells(nb) == [expected, None]


@pytest.mark.parametrize("ctor", [dn.vector_decimal_from_sequence, dn.vector_decimal128_from_sequence])
@pytest.mark.parametrize("scale,text", [(0, "12"), (2, "12.34"), (5, "12.34500")])
def test_decimal_scale_reaches_every_renderer(ctor, scale, text):
    """The scale decides where the point goes -- losing it moves it."""
    value = Decimal(text)
    nb = ctor([value, None], 18, scale)
    assert _json_values(nb) == [float(text), None]
    assert _jsonl_values(nb) == [float(text), None]
    assert _csv_cells(nb) == [text, None]


@pytest.mark.parametrize("dimension", [1, 3, 8])
def test_vector_fp16_dimension_reaches_the_writers(dimension):
    """The dimension is the row stride -- a wrong one reads the wrong values."""
    row = [float(k) for k in range(dimension)]
    nb = dn.vector_fp16_from_sequence([row, None], dimension)
    assert _jsonl_values(nb) == [row, None]
    assert _csv_cells(nb) == [json.dumps(row).replace(" ", ""), None]


def test_array_element_unit_reaches_the_writers():
    """The ARRAY child's own descriptor -- the second half of the collapse.

    The child's unit used to arrive as its own `cunits` array; it now rides in
    the same descriptor as the parent's.
    """
    nb = dn.vector_array_from_sequence([[1700000000000, 1700000001000], None])
    dn.vector_retag_array_child_as_timestamp64(nb, "ms")
    expected = ["2023-11-14T22:13:20+00:00", "2023-11-14T22:13:21+00:00"]

    assert _json_values(nb) == [expected, None]
    assert _jsonl_values(nb) == [expected, None]
    assert _csv_cells(nb) == [json.dumps(expected).replace(" ", ""), None]


# ---------------------------------------------------------------------------
# The kind: IPv4 vs plain unsigned
# ---------------------------------------------------------------------------

# 0 and 2**32-1 are the ends of the range; 3232235777 is 192.168.1.1, the value
# that catches an octet order reversed somewhere between here and ipv4.h.
IPV4_VALUES = [0, 3232235777, None, 4294967295]
IPV4_TEXT = ["0.0.0.0", "192.168.1.1", None, "255.255.255.255"]


def _ipv4_vector(values=IPV4_VALUES):
    return dn.vector_retag_uint32_as_ipv4(dn.vector_uint32_from_sequence(values))


def test_ipv4_descriptor_is_visible_to_consumers():
    """logical_type_kind is how a consumer tells an address from an integer."""
    assert _ipv4_vector().logical_type_kind is dn.LogicalKind.IPV4
    assert dn.vector_uint32_from_sequence(IPV4_VALUES).logical_type_kind is None


def test_ipv4_renders_dotted_decimal_in_json():
    """Quoted -- an address is text, not a JSON number."""
    nb = _ipv4_vector()
    assert _json_values(nb) == IPV4_TEXT
    assert _jsonl_values(nb) == IPV4_TEXT
    assert b'"192.168.1.1"' in Vector(nb)._to_json()
    assert '"192.168.1.1"' in write_jsonl(_morsel(nb)).decode()


def test_ipv4_renders_dotted_decimal_in_csv():
    assert _csv_cells(_ipv4_vector()) == IPV4_TEXT


def test_ipv4_csv_field_is_quoted_when_the_delimiter_is_a_dot():
    """'.' is a legal delimiter, and an address is full of them.

    Written raw, 192.168.1.1 would split one column into four. ec_ipv4 goes
    through csv_field, so the field is quoted per RFC 4180 and the reader gets
    the address back whole.
    """
    body = write_csv(_morsel(_ipv4_vector()), delimiter=".").decode()
    assert '"192.168.1.1"' in body

    rows = list(_csv.reader(io.StringIO(body), delimiter="."))[1:]
    assert [None if not row or row[0] == "" else row[0] for row in rows] == IPV4_TEXT


def test_ipv4_renders_from_a_dictionary_shaped_column():
    """The emitter must read data[selection[i]], not assume a dense column."""
    values = [0, 3232235777, None, 4294967295, 3232235777]
    nb = _ipv4_vector(values).dictionary_encode()
    assert nb.is_dict  # the shape under test actually materialized
    expected = ["0.0.0.0", "192.168.1.1", None, "255.255.255.255", "192.168.1.1"]

    assert _json_values(nb) == expected
    assert _jsonl_values(nb) == expected
    assert _csv_cells(nb) == expected


def test_plain_unsigned_without_a_descriptor_stays_numeric():
    """The other side of the kind check: no descriptor, no dotted decimal.

    Asserted on every width, because the IPv4 branch sits in the switch case
    all four unsigned widths share.
    """
    for ctor, values in (
        (dn.vector_uint8_from_sequence, [0, 255]),
        (dn.vector_uint16_from_sequence, [0, 65535]),
        (dn.vector_uint32_from_sequence, IPV4_VALUES),
        (dn.vector_uint64_from_sequence, [0, 2**64 - 1]),
    ):
        nb = ctor(values)
        assert _json_values(nb) == values
        assert _jsonl_values(nb) == values
        assert _csv_cells(nb) == [None if v is None else str(v) for v in values]


if __name__ == "__main__":  # pragma: no cover
    import pytest as _pytest

    _pytest.main([__file__, "-q"])
