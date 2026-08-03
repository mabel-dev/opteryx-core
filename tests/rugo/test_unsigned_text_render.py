# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Unsigned integer columns render their value on every text-export path.

The two shared renderers -- draken's render_json_scalar (interop/value_format.hpp,
behind Vector._to_json and ARRAY elements) and rugo's writer-selection switch
(rugo/src/_text_render.hpp, behind write_csv / write_jsonl) -- covered the signed,
float, bool, temporal, decimal and string families but had no case for
UINT8/16/32/64. All four fell through to `null` in JSON/JSONL and to an empty
CSV field, silently, for any real value.

The specific trap pinned here is UINT64 above INT64_MAX: rendering those through
the existing int64 path would print 2**63 as -9223372036854775808. They go
through fmt_uint64 instead.

Unsigned columns reach these writers from the Parquet scan path (Parquet
unsigned annotations map to real unsigned DrakenTypes), so each case is asserted
both on a directly-constructed vector and after a Parquet round trip -- with
dictionary encoding on and off, because a dictionary-shaped column takes a
different branch in resolve_col than a dense one.
"""

import json
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import draken.draken_native as dn
import rugo.parquet as rp
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector
from rugo.csv import write_csv
from rugo.jsonl import write_jsonl

# (label, constructor, values incl. the type's maximum and a NULL)
CASES = [
    ("uint8", dn.vector_uint8_from_sequence, [0, 200, None, 255]),
    ("uint16", dn.vector_uint16_from_sequence, [0, 40000, None, 65535]),
    ("uint32", dn.vector_uint32_from_sequence, [0, 3000000000, None, 4294967295]),
    # 2**63 and 2**64-1 are the values an int64 rendering path gets wrong.
    ("uint64", dn.vector_uint64_from_sequence, [0, 2**63, None, 2**64 - 1]),
]


def _morsel(ctor, values, name="c"):
    return Morsel.from_vectors([name], [Vector(ctor(values))])


def _jsonl_values(morsel, name="c"):
    return [json.loads(line)[name] for line in write_jsonl(morsel).decode().splitlines()]


def _csv_values(morsel):
    body = write_csv(morsel).decode().splitlines()[1:]  # drop header
    return [None if cell == "" else int(cell) for cell in body]


@pytest.mark.parametrize("label,ctor,values", CASES)
def test_vector_to_json_renders_unsigned(label, ctor, values):
    """draken's render_json_scalar -- values, not nulls."""
    assert json.loads(Vector(ctor(values))._to_json()) == values


@pytest.mark.parametrize("label,ctor,values", CASES)
def test_jsonl_writer_renders_unsigned(label, ctor, values):
    assert _jsonl_values(_morsel(ctor, values)) == values


@pytest.mark.parametrize("label,ctor,values", CASES)
def test_csv_writer_renders_unsigned(label, ctor, values):
    assert _csv_values(_morsel(ctor, values)) == values


@pytest.mark.parametrize("dictionary", [True, False])
@pytest.mark.parametrize("label,ctor,values", CASES)
def test_renders_unsigned_from_parquet_scan(label, ctor, values, dictionary):
    """The path unsigned columns actually arrive by: a Parquet read.

    Repeating the values gives the dictionary=True case a column whose physical
    value count is below its row count, which is the shape that takes the
    non-dense branch of resolve_col.
    """
    rows = values * 4
    buf = rp.write_parquet(_morsel(ctor, rows), compression="none", dictionary=dictionary)
    with rp.read_parquet(buf) as reader:
        out = list(reader)[0]

    assert out.column("c").to_pylist() == rows  # the read itself is sound
    assert _jsonl_values(out) == rows
    assert _csv_values(out) == rows


def test_uint64_above_int64_max_is_not_signed():
    """The exact failure an int64 rendering path produces: a negative number.

    Asserted on the rendered TEXT (not the parsed value) so a '-' can never
    slip through whatever the JSON/CSV reader would coerce it back to.
    """
    values = [2**63, 2**63 + 1, 2**64 - 1]
    m = _morsel(dn.vector_uint64_from_sequence, values)

    rendered = [Vector(dn.vector_uint64_from_sequence(values))._to_json().decode()]
    rendered.append(write_jsonl(m).decode())
    rendered.append(write_csv(m).decode())

    for text in rendered:
        assert "-" not in text, text
        for v in values:
            assert str(v) in text, text


if __name__ == "__main__":  # pragma: no cover
    import pytest as _pytest

    _pytest.main([__file__, "-q"])
