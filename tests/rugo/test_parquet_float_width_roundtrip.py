"""
Float width survives a rugo write -> read round trip.

Parquet has two float physical types, FLOAT (binary32) and DOUBLE (binary64),
and rugo now writes each draken float vector at its own width. It did NOT until
2026-08-21: a FLOAT32 vector was widened to a parquet float64 column on write
(`ci.type = PT_DOUBLE`), and read back as a FLOAT64 vector. That is lossless per
VALUE and wrong per COLUMN — the file DECLARES float64, so no reader can recover
the 4-byte column and rugo could not round-trip a FLOAT32 at all.

⛔ ASSERT THE PHYSICAL TYPE, NOT JUST THE VALUES. Every binary32 value is exact
in binary64, so a widened column compares equal to the original on every value
it holds. The width is only visible in the DrakenType and in the footer, which is
why both are pinned here. A declared width that is not the stored width is the
whole silent-wrong-rows class — see
`tests/sql/test_narrow_width_column_predicates.py` for what it costs downstream.

The oracle is pyarrow for the file itself (a file only rugo can read is a
defect), and rugo's own reader for the round trip.
"""

import io
import os
import random
import struct
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import draken.draken_native as dn
import rugo.parquet as rp
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector

# (label, draken constructor, arrow type name, values)
# The float32 values are all exact in binary32, so `to_pylist()` compares equal
# without a tolerance and a mismatch means a real defect, never rounding.
CASES = [
    ("float32", dn.vector_float32_from_sequence, "float", [-1.5, 0.0, 0.125, 3.5]),
    ("float64", dn.vector_float64_from_sequence, "double", [-1.5, 0.0, 0.1, 3.5]),
]


def _morsel(ctor, values, name="c"):
    return Morsel.from_vectors([name], [Vector(ctor(values))])


def _read_back(buf, name="c"):
    with rp.read_parquet(buf) as reader:
        morsels = list(reader)
    column_type = None
    values = []
    for morsel in morsels:
        column = morsel.column(name)
        column_type = column.type
        values.extend(column.to_pylist())
    return column_type, values


@pytest.mark.parametrize("compression", ["none", "zstd"])
@pytest.mark.parametrize("dictionary", [True, False])
@pytest.mark.parametrize("label,ctor,arrow_type,values", CASES)
def test_roundtrips_through_rugo(label, ctor, arrow_type, values, compression, dictionary):
    """rugo write -> rugo read returns the exact DrakenType and values."""
    source = _morsel(ctor, values)
    expected_type = source.column("c").type

    buf = rp.write_parquet(source, compression=compression, dictionary=dictionary)
    column_type, actual = _read_back(buf)

    assert column_type == expected_type, f"{label}: {column_type} != {expected_type}"
    assert actual == values


@pytest.mark.parametrize("dictionary", [True, False])
@pytest.mark.parametrize("label,ctor,arrow_type,values", CASES)
def test_pyarrow_reads_declared_width(label, ctor, arrow_type, values, dictionary):
    """The file is spec-conformant: pyarrow sees FLOAT, not DOUBLE."""
    import pyarrow.parquet as pq

    buf = rp.write_parquet(_morsel(ctor, values), compression="none", dictionary=dictionary)
    parquet_file = pq.ParquetFile(io.BytesIO(buf))

    assert str(parquet_file.schema_arrow.field(0).type) == arrow_type
    assert parquet_file.read().column(0).to_pylist() == values


@pytest.mark.parametrize("label,ctor,arrow_type,values", CASES)
def test_roundtrips_with_nulls(label, ctor, arrow_type, values):
    """Interior nulls must not disturb the declared width."""
    with_nulls = [values[0], None, values[-1]]
    source = _morsel(ctor, with_nulls)
    expected_type = source.column("c").type

    buf = rp.write_parquet(source, compression="none", dictionary=False)
    column_type, actual = _read_back(buf)

    assert column_type == expected_type, f"{label}: {column_type} != {expected_type}"
    assert actual == with_nulls


def test_float32_statistics_are_written_at_four_bytes():
    """min/max are PLAIN-encoded at the column's physical width. Eight bytes of
    stats on a 4-byte column is not a wrong number — it is unparseable, and a
    reader that trusts it prunes arbitrary row groups."""
    import pyarrow.parquet as pq

    values = [-1.5, 0.125, 3.5, 0.25]
    buf = rp.write_parquet(_morsel(dn.vector_float32_from_sequence, values),
                           compression="none", dictionary=False)
    stats = pq.ParquetFile(io.BytesIO(buf)).metadata.row_group(0).column(0).statistics
    assert (stats.min, stats.max) == (min(values), max(values))


@pytest.mark.parametrize(
    "options",
    [
        {"dictionary": True},                                  # auto-built dictionary
        {"dictionary": False},                                 # PLAIN
        {"dictionary": False, "max_rows_per_row_group": 512},  # multi row group
        {"dictionary": False, "max_page_bytes": 1024},         # multi page per chunk
        {"dictionary": True, "bloom_filters": True},           # bloom hashes the values
    ],
)
def test_float32_survives_every_write_shape(options):
    """The width has to hold on every path that slices or re-buffers the column:
    the dictionary builder, row-group splitting, page splitting and the bloom
    hash all index the value buffer themselves, so each one is a place a 4-byte
    buffer can be walked with an 8-byte stride."""
    random.seed(19)
    values = [random.choice([0.125, 0.25, 0.5, 0.75, 1.0, -0.5]) for _ in range(4096)]
    source = _morsel(dn.vector_float32_from_sequence, values)

    buf = rp.write_parquet(source, compression="none", **options)
    column_type, actual = _read_back(buf)

    assert column_type == dn.DrakenType.FLOAT32
    assert actual == values


def test_float32_preserved_dictionary_shape_stays_float32():
    """A dict-SHAPED input vector takes the writer's PRESERVE path (`codes` +
    dictionary values) rather than the auto-build one — a separate set of buffer
    reads, and so a separate chance to widen."""
    dictionary_values = [0.125, 0.25, 0.5]
    codes = [0, 1, 2, 2, 1, 0, 0, 2]
    vector = Vector(dn.vector_float32_from_dict(dictionary_values, codes))
    assert vector.type == dn.DrakenType.FLOAT32  # guards the fixture

    buf = rp.write_parquet(Morsel.from_vectors(["c"], [vector]), compression="none")
    column_type, actual = _read_back(buf)

    assert column_type == dn.DrakenType.FLOAT32
    assert actual == [dictionary_values[code] for code in codes]


def test_the_fixture_values_are_exact_in_binary32():
    """Guards the CASES table: an inexact float32 value would make every
    comparison above need a tolerance, and a tolerance would hide the widening
    this file exists to catch."""
    for label, _, _, values in CASES:
        if label != "float32":
            continue
        for value in values:
            assert struct.unpack("<f", struct.pack("<f", value))[0] == value


if __name__ == "__main__":  # pragma: no cover
    import pytest as _pytest

    raise SystemExit(_pytest.main([__file__, "-q"]))
