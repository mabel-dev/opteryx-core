"""
Integer width/signedness survives a rugo write -> read round trip.

Parquet stores integers only as INT32 or INT64, so every narrower width — and
unsignedness at any width — rides the smallest physical type that holds it plus
an INTEGER(bitWidth, isSigned) annotation. These tests pin BOTH ends of that:

  * pyarrow is the read-side oracle for the file itself (the hard acceptance
    criterion — a file only rugo can read is a defect), and
  * rugo's own reader must hand back the SAME DrakenType that went in.

Asserting the DrakenType (not just the values) is the point: a width or
signedness drift is invisible in a value comparison whenever the magnitudes
happen to coincide.
"""

import io
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import draken.draken_native as dn
import rugo.parquet as rp
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector

# (label, draken constructor, boundary values for the type)
CASES = [
    ("int8", dn.vector_int8_from_sequence, [-128, 0, 127]),
    ("int16", dn.vector_int16_from_sequence, [-32768, 0, 32767]),
    ("int32", dn.vector_int32_from_sequence, [-2147483648, 0, 2147483647]),
    ("int64", dn.vector_from_sequence, [-(2**63), 0, 2**63 - 1]),
    ("uint8", dn.vector_uint8_from_sequence, [0, 200, 255]),
    ("uint16", dn.vector_uint16_from_sequence, [0, 40000, 65535]),
    ("uint32", dn.vector_uint32_from_sequence, [0, 3000000000, 4294967295]),
    ("uint64", dn.vector_uint64_from_sequence, [0, 2**63, 2**64 - 1]),
]

# Arrow type string each case must surface as through the oracle.
ARROW_TYPE = {c[0]: c[0] for c in CASES}


def _morsel(ctor, values, name="c"):
    return Morsel.from_vectors([name], [Vector(ctor(values))])


@pytest.mark.parametrize("compression", ["none", "zstd"])
@pytest.mark.parametrize("dictionary", [True, False])
@pytest.mark.parametrize("label,ctor,values", CASES)
def test_roundtrips_through_rugo(label, ctor, values, compression, dictionary):
    """rugo write -> rugo read returns the exact DrakenType and values."""
    src = _morsel(ctor, values)
    expected_type = src.column("c").type

    buf = rp.write_parquet(src, compression=compression, dictionary=dictionary)
    with rp.read_parquet(buf) as reader:
        out = list(reader)[0]

    col = out.column("c")
    assert col.type == expected_type, f"{label}: {col.type} != {expected_type}"
    assert col.to_pylist() == values


@pytest.mark.parametrize("dictionary", [True, False])
@pytest.mark.parametrize("label,ctor,values", CASES)
def test_pyarrow_reads_declared_width(label, ctor, values, dictionary):
    """The file is spec-conformant: pyarrow sees the declared width, not INT64."""
    import pyarrow.parquet as pq

    buf = rp.write_parquet(_morsel(ctor, values), compression="none", dictionary=dictionary)
    pf = pq.ParquetFile(io.BytesIO(buf))

    assert str(pf.schema_arrow.field(0).type) == ARROW_TYPE[label]
    assert pf.read().column(0).to_pylist() == values


@pytest.mark.parametrize("dictionary", [True, False])
def test_unsigned_stats_use_unsigned_ordering(dictionary):
    """Values astride the signed midpoint sit in NEGATIVE physical slots, so a
    signed min/max would invert the range and let a reader prune a row group
    that holds matching rows."""
    import pyarrow.parquet as pq

    for label, ctor, values in [
        ("uint16", dn.vector_uint16_from_sequence, [1, 40000, 7, 65535, 2]),
        ("uint32", dn.vector_uint32_from_sequence, [1, 3000000000, 7, 4294967295, 2]),
        ("uint64", dn.vector_uint64_from_sequence, [1, 2**63, 7, 2**64 - 1, 2]),
    ]:
        buf = rp.write_parquet(
            _morsel(ctor, values), compression="none", dictionary=dictionary
        )
        stats = pq.ParquetFile(io.BytesIO(buf)).metadata.row_group(0).column(0).statistics
        assert (stats.min, stats.max) == (min(values), max(values)), label


@pytest.mark.parametrize("label,ctor,values", CASES)
def test_roundtrips_with_nulls(label, ctor, values):
    """Interior nulls must not disturb the declared width."""
    vals = [values[0], None, values[-1]]
    src = _morsel(ctor, vals)
    expected_type = src.column("c").type

    buf = rp.write_parquet(src, compression="none", dictionary=False)
    with rp.read_parquet(buf) as reader:
        out = list(reader)[0]

    col = out.column("c")
    assert col.type == expected_type, f"{label}: {col.type} != {expected_type}"
    assert col.to_pylist() == vals


if __name__ == "__main__":  # pragma: no cover
    import pytest as _pytest

    raise SystemExit(_pytest.main([__file__, "-q"]))
