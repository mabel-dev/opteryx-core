"""DECIMAL column statistics decode to exact decimals, not unscaled integers.

A DECIMAL(P,S) column stores the UNSCALED integer - INT32/INT64 little-endian,
BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY big-endian two's complement. `decode_value`
used to hand the INT32/INT64 form back as that raw integer, so a column holding
1.10..7.70 reported bounds of 110..770 and row-group pruning discarded groups
that genuinely match: `d = 3.30` tested 3.30 < 110 and returned no rows, as did
every `<`, `<=`, IN and BETWEEN (`>` survived only because 770 exceeds the
literal). Surfaced through an Iceberg catalog, whose connector pushes DECIMAL
predicates into the scan.
"""

import io
import struct
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from rugo import parquet
from rugo.rugo_native import decode_value

VALUES = [Decimal(v) for v in ("1.10", "2.20", "3.30", "4.40", "5.50", "6.60", "7.70")]


@pytest.mark.parametrize(
    "physical, logical, raw, expected",
    [
        (b"int32", b"decimal(9,2)", struct.pack("<i", 110), Decimal("1.10")),
        (b"int32", b"decimal(9,2)", struct.pack("<i", -770), Decimal("-7.70")),
        (b"int64", b"decimal(18,2)", struct.pack("<q", 330), Decimal("3.30")),
        (b"int64", b"decimal(18,0)", struct.pack("<q", 42), Decimal("42")),
        (
            b"fixed_len_byte_array",
            b"decimal(38,2)",
            (330).to_bytes(16, "big", signed=True),
            Decimal("3.30"),
        ),
        (
            b"byte_array",
            b"decimal(20,3)",
            (-1234).to_bytes(2, "big", signed=True),
            Decimal("-1.234"),
        ),
    ],
)
def test_decimal_stat_decodes_at_scale(physical, logical, raw, expected):
    value = decode_value(physical, logical, raw, False)
    assert isinstance(value, Decimal)
    assert value == expected


def test_plain_integers_are_unchanged():
    """Only the decimal annotation rescales; an unannotated INT32 is still an int."""
    assert decode_value(b"int32", b"int32", struct.pack("<i", 110), False) == 110
    assert decode_value(b"int32", b"", struct.pack("<i", 110), False) == 110


def test_unparseable_decimal_annotation_raises():
    """Returning the raw integer instead would reintroduce the wrong bounds."""
    with pytest.raises(ValueError):
        decode_value(b"int32", b"decimal(9)", struct.pack("<i", 110), False)


def _decimal_file(precision: int, as_integer: bool) -> bytes:
    table = pa.table({"d": pa.array(VALUES, pa.decimal128(precision, 2))})
    sink = io.BytesIO()
    pq.write_table(table, sink, store_decimal_as_integer=as_integer)
    return sink.getvalue()


@pytest.mark.parametrize(
    "precision, as_integer",
    [(9, True), (18, True), (9, False), (38, False)],
    ids=["int32", "int64", "flba-9", "flba-38"],
)
@pytest.mark.parametrize(
    "op, value, keep",
    [
        ("=", Decimal("3.30"), 1),
        ("<", Decimal("8.0"), 1),
        ("<=", Decimal("3.30"), 1),
        (">", Decimal("4.0"), 1),
        ("=", Decimal("9.99"), 0),
        ("<", Decimal("1.10"), 0),
        (">", Decimal("7.70"), 0),
    ],
)
def test_row_group_pruning_uses_scaled_bounds(precision, as_integer, op, value, keep):
    """The single row group spans [1.10, 7.70]: kept exactly when the predicate
    can match inside that range, pruned exactly when it cannot."""
    data = _decimal_file(precision, as_integer)
    assert parquet._row_group_mask(data, None, [("d", op, value)]) == [keep]


@pytest.mark.parametrize(
    "precision, as_integer",
    [(9, True), (18, True), (9, False), (38, False)],
    ids=["int32", "int64", "flba-9", "flba-38"],
)
@pytest.mark.parametrize(
    "op, value, keep",
    [
        # Float literals ON the bounds. 7.7 as a float is 7.7000000000000001776,
        # above Decimal('7.70'), so an exact float-vs-Decimal compare pruned the
        # row group that holds the value.
        ("=", 7.7, 1),
        (">=", 7.7, 1),
        ("=", 1.1, 1),
        ("<=", 1.1, 1),
        ("in", [7.7, 99.0], 1),
        ("=", 7, 1),  # int literal inside the range
        ("=", 7.71, 0),
        (">", 7.7, 0),
        ("in", [0.5, 99.0], 0),
    ],
)
def test_float_literal_prunes_as_the_decimal_it_spells(precision, as_integer, op, value, keep):
    data = _decimal_file(precision, as_integer)
    assert parquet._row_group_mask(data, None, [("d", op, value)]) == [keep]
