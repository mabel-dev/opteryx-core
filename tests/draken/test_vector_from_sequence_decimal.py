"""
vector_from_sequence builds a DECIMAL only from the caller's descriptor (2026-11-15).

It used to build every DECIMAL as DECIMAL(18, 6), whatever the column was declared as,
and had no DECIMAL128 at all. A DECIMAL / DECIMAL128 now needs `logical` (the column's
LogicalType, which carries precision and scale) and is refused without it.
"""

import decimal

import pytest

from draken.draken_native import DrakenType, LogicalKind, LogicalType
from draken.interop.vector_sequence import vector_from_sequence


def _decimal(precision, scale):
    return LogicalType(kind=LogicalKind.DECIMAL, precision=precision, scale=scale)


@pytest.mark.parametrize("dtype", [DrakenType.DECIMAL, DrakenType.DECIMAL128])
def test_decimal_without_descriptor_is_refused(dtype):
    with pytest.raises(ValueError, match="needs its precision and scale"):
        vector_from_sequence([decimal.Decimal("1.5")], dtype)


@pytest.mark.parametrize(
    "dtype, precision, scale, text",
    [
        (DrakenType.DECIMAL, 15, 2, "9000000000000.25"),
        (DrakenType.DECIMAL, 3, 1, "9.8"),
        (DrakenType.DECIMAL128, 38, 0, "9" * 38),
        (DrakenType.DECIMAL128, 20, 2, "999999999999999999.99"),
    ],
)
def test_decimal_is_built_at_the_descriptor(dtype, precision, scale, text):
    vector = vector_from_sequence([decimal.Decimal(text), None], dtype, logical=_decimal(precision, scale))
    assert vector.to_pylist() == [decimal.Decimal(text), None]
    assert str(vector.to_pylist()[0]) == text


def test_a_value_past_the_descriptor_is_refused():
    with pytest.raises(OverflowError):
        vector_from_sequence([decimal.Decimal("123.4")], DrakenType.DECIMAL, logical=_decimal(3, 1))
