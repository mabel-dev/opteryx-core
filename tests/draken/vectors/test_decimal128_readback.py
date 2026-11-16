"""
DECIMAL128 values read back into Python exactly (2026-11-15).

The int128 -> decimal.Decimal conversion did `Decimal(unscaled).scaleb(-scale)`, and
`scaleb` rounds to the thread's decimal context precision - 28 digits by default - so
every DECIMAL128 value past 28 significant digits read back silently rounded:
38 nines came back as Decimal('1.000000000000000000000000000E+38').
"""

import decimal

import pytest

import draken.draken_native as dn


@pytest.mark.parametrize(
    "text, precision, scale",
    [
        ("9" * 38, 38, 0),
        ("-" + "9" * 38, 38, 0),
        ("12345678901234567890123456789012345678", 38, 0),
        ("123456789012345678901234567890.12345678", 38, 8),
        ("1.50", 20, 2),  # trailing zeros are kept
        ("0.000", 20, 3),
    ],
)
def test_decimal128_reads_back_exactly(text, precision, scale):
    value = decimal.Decimal(text)
    vector = dn.vector_decimal128_from_sequence([value, None], precision, scale)
    back = vector.to_pylist()
    assert back == [value, None]
    assert str(back[0]) == text


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
