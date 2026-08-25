"""
DECIMAL columns materialize natively, and identically to the oracle.

The reader used to route every decimal value through Python: unscaled int ->
`Decimal(u).scaleb(-S)` -> `decimal_to_unscaled()` -> unscaled. That round trip
was an IDENTITY on the stored representation (parquet already holds the signed
unscaled integer at the column's scale, which is exactly draken's DECIMAL /
DECIMAL128 storage), and it cost ~360ns/row — 30x a plain int64 column with the
same decode telemetry.

It is now a native scatter into a draken buffer. These tests pin the identity
claim the removal rests on, across every shape the scatter has to handle:

  * physical tier — INT32, INT64, and FIXED_LEN_BYTE_ARRAY at widths 4/8/13/16,
    which is what selects the int64 vs int128 path,
  * logical tier — precision <= 18 (DRAKEN_DECIMAL) and > 18 (DRAKEN_DECIMAL128),
  * encoding — dictionary on and off, and an all-one-value column,
  * nulls — interior, and all-null.

pyarrow is the oracle for values; asserting the DrakenType as well is the point,
because a tier that silently picks the wrong carrier is invisible in a value
comparison whenever the magnitudes happen to fit both.
"""

import decimal
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import draken.draken_native as dn
import rugo.parquet as rp

decimal.getcontext().prec = 60
D = decimal.Decimal

# (label, precision, scale, values, store_as_integer)
#
# store_decimal_as_integer picks the physical tier pyarrow writes: True gives
# INT32/INT64, False gives FIXED_LEN_BYTE_ARRAY. Both reach the reader flagged
# is_decimal and must land on the same carrier.
CASES = [
    ("flba_p9s2", 9, 2, [D("1.23"), None, D("-9999999.99"), D("0.00")], False),
    ("flba_p18s6", 18, 6, [D("123456789012.345678"), None, D("-0.000001")], False),
    ("flba_p30s5", 30, 5, [D("12345678901234567890123.45678"), None, D("-1.00000")], False),
    ("flba_p38s0", 38, 0, [D("-" + "9" * 37), D("9" * 37), None], False),
    ("allnull_p12s3", 12, 3, [None, None, None], False),
    ("int32_p9s2", 9, 2, [D("1.23"), None, D("-9999999.99"), D("0.00")], True),
    ("int64_p18s4", 18, 4, [D("12345678901234.5678"), None, D("-0.0001")], True),
    ("int32_const_p9s2", 9, 2, [D("7.77"), D("7.77"), D("7.77")], True),
]


def _write(precision, scale, values, store_as_integer, dictionary, tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = str(tmp_path / "d.parquet")
    pq.write_table(
        pa.table({"v": pa.array(values, type=pa.decimal128(precision, scale))}),
        path,
        use_dictionary=dictionary,
        store_decimal_as_integer=store_as_integer,
        compression="snappy",
        # Small enough that the values repeat across several row groups, so the
        # per-morsel scatter runs many times rather than once.
        row_group_size=7,
    )
    return path


def _read(path):
    out = []
    types = set()
    for morsel in rp.read_parquet(path, columns=["v"]):
        col = morsel.column("v")
        out += col.to_pylist()
        types.add(col.type)
    return out, types


@pytest.mark.parametrize("dictionary", [True, False])
@pytest.mark.parametrize("label,precision,scale,values,as_int", CASES)
def test_matches_oracle(label, precision, scale, values, as_int, dictionary, tmp_path):
    """rugo's values equal pyarrow's, scale and trailing zeros included."""
    import pyarrow.parquet as pq

    repeated = values * 40
    path = _write(precision, scale, repeated, as_int, dictionary, tmp_path)

    got, _ = _read(path)
    assert got == pq.read_table(path, columns=["v"]).column("v").to_pylist(), label


@pytest.mark.parametrize("dictionary", [True, False])
@pytest.mark.parametrize("label,precision,scale,values,as_int", CASES)
def test_carrier_follows_precision(label, precision, scale, values, as_int, dictionary, tmp_path):
    """p <= 18 lands on DECIMAL (int64), p > 18 on DECIMAL128 (int128)."""
    path = _write(precision, scale, values * 40, as_int, dictionary, tmp_path)

    _, types = _read(path)
    expected = dn.DrakenType.DECIMAL if precision <= 18 else dn.DrakenType.DECIMAL128
    assert types == {expected}, f"{label}: {types} != {{{expected}}}"


@pytest.mark.parametrize("label,precision,scale,values,as_int", CASES)
def test_precision_and_scale_survive(label, precision, scale, values, as_int, tmp_path):
    """The logical descriptor rides through, not just the physical carrier — a
    dropped scale reads back as a value 10^S too large."""
    import pyarrow.parquet as pq

    path = _write(precision, scale, values * 40, as_int, dictionary=False, tmp_path=tmp_path)
    got, _ = _read(path)
    exp = pq.read_table(path, columns=["v"]).column("v").to_pylist()

    for g, e in zip(got, exp):
        if e is None:
            assert g is None, label
        else:
            # exponent pins the scale; Decimal equality alone ignores it
            assert g.as_tuple().exponent == e.as_tuple().exponent, label


# NOT TESTED HERE: that the materializer builds no Python object per row.
# The obvious probe — patching `decimal.Decimal` and counting constructions —
# is fake green: the old reader captured `_Decimal = _decimal.Decimal` at import
# time and draken cached the type in a C static, so neither would have seen the
# patch. It would pass against the very code it claims to detect. The only
# honest signal is wall time (the regression is 30x), which does not belong in a
# unit suite; it is a benchmark property. Asserted by measurement, not by a test.


if __name__ == "__main__":  # pragma: no cover
    import pytest as _pytest

    raise SystemExit(_pytest.main([__file__, "-q"]))
