# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Every numeric type compared against every other, over every operator.

WHAT THIS PINS. `draken_compare_dv` is identical-type only; anything else routes
to `draken_numeric_cmp` via the `_NUM_PHYS` gate in `compiled_expression.pyx`.
That gate listed the signed ints, the floats and the decimals — and no unsigned
width. So EVERY cross-type numeric comparison involving an unsigned column
declined to nullptr and, with no Python fallback on the native expression path,
raised `err_op=11`. The whole unsigned band of this matrix was a hard runtime
error, `WHERE u32col = i64col` included.

Unsigned against a LITERAL always worked — `_coerce_literal_physical`
materialises a literal in the column's own physical type, so the pair is
identical by the time it reaches the kernel. That is what confined this to
column-to-column comparisons and kept it hidden.

⛔ THE TRAP, AND WHY THIS TEST IS A MATRIX AND NOT FOUR CASES. Opening the gate
without teaching `fk_read_dec` / `fk_read_num_double` the unsigned widths is
STRICTLY WORSE than the error it replaces: their `default` arm reads eight bytes,
so a UINT8 column (one byte per row) is read as int64 and answers from whatever
follows it in memory. Measured, not theorised — gate-open/readers-absent scored
326 of these 600 comparisons WRONG, silently. The gate and the readers are one
change, and only a full matrix against an exact oracle catches the half-done
version; a handful of `u32 = i64` cases passes it happily.

THE ORACLE is Python's own arithmetic, not another query. Exact `Decimal` when
no float is involved, `float` when one is — which is what the kernel claims to
do (exact int128 for the all-integer/decimal case, double promotion only when a
float operand forces it). Values are chosen adversarially: every unsigned column
carries rows above its signed midpoint, every signed column carries negatives,
and the DECIMAL column carries a non-zero scale so the rescale path runs.
"""

import itertools
import os
import sys
import tempfile
from decimal import Decimal

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import draken.draken_native as dn
import opteryx
import rugo.parquet as rp
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector

# Row-aligned columns. Row 3 of every unsigned column sits at its signed
# midpoint boundary and rows 4-5 above it: read as signed those are negative, so
# any pair involving them inverts if a width is misread. The signed columns carry
# their own extremes so the reverse mistake shows up too.
VALUES = {
    "u8": [0, 1, 127, 128, 200, 255, 7, 128],
    "u16": [0, 1, 32767, 32768, 40000, 65535, 7, 32768],
    "u32": [0, 1, 2147483647, 2147483648, 3232236680, 4294967295, 7, 2147483648],
    "u64": [0, 1, (1 << 63) - 1, 1 << 63, (1 << 64) - 1, 200, 7, 128],
    "i8": [0, 1, -128, 127, -1, 100, 7, -5],
    "i16": [0, 1, -32768, 32767, -1, 200, 7, -5],
    "i32": [0, 1, -2147483648, 2147483647, -1, 255, 7, -5],
    "i64": [0, 1, -(1 << 63), (1 << 63) - 1, -1, 200, 7, -5],
    "f64": [0.0, 1.0, 127.0, 128.5, 200.0, 255.0, 7.0, -5.0],
    "dec": [
        Decimal("0.00"), Decimal("1.00"), Decimal("127.50"), Decimal("128.00"),
        Decimal("200.00"), Decimal("255.00"), Decimal("7.00"), Decimal("-5.00"),
    ],
}
COLUMNS = list(VALUES)
FLOAT_COLUMNS = {"f64"}

OPS = {
    "=": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
}


def _vectors():
    return {
        "u8": Vector(dn.vector_uint8_from_sequence(VALUES["u8"])),
        "u16": Vector(dn.vector_uint16_from_sequence(VALUES["u16"])),
        "u32": Vector(dn.vector_uint32_from_sequence(VALUES["u32"])),
        "u64": Vector(dn.vector_uint64_from_sequence(VALUES["u64"])),
        "i8": Vector(dn.vector_int8_from_sequence(VALUES["i8"])),
        "i16": Vector(dn.vector_int16_from_sequence(VALUES["i16"])),
        "i32": Vector(dn.vector_int32_from_sequence(VALUES["i32"])),
        "i64": Vector(dn.vector_from_sequence(VALUES["i64"])),
        "f64": Vector(dn.vector_float64_from_sequence(VALUES["f64"])),
        "dec": Vector(dn.vector_decimal_from_sequence(VALUES["dec"], 18, 2)),
    }


@pytest.fixture(scope="module")
def dataset():
    vectors = _vectors()
    buffer = rp.write_parquet(
        Morsel.from_vectors(list(vectors), list(vectors.values())), compression="none"
    )
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = os.path.join(tmp, "numerics")
        os.makedirs(data_dir)
        with open(os.path.join(data_dir, "data.parquet"), "wb") as handle:
            handle.write(buffer)
        yield data_dir


def _expected(left, right, op):
    """SQL numeric promotion: exact when no float is involved, double when one is."""
    compare = OPS[op]
    pairs = zip(VALUES[left], VALUES[right])
    if left in FLOAT_COLUMNS or right in FLOAT_COLUMNS:
        return sum(1 for a, b in pairs if compare(float(a), float(b)))
    return sum(1 for a, b in pairs if compare(Decimal(a), Decimal(b)))


def _actual(dataset, left, right, op):
    session = opteryx.session()
    sql = f"SELECT COUNT(*) AS c FROM '{dataset}' WHERE {left} {op} {right}"
    for morsel in session.execute_to_morsels(sql):
        return morsel.column(b"c").to_pylist()[0]
    return None


@pytest.mark.parametrize("op", list(OPS))
@pytest.mark.parametrize("right", COLUMNS)
@pytest.mark.parametrize("left", COLUMNS)
def test_cross_type_comparison_matches_exact_arithmetic(dataset, left, right, op):
    assert _actual(dataset, left, right, op) == _expected(left, right, op)


def test_the_unsigned_band_is_actually_exercised():
    """Guards the matrix itself: if the fixture ever stopped carrying values above
    the signed midpoints, every case above would still pass while testing nothing
    that distinguishes a signed read from an unsigned one."""
    assert max(VALUES["u8"]) > 127
    assert max(VALUES["u16"]) > 32767
    assert max(VALUES["u32"]) > (1 << 31) - 1
    assert max(VALUES["u64"]) > (1 << 63) - 1
    assert min(VALUES["i8"]) < 0 and min(VALUES["i64"]) < 0
    assert any(v.as_tuple().exponent != 0 for v in VALUES["dec"])


def test_unsigned_against_a_literal_still_works(dataset):
    """The path that always worked — the literal is materialised in the column's
    own physical type, so it never reaches the mixed-numeric router. Pinned so a
    change to the router cannot quietly take the literal path with it."""
    assert _actual(dataset, "u32", "3232236680", "=") == VALUES["u32"].count(3232236680)
    assert _actual(dataset, "u64", str((1 << 64) - 1), "=") == 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
