"""
Differential test for c-native DECIMAL binary-op promotion (S-A.3 completion).

Exercises the draken_binop kernel for every DECIMAL operand-kind combination that
goes c-native — DECIMAL×DECIMAL, DECIMAL×DECIMAL128, DECIMAL128×DECIMAL128,
DECIMAL×INT64, DECIMAL128×INT64 — for {+,-,*,/}, BOTH operand orders, with
NEGATIVE values and NULLs, and compares each engine result against an exact Python
`Decimal` oracle quantised to the binder's own resolved result scale (PostgreSQL
rules). Also asserts the tested type combinations actually route c-native (not the
closure) via _c_native_binop.

The promotion cases (int64-backed operands whose bound result precision exceeds 18
→ DECIMAL128) are the ones this stage added; the int64-tier (≤18) and the already
int128 (DECIMAL128 operand) cases are included to guard against regression.
"""

import os
import sys
from decimal import Decimal, ROUND_HALF_EVEN, getcontext

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.types.type_unification import _decimal_result
import opteryx.types.logical_type as L
from opteryx.compiled.expression import compiled_expression as ce

getcontext().prec = 200

# op name -> (SQL operator, BCBinaryOpCode used by _c_native_binop)
OPS = {
    "Plus": ("+", 1),
    "Minus": ("-", 2),
    "Multiply": ("*", 3),
    "Divide": ("/", 4),
}

# Operand specs: label -> (SQL cast type, ColumnType for result-type derivation).
# Dsmall stays int64 (DECIMAL); D is int64 but promotes when multiplied; D128 is
# int128; I is INT64 (scale-0 decimal).
SPECS = {
    "Dsmall": ("DECIMAL(8,2)", L.DECIMAL(8, 2)),
    "D": ("DECIMAL(18,2)", L.DECIMAL(18, 2)),
    "D128": ("DECIMAL(20,4)", L.DECIMAL(20, 4)),
    "I": ("INTEGER", L.INT64),
}

# (left_spec, right_spec). Both orders covered explicitly for the mixed kinds.
COMBOS = [
    ("Dsmall", "Dsmall"),  # int64-tier (regression guard)
    ("D", "D"),            # promotes to DECIMAL128 on * (and + with carry)
    ("D", "D128"),
    ("D128", "D"),
    ("D128", "D128"),
    ("D", "I"),
    ("I", "D"),
    ("D128", "I"),
    ("I", "D128"),
]

# Left decimal source (col a), right decimal source (col b, never zero for /),
# left int source (col k), right int source (col j, never zero for /). Values are
# exact at scale 2 and 4 (≤ 2 decimal places). Rows 6 and 7 carry NULLs (a-null,
# b/j-null) to exercise both-side null propagation.
ROWS = [
    # a,        b,       k,    j
    ("12.50", "3.25", "3", "2"),
    ("-4.25", "-1.50", "-7", "-3"),
    ("7.00", "8.00", "1", "5"),
    ("123.45", "-2.20", "5", "-1"),
    ("-99.99", "0.05", "-2", "9"),
    ("5.00", "-7.75", "4", "-4"),
    ("NULL", "4.00", "6", "7"),     # a (and k stays valid) — left decimal null
    ("10.00", "NULL", "2", "NULL"), # b and j null — right null
]

CAST_OF = {
    "a": "a", "b": "b", "k": "k", "j": "j",
}


def _src_for(side, spec_label):
    """Pick the source column for an operand: decimals from a/b, ints from k/j."""
    if spec_label == "I":
        return "k" if side == "left" else "j"
    return "a" if side == "left" else "b"


def _oracle(op_name, lval, rval, result_scale):
    if lval is None or rval is None:
        return None
    a = Decimal(lval)
    b = Decimal(rval)
    if op_name == "Plus":
        v = a + b
    elif op_name == "Minus":
        v = a - b
    elif op_name == "Multiply":
        v = a * b
    else:  # Divide
        v = a / b
    return v.quantize(Decimal(1).scaleb(-result_scale), rounding=ROUND_HALF_EVEN)


def _row_value(col, row):
    raw = {"a": row[0], "b": row[1], "k": row[2], "j": row[3]}[col]
    return None if raw == "NULL" else raw


def _build_values_clause():
    parts = []
    for a, b, k, j in ROWS:
        parts.append(f"({a}, {b}, {k}, {j})")
    return "(VALUES " + ", ".join(parts) + ") AS t(a, b, k, j)"


def test_decimal_binop_promotion_differential():
    session = opteryx.session()
    values = _build_values_clause()
    failures = []
    checked = 0

    for (ls, rs) in COMBOS:
        lcast, lct = SPECS[ls]
        rcast, rct = SPECS[rs]
        lsrc = _src_for("left", ls)
        rsrc = _src_for("right", rs)
        for op_name, (sql_op, op_code) in OPS.items():
            result_ct = _decimal_result(lct, rct, op_name)
            result_scale = result_ct.logical.scale
            lphys = lct.physical.name
            rphys = rct.physical.name
            resphys = result_ct.physical.name

            # Assert this combination routes c-native (not the closure).
            assert ce._c_native_binop(op_code, lphys, rphys, resphys), (
                f"expected c-native routing for {lphys} {op_name} {rphys} "
                f"-> {resphys}, got closure"
            )

            expr = f"CAST({lsrc} AS {lcast}) {sql_op} CAST({rsrc} AS {rcast})"
            sql = f"SELECT {expr} AS r FROM {values}"
            got = []
            for m in session.execute_to_morsels(sql):
                got.extend(m.column(b"r").to_pylist())

            assert len(got) == len(ROWS), f"row count {len(got)} != {len(ROWS)} for {sql}"
            for idx, row in enumerate(ROWS):
                lval = _row_value(lsrc, row)
                rval = _row_value(rsrc, row)
                expected = _oracle(op_name, lval, rval, result_scale)
                actual = got[idx]
                checked += 1
                if expected is None:
                    if actual is not None:
                        failures.append(
                            f"{lphys} {op_name} {rphys} row{idx}: expected NULL got {actual!r}"
                        )
                    continue
                if actual is None or Decimal(actual) != expected:
                    failures.append(
                        f"{lphys}({lval}) {op_name} {rphys}({rval}) -> "
                        f"{resphys} s{result_scale} row{idx}: "
                        f"expected {expected} got {actual!r}"
                    )

    assert not failures, "differential mismatches:\n" + "\n".join(failures)
    print(f"differential OK: {checked} values across {len(COMBOS)} combos × {len(OPS)} ops")


def test_repro_query_correct():
    """The original live-bug repro returns correct values (DECIMAL × INT)."""
    session = opteryx.session()
    rows = []
    for m in session.execute_to_morsels(
        "SELECT CAST(gravity AS DECIMAL(10,2)) * id AS r FROM $planets"
    ):
        rows.extend(m.column(b"r").to_pylist())
    # Mercury 3.7*1, Venus 8.9*2, Earth 9.8*3, Mars 3.7*4, Jupiter 23.1*5
    assert rows[0] == Decimal("3.70")
    assert rows[1] == Decimal("17.80")
    assert rows[2] == Decimal("29.40")
    assert rows[3] == Decimal("14.80")
    assert rows[4] == Decimal("115.50")
    print("repro OK")


if __name__ == "__main__":
    test_decimal_binop_promotion_differential()
    test_repro_query_correct()
    print("✅ okay")
