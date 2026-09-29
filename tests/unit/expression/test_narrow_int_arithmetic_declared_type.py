"""The DECLARED result type of narrow-integer arithmetic equals what the kernel EMITS.

Draken's D.6 rule (architect decision, fixed_int_ops.h): `+ - * % DIV` widen to the NEXT
power — INT8->INT16, INT16->INT32, INT32->INT64, INT64 stays; UINT likewise. The binder
used to declare "the wider operand" instead, so INT32 * INT32 was declared INT32 while the
kernel emitted INT64. Two consequences: the schema lied about column types, and a
constant-folded product re-materialised its INT64 value under an INT32 tag —
`CAST(2147483647 AS INT32) * CAST(2147483647 AS INT32)` died with
"int32: value out of range".
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.types import logical_type as lt
from opteryx.types.type_unification import compute_result_logical_type
from opteryx.types.logical_type import LogicalCategory


def _run(sql):
    morsels = list(opteryx.session().execute_to_morsels(sql))
    types = [str(t) for t in morsels[0].column_types]
    rows = []
    for m in morsels:
        rows.extend(m.to_arrow().to_pylist())
    return types, rows


@pytest.mark.parametrize(
    "left,right,op,expected",
    [
        (lt.INT8, lt.INT8, "Multiply", lt.INT16),
        (lt.INT16, lt.INT16, "Plus", lt.INT32),
        (lt.INT32, lt.INT32, "Minus", lt.INT64),
        (lt.INT64, lt.INT64, "Plus", lt.INT64),
        (lt.INT32, lt.INT64, "Multiply", lt.INT64),
        (lt.INT8, lt.INT32, "Plus", lt.INT64),
        (lt.INT32, lt.INT32, "Modulo", lt.INT64),
        (lt.INT32, lt.INT32, "MyIntegerDivide", lt.INT64),
        (lt.UINT8, lt.UINT8, "Multiply", lt.UINT16),
        (lt.UINT32, lt.UINT32, "Plus", lt.UINT64),
        (lt.UINT64, lt.UINT64, "Plus", lt.UINT64),
        # signed x narrow unsigned: the kernel widens from the signed rank covering the
        # unsigned side (UINT8 -> as INT16), so UINT8 - INT8 is INT32, not INT16.
        (lt.UINT8, lt.INT8, "Minus", lt.INT32),
        (lt.UINT32, lt.INT64, "Plus", lt.INT64),
    ],
)
def test_declared_width_is_the_d6_next_power(left, right, op, expected):
    got = compute_result_logical_type(left, right, op, LogicalCategory.INTEGER)
    assert got.physical == expected.physical


def test_folded_literal_products_no_longer_die_and_are_typed_as_emitted():
    types, rows = _run("SELECT CAST(2147483647 AS INT32) * CAST(2147483647 AS INT32) AS r")
    assert rows == [{"r": 2147483647 * 2147483647}] and types == ["DrakenType.INT64"]
    types, rows = _run("SELECT CAST(2147483647 AS INT32) + CAST(1 AS INT32) AS r")
    assert rows == [{"r": 2147483648}] and types == ["DrakenType.INT64"]
    types, rows = _run("SELECT CAST(100 AS INT8) * CAST(100 AS INT8) AS r")
    assert rows == [{"r": 10000}] and types == ["DrakenType.INT16"]
    types, rows = _run("SELECT CAST(200 AS UINT8) * CAST(200 AS UINT8) AS r")
    assert rows == [{"r": 40000}] and types == ["DrakenType.UINT16"]


def test_column_and_folded_paths_agree_on_type_and_value():
    col = (
        "SELECT a * b AS r FROM (SELECT CAST(2147483647 AS INT32) AS a, "
        "CAST(2147483647 AS INT32) AS b UNION ALL SELECT CAST(1 AS INT32) AS a, "
        "CAST(1 AS INT32) AS b) AS t"
    )
    col_types, col_rows = _run(col)
    lit_types, lit_rows = _run("SELECT CAST(2147483647 AS INT32) * CAST(2147483647 AS INT32) AS r")
    assert col_types == lit_types
    assert {r["r"] for r in col_rows} >= {lit_rows[0]["r"]}
