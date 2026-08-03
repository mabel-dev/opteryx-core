"""Bitwise ops against an integer literal must run on the c-native path.

Draken's int_bitwise requires IDENTICAL physical operand types, so the c-native
gate demands equality up front. An integer literal binds at its own natural
width, so `id | 1` against `$planets.id` (INT8) was INT8 | INT64 and got refused
at plan time with "outside the c-native kernel set" — while `id | id` worked.
There is no fallback engine, so a refusal here is a hard error, not a slow path.

Arithmetic never hit this: its gate asks only that both sides be numeric.

The fix materialises the literal in the COLUMN's physical type, the same way the
comparison path already does. `_coerce_literal_physical` returns only value-exact
coercions, so a literal that will not fit the column's width is left alone and the
expression stays non-c-native — losing speed, never correctness.

`<<` and `>>` are absent below on purpose: they have no infix parser in the
Opteryx dialect, so they are unreachable from SQL.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


def column(sql, name="x"):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        out.extend(morsel.column(name).to_pylist())
    return out


# $planets.id is INT8 and starts 1, 2, 3 — a narrow column, which is exactly the
# case that exposed the gap. A literal binds INT64.
IDS = [1, 2, 3]


@pytest.mark.parametrize(
    "op,expected",
    [
        ("|", [i | 1 for i in IDS]),
        ("&", [i & 1 for i in IDS]),
        ("^", [i ^ 1 for i in IDS]),
    ],
)
def test_bitwise_column_op_literal(op, expected):
    assert column(f"SELECT id {op} 1 AS x FROM $planets LIMIT 3") == expected


@pytest.mark.parametrize("op", ["|", "&", "^"])
def test_bitwise_literal_op_column(op):
    """Coercion must fire whichever side the literal is on."""
    left = column(f"SELECT 1 {op} id AS x FROM $planets LIMIT 3")
    right = column(f"SELECT id {op} 1 AS x FROM $planets LIMIT 3")
    assert left == right


@pytest.mark.parametrize("op", ["|", "&", "^"])
def test_bitwise_column_op_column_still_works(op):
    """The case that always worked must keep working — the coercion only fires
    when exactly one side is a literal."""
    assert column(f"SELECT id {op} id AS x FROM $planets LIMIT 3") == [
        eval(f"{i} {op} {i}") for i in IDS  # noqa: S307 - fixed operators, fixed ints
    ]


def test_literal_wider_than_the_column_is_declined_not_wrapped():
    """9999 does not fit INT8. The coercion declines rather than narrowing, so the
    expression stays non-c-native and fails loud — it must NOT silently wrap to a
    value that fits."""
    with pytest.raises(Exception):
        column("SELECT id | 9999 AS x FROM $planets LIMIT 3")


def test_arithmetic_with_literal_unaffected():
    """Control: arithmetic never had this problem and must be untouched."""
    assert column("SELECT id + 1 AS x FROM $planets LIMIT 3") == [i + 1 for i in IDS]


def test_bitwise_result_values_are_correct_not_merely_admitted():
    """Guards against a fix that admits the expression to the c-native path but
    computes on a mis-coerced operand: these values are wrong if the literal were
    truncated or the operands swapped."""
    assert column("SELECT id | 4 AS x FROM $planets LIMIT 3") == [5, 6, 7]
    assert column("SELECT id & 2 AS x FROM $planets LIMIT 3") == [0, 2, 2]


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
