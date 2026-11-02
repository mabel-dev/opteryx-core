"""
Untyped NULL x arithmetic/bitwise operator matrix.

Every type in reference/types.json against every `binary` and `bitwise` operator in
reference/operators.json, generated — no hand-written expectations. The rule: an
untyped NULL operand takes the other operand's type (`x + NULL` is NULL typed as
`x + x`). So for each (type, operator) the same expression over two values is the
oracle:

* answered -> `NULL op v` and `v op NULL` both run, answer NULL, and carry the
  result type of `v op v`;
* refused  -> a NULL form may not answer anything but NULL.

`NULL op NULL` has no type to adopt and is refused.

Before this rule every such expression was refused by the native engine — the
untyped pair had no kernel. Constant folding had answered the literal forms through
the deleted Python evaluator.
"""

import json
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.exceptions import IncorrectTypeError

_REFERENCE = os.path.join(os.path.dirname(__file__), "..", "..", "reference")

with open(os.path.join(_REFERENCE, "types.json")) as f:
    _TYPES = json.load(f)
with open(os.path.join(_REFERENCE, "operators.json")) as f:
    _OPERATORS = json.load(f)

_SKIPPED_TYPES = {"null", "variant"}
_ARITHMETIC = sorted(op for op, spec in _OPERATORS.items() if spec["category"] in ("binary", "bitwise"))


def _value(spec):
    example = spec["metadata"]["example"]
    # parenthesised only where `::` needs it: a parenthesised literal is not a constant
    return f"({example})" if "::" in example else example


def _cases():
    for op in _ARITHMETIC:
        symbol = _OPERATORS[op]["sql_symbol"]
        for name, spec in sorted(_TYPES.items()):
            if name not in _SKIPPED_TYPES:
                yield pytest.param(symbol, _value(spec), id=f"{name}-{op}")


def _result(sql):
    morsels = list(opteryx.session().execute_to_morsels(sql))
    column = morsels[0].column(morsels[0].column_names[0])
    return column.type, [v for m in morsels for v in m.column(m.column_names[0]).to_pylist()]


def _runs(sql):
    try:
        return True, _result(sql)
    except Exception as err:  # the oracle: refused or not
        return False, err


@pytest.mark.parametrize("symbol,value", list(_cases()))
def test_untyped_null_operand(symbol, value):
    answered, oracle = _runs(f"SELECT {value} {symbol} {value}")
    for sql in (f"SELECT NULL {symbol} {value}", f"SELECT {value} {symbol} NULL"):
        ran, result = _runs(sql)
        if answered:
            assert ran, f"{sql} refused though the value form answers: {result}"
            assert result == (oracle[0], [None]), f"{sql} -> {result}, value form type {oracle[0]}"
        elif ran:
            assert result[1] == [None], f"{sql} answered {result[1]} though the value form is refused"


@pytest.mark.parametrize("symbol", sorted({_OPERATORS[op]["sql_symbol"] for op in _ARITHMETIC}))
def test_null_with_null_is_refused(symbol):
    with pytest.raises(IncorrectTypeError):
        _result(f"SELECT NULL {symbol} NULL")


def test_null_operand_against_a_column():
    assert _result("SELECT id + NULL FROM $planets") == _result("SELECT id + CAST(NULL AS INT8) FROM $planets")
    assert _result("SELECT COUNT(*) FROM $planets WHERE id = NULL + 1")[1] == [0]
