"""
Regression tests for NULL string operands in the `||` (StringConcat) operator.

Bug fixed here: a NULL literal used as a string-concat operand produced garbage
instead of NULL. `SELECT name || CAST(NULL AS VARCHAR)` returned strings like
``'Mercury<draken.draken_native.Vector object at 0x...>'`` rather than NULL.

Two independent defects combined to cause it:

  1. `CAST(NULL AS VARCHAR)` collapsed to an *untyped* NULL literal — the
     compile-time literal-cast folder discarded the target type. A typed NULL
     of VARCHAR is required so the operand materialises as a real null-validity
     VARCHAR constant rather than an untyped DRAKEN_NULL vector.

  2. Even with a type tag, the C++ lowerer read `type.physical` via
     ``PyLong_AsLong`` — but ``DrakenType`` is a plain (non-int) Enum, so that
     silently failed and the physical tag was lost (always -1).

  3. The arithmetic concat closure stringified a DRAKEN_NULL operand's Python
     repr (``str(vector)``) instead of treating it as null — so even a bare
     ``name || NULL`` emitted garbage.

A DRAKEN_NULL vector has ``data == NULL`` and ``validity == NULL`` (⇒ all rows
*valid*), so a string kernel handed one reads a garbage arena and marks every
row non-null. The fix makes ``x || NULL`` short-circuit to NULL, matching
DuckDB. Verified against DuckDB at fix time.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def _col(sql, name="x"):
    rows = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        rows.extend(morsel.column(name).to_pylist())
    return rows


def test_concat_typed_null_literal_right():
    # name || CAST(NULL AS VARCHAR) → NULL for every row (NULL || x = NULL).
    out = _col("SELECT name || CAST(NULL AS VARCHAR) AS x FROM $planets")
    assert out == [None] * len(out), out


def test_concat_typed_null_literal_left():
    # CAST(NULL AS VARCHAR) || name → NULL for every row.
    out = _col("SELECT CAST(NULL AS VARCHAR) || name AS x FROM $planets")
    assert out == [None] * len(out), out


def test_select_typed_null_literal():
    # CAST(NULL AS VARCHAR) on its own must be NULL, not garbage.
    out = _col("SELECT CAST(NULL AS VARCHAR) AS x FROM $planets")
    assert out == [None] * len(out), out


def test_concat_bare_null_literal():
    # An untyped NULL literal must also short-circuit to NULL (closure path).
    out = _col("SELECT name || NULL AS x FROM $planets")
    assert out == [None] * len(out), out


def test_concat_real_column_null_unaffected():
    # Real-column nulls already worked; ensure they still do (mix of values+NULL).
    out = _col(
        "SELECT CASE WHEN name = 'Earth' THEN NULL ELSE name END || '!' AS x "
        "FROM $planets ORDER BY id"
    )
    assert out[2] is None, out  # Earth row → NULL
    assert out[0] == "Mercury!", out
    assert out[1] == "Venus!", out


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
