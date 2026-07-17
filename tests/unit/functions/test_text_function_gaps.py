"""
Native-kernel coverage for three previously-REFUSED text functions, plus the
CONCAT/CONCAT_WS family-wide non-string-operand fix:

  * TO_CHAR(codepoint)   — draken_to_char (function_string_extra.cpp)
  * CONCAT_WS(sep, x)    — 2-arg form, rewritten to `x || ''` (predicate_rewriter)
  * RANDOM_STRING(n)     — draken_random_string (function_string_extra.cpp)
  * CONCAT/CONCAT_WS     — every arity now auto-casts non-string operands to
                           VARCHAR before building the StringConcat chain
                           (_stringify_for_concat, predicate_rewriter.py)

All three functions raised ``NotSupportedError`` at PLAN time before this change:
the native engine has no per-morsel Python fallback for projections, so a column
expression lacking a c-native kernel is refused, not run slowly. A literal-only
smoke test would have passed via constant folding while the column form stayed
refused — so every assertion below exercises a COLUMN, and correctness is checked
against an INDEPENDENT Python oracle rather than the (unreachable) Python
implementation.

Contracts verified here:

  TO_CHAR — inverse of TO_ASCII. Integer codepoint → its UTF-8 encoding (one
  character), VARCHAR. Codepoint 0 → empty string (mirrors TO_ASCII's empty→0);
  a value that is not a Unicode scalar (negative, > U+10FFFF, or a surrogate) is a
  LOUD error, never a replacement character. NULL row → NULL.

  CONCAT_WS(sep, x) — with one value the separator never appears, so the result is
  x rendered as a string.

  CONCAT/CONCAT_WS non-string operands — StringConcat (`||`) is string-only
  natively, so every CONCAT/CONCAT_WS arity used to refuse a non-string operand
  (CONCAT(id, name), CONCAT_WS('-', id, name), ...) even though `CAST(id AS
  VARCHAR)` already worked. Each operand is now auto-cast to VARCHAR unless it is
  already string-family or NULL-typed (NULL short-circuits via the existing
  StringConcat NULL-operand rule, no cast needed).

  RANDOM_STRING(n) — n random BYTES per row as VARBINARY (architect ruling
  2026-07-17). Volatile: one draw per row. n = 0 → empty; NULL operand → NULL;
  negative n → loud error.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def _col(sql, name="x"):
    rows = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        if morsel is not None:
            rows.extend(morsel.column(name).to_pylist())
    return rows


# --------------------------------------------------------------------------- #
# TO_CHAR
# --------------------------------------------------------------------------- #
def test_to_char_ascii_column():
    # id + 64 → 'A'.. via the COLUMN path (id is a column ⇒ not constant-folded).
    ids = _col("SELECT id AS x FROM $planets ORDER BY id")
    out = _col("SELECT TO_CHAR(id + 64) AS x FROM $planets ORDER BY id")
    assert out == [chr(i + 64) for i in ids], out


def test_to_char_multibyte_utf8_column():
    # id + 9728 lands in the ☀-family (U+2600.., 3-byte UTF-8) — exercises the
    # multi-byte encode path over a column.
    ids = _col("SELECT id AS x FROM $planets ORDER BY id")
    out = _col("SELECT TO_CHAR(id + 9728) AS x FROM $planets ORDER BY id")
    assert out == [chr(i + 9728) for i in ids], out


def test_to_char_zero_is_empty_string():
    # Codepoint 0 → '' (the documented inverse of TO_ASCII('') → 0), not a NUL byte.
    out = _col("SELECT TO_CHAR(id - id) AS x FROM $planets")
    assert out == [""] * len(out), out


def test_to_char_null_row():
    out = _col(
        "SELECT TO_CHAR(CASE WHEN id = 3 THEN NULL ELSE id + 64 END) AS x "
        "FROM $planets ORDER BY id"
    )
    assert out[2] is None, out          # id == 3 → NULL
    assert out[0] == "A", out           # id == 1 → 'A'


def test_to_char_roundtrip_with_to_ascii():
    # TO_ASCII(TO_CHAR(cp)) == cp for every in-range codepoint (column path).
    rows = _col(
        "SELECT id + 64 = TO_ASCII(TO_CHAR(id + 64)) AS x FROM $planets ORDER BY id"
    )
    assert all(rows), rows


def test_to_char_out_of_range_fails_loud():
    # > U+10FFFF must raise, never silently degrade to a replacement character.
    for expr in ("id + 2000000000", "0 - id", "id + 55295"):  # too-big, negative, +55295→surrogate at id=1
        try:
            _col(f"SELECT TO_CHAR({expr}) AS x FROM $planets")
            raised = False
        except Exception:
            raised = True
        assert raised, f"TO_CHAR({expr}) should have raised"


# --------------------------------------------------------------------------- #
# CONCAT_WS — 2-arg form
# --------------------------------------------------------------------------- #
def test_concat_ws_two_arg_equals_value():
    # One value ⇒ the separator never appears ⇒ result is just the value.
    out = _col("SELECT CONCAT_WS('-', name) AS x FROM $planets ORDER BY id")
    names = _col("SELECT name AS x FROM $planets ORDER BY id")
    assert out == names, out


def test_concat_ws_multi_arg_still_works():
    # The >2 rewrite must be unaffected by the new ==2 arm.
    out = _col("SELECT CONCAT_WS('-', name, name) AS x FROM $planets ORDER BY id")
    names = _col("SELECT name AS x FROM $planets ORDER BY id")
    assert out == [f"{n}-{n}" for n in names], out


def test_concat_ws_two_arg_non_string_is_stringified():
    # Non-string operands are now auto-cast to VARCHAR (family-wide fix), so the
    # 2-arg form stringifies an int column exactly like CAST(id AS VARCHAR) would.
    out = _col("SELECT CONCAT_WS('-', id) AS x FROM $planets ORDER BY id")
    ids = _col("SELECT id AS x FROM $planets ORDER BY id")
    assert out == [str(i) for i in ids], out


# --------------------------------------------------------------------------- #
# CONCAT / CONCAT_WS — family-wide non-string operand coercion
# --------------------------------------------------------------------------- #
def test_concat_int_and_string_columns():
    out = _col("SELECT CONCAT(id, name) AS x FROM $planets ORDER BY id")
    ids = _col("SELECT id AS x FROM $planets ORDER BY id")
    names = _col("SELECT name AS x FROM $planets ORDER BY id")
    assert out == [f"{i}{n}" for i, n in zip(ids, names)], out


def test_concat_string_and_int_columns_order_matters():
    out = _col("SELECT CONCAT(name, id) AS x FROM $planets ORDER BY id")
    ids = _col("SELECT id AS x FROM $planets ORDER BY id")
    names = _col("SELECT name AS x FROM $planets ORDER BY id")
    assert out == [f"{n}{i}" for i, n in zip(ids, names)], out


def test_concat_ws_three_arg_non_string_first_operand():
    out = _col("SELECT CONCAT_WS('-', id, name) AS x FROM $planets ORDER BY id")
    ids = _col("SELECT id AS x FROM $planets ORDER BY id")
    names = _col("SELECT name AS x FROM $planets ORDER BY id")
    assert out == [f"{i}-{n}" for i, n in zip(ids, names)], out


def test_concat_float_column():
    out = _col("SELECT CONCAT(name, 3.14) AS x FROM $planets ORDER BY id LIMIT 1")
    assert out == ["Mercury3.14"], out


def test_concat_bool_column():
    out = _col("SELECT CONCAT(name, id = 1) AS x FROM $planets ORDER BY id")
    assert out[0] == "Mercurytrue", out
    assert out[1] == "Venusfalse", out


def test_concat_null_row_still_short_circuits():
    # A NULL-valued (but typed) operand must still propagate NULL rather than
    # being stringified to the literal text "None"/"null".
    out = _col(
        "SELECT CONCAT(CASE WHEN id = 3 THEN NULL ELSE name END, '!') AS x "
        "FROM $planets ORDER BY id"
    )
    assert out[2] is None, out
    assert out[0] == "Mercury!", out


def test_concat_untyped_null_literal_still_short_circuits():
    out = _col("SELECT CONCAT(name, NULL) AS x FROM $planets")
    assert out == [None] * len(out), out


# --------------------------------------------------------------------------- #
# RANDOM_STRING
# --------------------------------------------------------------------------- #
def test_random_string_fixed_width_bytes():
    # OCTET_LENGTH counts bytes for every string family — every row is exactly n.
    out = _col("SELECT OCTET_LENGTH(RANDOM_STRING(24)) AS x FROM $planets")
    assert out == [24] * len(out), out


def test_random_string_per_row_width_from_column():
    # Width taken from a per-row column value (inline slots, ≤ 12 bytes).
    ids = _col("SELECT id AS x FROM $planets ORDER BY id")
    out = _col("SELECT OCTET_LENGTH(RANDOM_STRING(id)) AS x FROM $planets ORDER BY id")
    assert out == ids, out


def test_random_string_wide_arena_path():
    # Width 40 > STR_INLINE_MAX (12) ⇒ exercises the arena (extern-slot) path.
    out = _col("SELECT OCTET_LENGTH(RANDOM_STRING(40)) AS x FROM $planets")
    assert out == [40] * len(out), out


def test_random_string_zero_is_empty():
    out = _col("SELECT OCTET_LENGTH(RANDOM_STRING(0)) AS x FROM $planets")
    assert out == [0] * len(out), out


def test_random_string_returns_varbinary_and_is_volatile():
    vals = _col("SELECT RANDOM_STRING(8) AS x FROM $planets")
    assert all(isinstance(v, (bytes, bytearray)) for v in vals), [type(v) for v in vals]
    assert all(len(v) == 8 for v in vals), vals
    # Volatile ⇒ one independent draw per row: not all rows identical.
    assert len(set(bytes(v) for v in vals)) > 1, vals


def test_random_string_null_row():
    out = _col(
        "SELECT OCTET_LENGTH(RANDOM_STRING(CASE WHEN id = 3 THEN NULL ELSE id END)) AS x "
        "FROM $planets ORDER BY id"
    )
    assert out[2] is None, out          # id == 3 → NULL
    assert out[0] == 1, out             # id == 1 → 1 byte


def test_random_string_negative_fails_loud():
    try:
        _col("SELECT RANDOM_STRING(0 - id) AS x FROM $planets")
        raised = False
    except Exception:
        raised = True
    assert raised, "negative RANDOM_STRING length should raise"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
