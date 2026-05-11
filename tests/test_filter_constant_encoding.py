"""Verify that equality filters emit constant-encoded columns.

`WHERE col = <literal>` makes `col` provably constant in the surviving rows.
The filter operator replaces such columns with constant-encoded vectors so
downstream operators (joins, projections, appends) move less data.
"""

import os, sys
sys.path.insert(0, os.path.join(sys.path[0], ".."))

import opteryx
from draken.encoding import DRAKEN_ENCODING_CONSTANT


def _morsels(sql):
    session = opteryx.session()
    return [m for m in session.execute_to_morsels(sql) if m is not None and m.num_rows > 0]


def _column(morsel, name):
    key = name.encode() if isinstance(name, str) else name
    return morsel.column(key, key)


def test_int_equality_emits_constant():
    rows = _morsels("SELECT id, name FROM $planets WHERE id = 3")
    assert rows, "expected at least one row"
    for m in rows:
        col = _column(m, "id")
        assert col.encoding == DRAKEN_ENCODING_CONSTANT, (
            f"id should be constant-encoded, got encoding={col.encoding}"
        )
        # name was not in the predicate; should not be forced constant
        name_col = _column(m, "name")
        assert name_col.encoding != DRAKEN_ENCODING_CONSTANT


def test_string_equality_emits_constant():
    rows = _morsels("SELECT name, id FROM $planets WHERE name = 'Earth'")
    assert rows
    for m in rows:
        col = _column(m, "name")
        assert col.encoding == DRAKEN_ENCODING_CONSTANT


def test_conjunction_marks_both_constant():
    rows = _morsels("SELECT id, name FROM $planets WHERE id = 3 AND name = 'Earth'")
    assert rows
    for m in rows:
        assert _column(m, "id").encoding == DRAKEN_ENCODING_CONSTANT
        assert _column(m, "name").encoding == DRAKEN_ENCODING_CONSTANT


def test_inequality_does_not_emit_constant():
    rows = _morsels("SELECT id FROM $planets WHERE id > 3")
    assert rows
    for m in rows:
        col = _column(m, "id")
        # Multiple surviving values; must not be constant-encoded
        assert col.encoding != DRAKEN_ENCODING_CONSTANT


def test_expression_lhs_does_not_emit_constant():
    # The predicate is on lower(name), not name — name itself isn't constant
    rows = _morsels("SELECT name FROM $planets WHERE LOWER(name) = 'earth'")
    assert rows
    for m in rows:
        col = _column(m, "name")
        assert col.encoding != DRAKEN_ENCODING_CONSTANT


def test_filter_correctness_unchanged():
    # Sanity: result rows are correct
    rows = _morsels("SELECT id, name FROM $planets WHERE id = 3")
    total = sum(m.num_rows for m in rows)
    assert total == 1


if __name__ == "__main__":
    test_int_equality_emits_constant()
    test_string_equality_emits_constant()
    test_conjunction_marks_both_constant()
    test_inequality_does_not_emit_constant()
    test_expression_lhs_does_not_emit_constant()
    test_filter_correctness_unchanged()
    print("OK")
