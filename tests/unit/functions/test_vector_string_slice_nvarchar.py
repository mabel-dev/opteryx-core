"""
SUBSTRING / LEFT / RIGHT with column-valued position/count/length — NVARCHAR
codepoint-awareness, end to end through SQL.

Column-valued SUBSTRING/LEFT/RIGHT is served by the C-ABI kernels
draken_substring_dynamic / draken_left_dynamic / draken_right_dynamic
(draken/ops/kernels/function_kernels.cpp). For NVARCHAR they slice by codepoint,
never by byte offset (which would cut multi-byte codepoints in half); VARCHAR
stays byte-level.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


# Deliberately non-ASCII: 2-byte (é, ö) and 4-byte (😀) UTF-8 codepoints, so a
# byte-sliced implementation lands mid-codepoint for most cut points.
_ROWS = ["héllo wörld", "😀😀abc", "plain ascii"]


def _py_substring(s, start, count=None):
    """Independent oracle mirroring fk_substr_range's SQL SUBSTRING semantics:
    1-based `start` (negative counts from the end), `count` optional (runs to end
    when absent), clamped to [0, len(s)]. Operates on codepoints — correct for
    NVARCHAR directly; VARCHAR callers pass in a byte sequence instead."""
    n = len(s)
    s0 = start - 1 if start > 0 else start
    s0 = max(0, s0 + n) if s0 < 0 else min(s0, n)
    if count is None:
        e = n
    else:
        e = count + (start - 1 if start > 0 else start)
        e = max(0, e + n) if e < 0 else min(e, n)
    e = max(e, s0)
    return s[s0:e]


def _rows(sql, *cols):
    out = {c: [] for c in cols}
    for morsel in opteryx.session().execute_to_morsels(sql):
        if morsel is not None:
            for c in cols:
                out[c].extend(morsel.column(c).to_pylist())
    return tuple(out[c] for c in cols) if len(cols) > 1 else out[cols[0]]


def test_substring_sql_nvarchar_column_start_and_count():
    # NVARCHAR literal cast to a runtime value, sliced with COLUMN start/count
    # (id) — non-ASCII (2-byte + 4-byte codepoints) so a byte-sliced kernel
    # would land mid-codepoint. Exercises draken_substring_dynamic via SQL.
    text = "héllo wörld😀"
    ids, out = _rows(
        f"SELECT id, SUBSTRING(CAST('{text}' AS NVARCHAR), id, id) AS x "
        "FROM $planets ORDER BY id",
        "id", "x",
    )
    expected = [_py_substring(text, i, i) for i in ids]
    assert out == expected, out


def test_left_right_sql_nvarchar_column_length():
    text = "héllo wörld😀"
    ids, left = _rows(
        f"SELECT id, LEFT(CAST('{text}' AS NVARCHAR), id) AS x FROM $planets ORDER BY id",
        "id", "x",
    )
    assert left == [text[:i] for i in ids], left

    ids, right = _rows(
        f"SELECT id, RIGHT(CAST('{text}' AS NVARCHAR), id) AS x FROM $planets ORDER BY id",
        "id", "x",
    )
    assert right == [text[-i:] if i else "" for i in ids], right


def test_substring_sql_varchar_column_stays_byte_level():
    # $planets.name is VARCHAR (ASCII-only), so byte-level and codepoint-level
    # slicing agree — this locks out a regression where the dynamic kernel
    # forced NVARCHAR-style codepoint counting onto VARCHAR.
    ids, names, out = _rows(
        "SELECT id, name, SUBSTRING(name, 2, id) AS x FROM $planets ORDER BY id",
        "id", "name", "x",
    )
    expected = [_py_substring(n, 2, i) for n, i in zip(names, ids)]
    assert out == expected, out


def test_substring_sql_null_position_and_count_rows():
    # NULLIF(id, 3) is NULL on the id=3 row: TVL null propagation must null out
    # that output row while leaving every other row correct.
    text = "héllo wörld😀"
    ids, out = _rows(
        f"SELECT id, SUBSTRING(CAST('{text}' AS NVARCHAR), id, NULLIF(id, 3)) AS x "
        "FROM $planets ORDER BY id",
        "id", "x",
    )
    expected = [None if i == 3 else _py_substring(text, i, i) for i in ids]
    assert out == expected, out


def test_substring_sql_negative_count_column_start():
    # Negative (literal) count with a COLUMN start — RIGHT-style "count back
    # from start" semantics, reached via the SUBSTRING dynamic kernel because
    # `start` is a column.
    ids, out = _rows(
        "SELECT id, SUBSTRING('hello world', id, -2) AS x FROM $planets ORDER BY id",
        "id", "x",
    )
    expected = [_py_substring("hello world", i, -2) for i in ids]
    assert out == expected, out


def test_substring_sql_mixed_literal_start_column_count():
    # start is a LITERAL, count is a COLUMN — the dynamic kernel supports mixed
    # literal/column operands for free (a literal linearizes to a constant-shape
    # vector), so this need not fall back to the all-literal fast path.
    text = "héllo wörld😀"
    ids, out = _rows(
        f"SELECT id, SUBSTRING(CAST('{text}' AS NVARCHAR), 2, id) AS x "
        "FROM $planets ORDER BY id",
        "id", "x",
    )
    expected = [_py_substring(text, 2, i) for i in ids]
    assert out == expected, out


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
