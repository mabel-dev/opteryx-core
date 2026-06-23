"""
Regression tests for comparing an integer column to a FLOAT literal.

Before the fix, an integer column compared to a non-integer FLOAT literal
produced a bogus all-true / all-false mask: the c-native fast path correctly
declined the INT-vs-FLOAT64 type mismatch, but the fallback read the float64
bytes through an integer kernel (garbage), so `id > 4.5` returned every row and
`id < 4.5` returned none.

Two layers now make this correct:
  (a) native compare_vector / compare_scalar promote the integer operand to
      FLOAT64 and compare as reals (the general fallback, also covers the
      float-column-vs-int direction and out-of-int64-range literals);
  (b) a bind-time rewrite collapses `int_expr <op> fractional_const` to an
      exact integer bound (id > 4.5 → id >= 5; id = 4.5 → FALSE), keeping the
      native integer fast path and enabling row-group pruning.

Ground-truth values cross-checked against DuckDB over the same data.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


def _ids(sql):
    sess = opteryx.session()
    out = []
    for m in sess.execute_to_morsels(sql):
        out += m.column(b"id").to_pylist()
    return sorted(out)


def _explain_filter(sql):
    sess = opteryx.session()
    for m in sess.execute_to_morsels("EXPLAIN " + sql):
        for row in m:
            line = " | ".join(str(x) for x in row)
            if "Filter" in line:
                return line
    return ""


# id is INT8 in $planets; values are 1..9.
ALL = [1, 2, 3, 4, 5, 6, 7, 8, 9]

# DuckDB-verified expectations for `id <op> 4.5`.
FRACTIONAL = {
    ">": [5, 6, 7, 8, 9],
    ">=": [5, 6, 7, 8, 9],
    "<": [1, 2, 3, 4],
    "<=": [1, 2, 3, 4],
    "=": [],
    "!=": ALL,
}


def test_int_column_vs_fractional_float_literal():
    """The reported P0: every operator was wrong (only '=' correct by luck)."""
    for op, expected in FRACTIONAL.items():
        assert _ids(f"SELECT id FROM $planets WHERE id {op} 4.5") == expected, op


def test_int_column_vs_fractional_literal_on_left():
    """Literal on the left flips the operator: 4.5 < id ≡ id > 4.5 ≡ id >= 5."""
    assert _ids("SELECT id FROM $planets WHERE 4.5 < id") == [5, 6, 7, 8, 9]
    assert _ids("SELECT id FROM $planets WHERE 4.5 >= id") == [1, 2, 3, 4]


def test_int_column_vs_whole_float_literal():
    """Whole-valued floats are exact integer comparisons (no off-by-one)."""
    assert _ids("SELECT id FROM $planets WHERE id = 4.0") == [4]
    assert _ids("SELECT id FROM $planets WHERE id > 4.0") == [5, 6, 7, 8, 9]
    assert _ids("SELECT id FROM $planets WHERE id <= 4.0") == [1, 2, 3, 4]
    assert _ids("SELECT id FROM $planets WHERE id != 4.0") == [1, 2, 3, 5, 6, 7, 8, 9]


def test_int_column_vs_negative_fractional_literal():
    """floor/ceil handle negative fractions: id > -4.5 ≡ id >= -4 (all rows)."""
    assert _ids("SELECT id FROM $planets WHERE id > -4.5") == ALL
    assert _ids("SELECT id FROM $planets WHERE id < -4.5") == []


def test_int64_column_vs_fractional_float_literal():
    """numberOfMoons is INT64 (the direct, non-narrow path)."""
    # DuckDB: numberOfMoons in {0,0,1,2,79,82,27,14,5}; > 1.5 → {2,79,82,27,14,5}.
    got = _ids("SELECT id FROM $planets WHERE number_of_moons > 1.5")
    assert got == _ids("SELECT id FROM $planets WHERE number_of_moons >= 2")


def test_out_of_int64_range_literal_uses_runtime_promotion():
    """Literals outside INT64 range decline the rewrite and hit kernel path (a)."""
    # Plan is NOT rewritten — still a float comparison.
    assert "1e+30" in _explain_filter("SELECT id FROM $planets WHERE id > 1e30")
    assert _ids("SELECT id FROM $planets WHERE id > 1e30") == []
    assert _ids("SELECT id FROM $planets WHERE id < 1e30") == ALL


def test_bind_time_rewrite_produces_integer_bounds():
    """Lock in option (b): the fractional comparison becomes an exact int bound."""
    assert "id >= 5" in _explain_filter("SELECT id FROM $planets WHERE id > 4.5")
    assert "id >= 5" in _explain_filter("SELECT id FROM $planets WHERE id >= 4.5")
    assert "id <= 4" in _explain_filter("SELECT id FROM $planets WHERE id < 4.5")
    assert "id <= 4" in _explain_filter("SELECT id FROM $planets WHERE id <= 4.5")
    # Equality against a fractional value can never hold → constant False.
    assert "False" in _explain_filter("SELECT id FROM $planets WHERE id = 4.5")


if __name__ == "__main__":  # pragma: no cover
    test_int_column_vs_fractional_float_literal()
    test_int_column_vs_fractional_literal_on_left()
    test_int_column_vs_whole_float_literal()
    test_int_column_vs_negative_fractional_literal()
    test_int64_column_vs_fractional_float_literal()
    test_out_of_int64_range_literal_uses_runtime_promotion()
    test_bind_time_rewrite_produces_integer_bounds()
    print("✅ okay")
