"""Execution-level regression tests for CAST(<json text> AS ARRAY<element>).

The companion to tests/unit/planner/test_array_cast_element_type.py, which pins the
element type SURVIVING the planner and explicitly defers what the kernel does with it.
This file pins the kernel's semantics, which are architect rulings, not defaults:

  1. Only VARIANT and VARCHAR (holding JSON array text) may cast to ARRAY. No other
     scalar can — `1::ARRAY<INTEGER>` is a plan-time error, NOT the one-element `[1]`.
  2. A row whose JSON is not an array (an object, or a bare scalar) FAILS.
  3. An element that is not already of the declared element type FAILS THE WHOLE ROW —
     elements are never individually nulled, and a number is never stringified to
     satisfy ARRAY<VARCHAR>.
  4. A plain `::` cast raises on a failing row; TRY_CAST nulls that row instead. Both
     dispositions share one kernel and one definition of "failing", so they cannot
     drift — `safe` is a field in cast_array_ctx, not a second kernel.
  5. A JSON `null` element is NOT a failure: it is an absent value, not a wrong-typed
     one, so it becomes a NULL element and the row survives.

Rule 3 is the one most likely to be "helpfully" relaxed later into per-element NULLs or
implicit stringification. That would silently change answers for every existing query,
which is why it is pinned here rather than left to the kernel's own comments.

There is no Python fallback for this cast (draken_cast_to_array is the only
implementation), so an unsupported source must fail at PLAN time, never degrade.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx

# A non-literal source: the sub-select makes `j` a real VARCHAR column, so these
# exercise the runtime kernel over a vector rather than any plan-time shortcut.
def _column_source(json_text: str) -> str:
    return f"SELECT j::{{cast}} FROM (SELECT '{json_text}' AS j FROM $planets LIMIT 2) AS t"


def _rows(sql, limit=4):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(morsel[i])
            if len(out) >= limit:
                return out
    return out


def _first(sql):
    rows = _rows(sql, limit=1)
    assert rows, f"no rows for {sql}"
    return rows[0][0]


# ---------------------------------------------------------------------------
# Happy paths — one per supported element-type family
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "json_text, target, expected",
    [
        ('["a","b","c"]', "ARRAY<VARCHAR>", ["a", "b", "c"]),
        ("[1,2,3]", "ARRAY<INTEGER>", [1, 2, 3]),
        ("[1.5,2.5]", "ARRAY<DOUBLE>", [1.5, 2.5]),
        ("[true,false]", "ARRAY<BOOLEAN>", [True, False]),
        ("[]", "ARRAY<INTEGER>", []),
        # Rule 5: a JSON null is an absent element, not a wrong-typed one.
        ("[1,null,3]", "ARRAY<INTEGER>", [1, None, 3]),
    ],
)
def test_cast_to_array_values(json_text, target, expected):
    assert _first(f"SELECT '{json_text}'::{target} FROM $planets LIMIT 1") == expected


def test_cast_to_array_over_a_column_not_a_literal():
    """The kernel must produce the same answer for a vector source as for a literal."""
    sql = "SELECT j::ARRAY<INTEGER> FROM (SELECT '[1,2,3]' AS j FROM $planets LIMIT 2) AS t"
    rows = _rows(sql, limit=2)
    assert [r[0] for r in rows] == [[1, 2, 3], [1, 2, 3]]


def test_variant_source_casts():
    """A VARIANT (the result of `->`) is the intended source — it must be admitted.

    birth_place->'town' is a JSON *string*, not an array, so this is admitted by the
    planner and then rejected by the kernel at run time. That split is the point: the
    source TYPE is legal, the row VALUE is not.
    """
    with pytest.raises(Exception) as err:
        _rows("SELECT (birth_place->'town')::ARRAY<VARCHAR> FROM testdata.astronauts", limit=1)
    assert "not a JSON array" in str(err.value)


# ---------------------------------------------------------------------------
# Rule 1 — source types. A scalar is never wrapped into a 1-element array.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("expr", ["1::ARRAY<INTEGER>", "1.5::ARRAY<DOUBLE>"])
def test_scalar_source_is_refused_at_plan_time(expr):
    with pytest.raises(Exception) as err:
        _rows(f"SELECT {expr} FROM $planets LIMIT 1")
    message = str(err.value)
    assert "ARRAY" in message
    # Must name the real rule, not surface a confusing internal parse failure.
    assert "unknown type" not in message


# ---------------------------------------------------------------------------
# Rules 2 + 3 — strict rejection. Plain `::` raises.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "json_text, target, reason",
    [
        ('{"k":1}', "ARRAY<INTEGER>", "not a JSON array"),      # object root
        ("42", "ARRAY<INTEGER>", "not a JSON array"),           # bare scalar root
        ("notjson", "ARRAY<VARCHAR>", "invalid JSON"),          # unparseable
        ('[1,"x"]', "ARRAY<INTEGER>", "element does not match"),  # mixed elements
        ("[1,2]", "ARRAY<VARCHAR>", "element does not match"),  # no implicit stringify
        ('["1","2"]', "ARRAY<INTEGER>", "element does not match"),  # no implicit parse
        ("[1.5]", "ARRAY<INTEGER>", "element does not match"),  # no silent truncation
    ],
)
def test_bad_row_raises_under_plain_cast(json_text, target, reason):
    with pytest.raises(Exception) as err:
        _rows(f"SELECT '{json_text}'::{target} FROM $planets LIMIT 1")
    assert reason in str(err.value)


def test_narrow_int_range_is_checked_not_wrapped():
    """An out-of-range value fails the row rather than silently wrapping."""
    with pytest.raises(Exception) as err:
        _rows("SELECT '[999]'::ARRAY<INT8> FROM $planets LIMIT 1")
    assert "element does not match" in str(err.value)


# ---------------------------------------------------------------------------
# Rule 4 — TRY_CAST nulls exactly the rows a plain cast raises on.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "json_text, target",
    [
        ('{"k":1}', "ARRAY<INTEGER>"),
        ("42", "ARRAY<INTEGER>"),
        ("notjson", "ARRAY<VARCHAR>"),
        ('[1,"x"]', "ARRAY<INTEGER>"),
        ("[1,2]", "ARRAY<VARCHAR>"),
    ],
)
def test_try_cast_nulls_the_rows_plain_cast_rejects(json_text, target):
    assert _first(f"SELECT TRY_CAST('{json_text}' AS {target}) FROM $planets LIMIT 1") is None


def test_try_cast_still_returns_good_rows():
    """TRY_CAST must not null rows that are fine — it changes disposition, not the rule."""
    assert _first("SELECT TRY_CAST('[1,2,3]' AS ARRAY<INTEGER>) FROM $planets LIMIT 1") == [1, 2, 3]


# ---------------------------------------------------------------------------
# Subscripting a computed ARRAY — the point of the cast existing.
#
# `arr[i]` reaches its elements through the column owner's child. A COMPUTED array
# has no column identity, so the compiler hoists it into its own ExprProject column
# (compiler.py _hoist_array_operands) and the subscript reads that column.
#
# That hoist only runs for expressions routed through _add_computed. A fully
# constant-folded expression is not, so it stays in the GIL VM — where a c-native
# kernel's ARRAY result used to be folded into the frame arena, which has nowhere to
# put VecResult.child. The offsets survived, the elements did not, and the subscript
# reported "DRAKEN_ARRAY vector has no child". The GIL VM now OWNS an ARRAY result
# (rc 6) instead of folding it, so the child survives. These pin both routes.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "expr, expected",
    [
        ("('[\"a\",\"b\",\"c\"]'::ARRAY<VARCHAR>)[0]", "a"),
        ("('[\"a\",\"b\",\"c\"]'::ARRAY<VARCHAR>)[2]", "c"),
        ("('[10,20,30]'::ARRAY<INTEGER>)[1]", 20),
        # Not a cast, but the same VM ownership path — a pre-existing ARRAY producer
        # that was broken by the identical arena/child mismatch.
        ("SPLIT('a,b,c', ',')[0]", "a"),
    ],
)
def test_subscript_of_a_constant_folded_array(expr, expected):
    assert _first(f"SELECT {expr} FROM $planets LIMIT 1") == expected


def test_subscript_of_a_computed_array_over_a_real_column():
    """The hoisted route: SPLIT over a column, subscripted. Must stay working."""
    rows = _rows("SELECT SPLIT(name, 'a')[0] FROM $planets LIMIT 2", limit=2)
    assert [r[0] for r in rows] == ["Mercury", "Venus"]


def test_subscript_past_the_end_is_null_not_an_error():
    assert _first("SELECT ('[]'::ARRAY<INTEGER>)[0] FROM $planets LIMIT 1") is None


# ---------------------------------------------------------------------------
# LENGTH over a computed ARRAY.
#
# `evaluate_c_native` refused any program that produced a child at all, so
# LENGTH(SPLIT(<literals>)) — whose FINAL result is INT64 — was rejected along with
# genuine ARRAY results. draken_length_array reads only the offsets and needs no
# child (which is precisely why it composes over a computed array where SORT and
# ARRAY_CONTAINS cannot), so the child there is a consumed intermediate and freeing
# it is correct. The guard now keys on the FINAL result type, not on "a child
# existed". These pin the values, so a future re-tightening cannot pass by
# returning the wrong count.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "expr, expected",
    [
        ("LENGTH(SPLIT('a,b,c', ','))", 3),
        ("LENGTH(SPLIT('a,b,c,d,e', ','))", 5),
        ("LENGTH('[1,2,3,4]'::ARRAY<INTEGER>)", 4),
        ("LENGTH('[]'::ARRAY<INTEGER>)", 0),
        ("LENGTH('[\"x\"]'::ARRAY<VARCHAR>)", 1),
    ],
)
def test_length_of_a_computed_array(expr, expected):
    assert _first(f"SELECT {expr} FROM $planets LIMIT 1") == expected


def test_length_of_a_computed_array_over_a_column():
    """The hoisted two-program route must keep working."""
    rows = _rows("SELECT LENGTH(SPLIT(name, 'a')) FROM $planets LIMIT 2", limit=2)
    assert [r[0] for r in rows] == [1, 1]


def test_length_of_a_native_array_column():
    rows = _rows("SELECT LENGTH(missions) FROM testdata.astronauts LIMIT 2", limit=2)
    assert [r[0] for r in rows] == [2, 1]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
