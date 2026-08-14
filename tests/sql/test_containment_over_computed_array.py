"""
Containment tests (`= ANY`, `@>`, `@>>`) over a COMPUTED array in
predicate position.

`JSONB_OBJECT_KEYS(doc)` worked in a projection but not in a WHERE clause: every
containment spelling was refused at plan time with "a filter predicate outside the
c-native kernel set". The containment kernels were never the problem - they work
fine against a plain ARRAY column. The gap was that an ARRAY's elements hang off
the column owner, not off the 40-byte DrakenVector, so the VM resolves them by
column identity against the morsel; a mid-expression intermediate has no identity
to resolve. The compiler already materializes such operands into their own column
(`_hoist_array_operands`), and already runs that hoist on the WHERE and HAVING
predicates - it just did not recognise the containment comparisons as ARRAY
consumers, only SORT/GREATEST/LEAST/LENGTH and `arr[i]`.

The hoist is deliberately SIDE-specific. `@>` and `@>>` carry an ARRAY-typed
*literal* needle set on their right, so probing both sides would materialize that
literal into a column and destroy the bind-time membership-blob lowering it feeds.

These tests assert ANSWERS, not merely the absence of a refusal: a filter that
passed every row would also "stop raising".
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.exceptions import NotSupportedError
from tests.helpers import execute_and_fetch_all

# Key 'd' is present ONLY for planets whose name starts with M, so a predicate that
# ignores the array entirely cannot pass these tests.
DOCS = """
(SELECT name,
        CASE WHEN name LIKE 'M%' THEN '{"a":1,"d":3}' ELSE '{"a":1,"z":9}' END AS doc
   FROM $planets)
"""

M_PLANETS = ["Mars", "Mercury"]
NON_M_PLANETS = ["Earth", "Jupiter", "Neptune", "Pluto", "Saturn", "Uranus", "Venus"]


def _names(sql):
    return sorted(row["name"] for row in execute_and_fetch_all(sql))


@pytest.mark.parametrize(
    "predicate, expected",
    [
        # the spellings from the original report (the report's fourth,
        # `ARRAY_CONTAINS(JSONB_OBJECT_KEYS(doc), 'd')`, is gone: the function
        # was removed, and it lowered to the identical AnyOpEq node as the
        # `= ANY` case directly above)
        ("'d' = ANY(JSONB_OBJECT_KEYS(doc))", M_PLANETS),
        ("JSONB_OBJECT_KEYS(doc) @> ('d','nope')", M_PLANETS),
        ("JSONB_OBJECT_KEYS(doc) @>> ('a','d')", M_PLANETS),
        # @> is contains-ANY, @>> is contains-ALL - they must not agree here
        ("JSONB_OBJECT_KEYS(doc) @> ('a','nope')", M_PLANETS + NON_M_PLANETS),
        ("JSONB_OBJECT_KEYS(doc) @>> ('a','nope')", []),
        # a key no document has
        ("'zzz' = ANY(JSONB_OBJECT_KEYS(doc))", []),
        # a key every document has
        ("'a' = ANY(JSONB_OBJECT_KEYS(doc))", M_PLANETS + NON_M_PLANETS),
        # composed with an unrelated predicate
        ("'d' = ANY(JSONB_OBJECT_KEYS(doc)) AND name LIKE 'Mar%'", ["Mars"]),
        ("'d' = ANY(JSONB_OBJECT_KEYS(doc)) OR name = 'Venus'", M_PLANETS + ["Venus"]),
        # the array operand is itself nested inside another array consumer
        ("'d' = ANY(SORT(JSONB_OBJECT_KEYS(doc)))", M_PLANETS),
    ],
)
def test_containment_over_computed_array_in_where(predicate, expected):
    assert _names(f"SELECT name FROM {DOCS} AS t WHERE {predicate}") == sorted(expected)


def test_containment_over_computed_array_in_having():
    """HAVING runs the same hoist as WHERE (both call _hoist_array_operands)."""
    sql = f"""
        SELECT name FROM {DOCS} AS t
         GROUP BY name, doc
        HAVING 'd' = ANY(JSONB_OBJECT_KEYS(doc))
    """
    assert _names(sql) == sorted(M_PLANETS)


def test_null_array_rows_do_not_match():
    """A NULL document yields a NULL key-set; containment over it is not a match,
    and must not crash the element scan."""
    docs = """
    (SELECT name, CASE WHEN name = 'Mars' THEN '{"d":1}' ELSE NULL END AS doc
       FROM $planets)
    """
    assert _names(f"SELECT name FROM {docs} AS t WHERE 'd' = ANY(JSONB_OBJECT_KEYS(doc))") == [
        "Mars"
    ]


def test_computed_array_from_split():
    """The gap was never JSON-specific - any function-produced ARRAY hit it."""
    assert _names("SELECT name FROM $planets WHERE 'Mar' = ANY(SPLIT(name, 's'))") == ["Mars"]


def test_hoisted_column_is_not_leaked_into_the_result():
    """The materialized array is a filter-internal helper; `SELECT *` must not
    grow a column because of how the predicate was compiled."""
    rows = execute_and_fetch_all(
        f"SELECT * FROM {DOCS} AS t WHERE 'd' = ANY(JSONB_OBJECT_KEYS(doc))"
    )
    assert sorted(rows[0].keys()) == ["doc", "name"], rows[0].keys()


# --- the literal-array forms must keep their bind-time lowerings ----------------
# These never needed a hoist: `x = ANY([...])` lowers to draken_in_list and the
# needle set of `@>`/`@>>` is baked into a membership blob. Materializing either
# into a column would be a silent pessimization, so they are pinned here.


def test_literal_array_any_still_works():
    assert _names("SELECT name FROM $planets WHERE id = ANY([1,2,3])") == [
        "Earth",
        "Mercury",
        "Venus",
    ]


def test_literal_array_contains_still_works():
    assert len(execute_and_fetch_all("SELECT LENGTH([1,2,3]) AS n FROM $planets LIMIT 1")) == 1


def test_fully_literal_containment_still_folds():
    assert len(execute_and_fetch_all("SELECT name FROM $planets WHERE ['a','b'] @> ('a','z')")) == 9


# --- refusal quality -----------------------------------------------------------


def test_refusal_names_the_construct_and_the_rewrite():
    """`NOT (x = ANY(arr))` is a separate, still-open gap - it is refused for a
    plain ARRAY column and for a literal array too, so it is not about computed
    arrays. What it must do is say WHICH sub-expression is the problem and how to
    rewrite it, rather than 'it will need rewriting to avoid that construct'."""
    with pytest.raises(NotSupportedError) as err:
        execute_and_fetch_all(
            f"SELECT name FROM {DOCS} AS t WHERE NOT ('d' = ANY(JSONB_OBJECT_KEYS(doc)))"
        )
    message = str(err.value)
    assert "JSONB_OBJECT_KEYS(doc)" in message, message
    assert "subquery" in message, message


def test_non_bool_predicate_refusal_is_distinct():
    """The filter gate has two independent halves. A program whose ops are all
    c-native but which does not end in a boolean mask must not be blamed on an
    operation - it gets the IS TRUE remedy instead."""
    with pytest.raises(NotSupportedError) as err:
        execute_and_fetch_all("SELECT name FROM $planets WHERE COALESCE(name = 'Mars', FALSE)")
    message = str(err.value)
    assert "true/false" in message, message
    assert "IS TRUE" in message, message


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
