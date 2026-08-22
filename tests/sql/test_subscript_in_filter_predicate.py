"""A subscript over a COMPUTED array must run in a filter predicate, not only in a
projection.

`SPLIT(domain,'.')[-3]` evaluated fine in the SELECT list and as a GROUP BY key, but
the identical expression in a WHERE or HAVING was refused:

    NotSupportedError: a subscript/extraction (-> ->> [i]) in a filter predicate
    `SPLIT(domain,'.')[-3] IS NOT NULL`, outside the c-native kernel set,
    is not supported.

The message blamed the kernel set. The kernel set was not the problem — the SAME
subscript with a COMPARISON on it (`SPLIT(domain,'.')[-3] = 'x'`) ran, and had run
all along. What separated the two was the shape of the node ABOVE the subscript.

An ARRAY's elements hang off the column owner, not off the 40-byte DrakenVector, so
every element-reading op needs its operand to be a real column. `_hoist_array_operands`
exists to give it one: it materializes the computed array into its own ExprProject
column and re-points the consumer at it. That hoist walked `parameters`, `left` and
`right` — and NOT `centre`, which is where NOT, IS NULL, IS NOT NULL, IS TRUE/FALSE
and BitwiseNot hang their single operand. So under any of those the hoist never saw
the array, the subscript reached the c-native gate with an arena intermediate for an
operand, and the gate refused it.

`LENGTH(SPLIT(x,'.')) IS NOT NULL` was the worse half of the same bug: LENGTH is
hoisted for a second reason (a produced ARRAY may only ever be a program's FINAL
result — c_execute_dv_inner carries the element vector out in ONE out_child slot), so
skipping the hoist did not refuse the query, it compiled a program that set out_child
in SPLIT and then returned INT64. That reached the engine and died at err_op=-97.

The fix is the three-word one the diagnosis implies: walk `centre` too, in the hoist,
in the remedy-naming helper (`_computed_array_subexpression`, which is why the refusal
carried no rewrite either), and in the `->`/`->>` shared-parse fusion.

PARITY IS THE ASSERTION. Every case below is checked against the subquery-wrapper
spelling — project the expression inside, filter outside — which is the rewrite the
error used to demand and which worked throughout. Same rows, or the fix is a
different query, not a fix.

JSON extraction (`->`, `->>`) is covered here too because the error names it in the
same breath ("-> ->> [i]"). It was never actually broken: its operand is a plain
column, so it needs no hoist. The tests pin that, so a future change to the shared
hoist cannot quietly take it out.

NOT COVERED, still open (see the session report):
  * HAVING that REPEATS a computed GROUP BY key expression instead of using its
    alias — `GROUP BY UPPER(name) HAVING UPPER(name) > 'A'` — fails in the planner
    with "projecting a column the engine could not resolve here". That is not a
    subscript defect: it hits `id+gravity` and `UPPER(name)` identically. The alias
    spelling (`HAVING u > 'A'`) is what is pinned below.
  * `<> ALL(array)`, and the `NOT (x = ANY(array))` that De Morgan turns into it,
    has no native kernel at all — it is refused over a plain ARRAY column too.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

# Four rows of dotted names, one NULL, one with no delimiter at all — so the
# subscript has an in-range hit, an out-of-range miss (NULL, not an error) and a
# NULL input to carry through.
NAMES = """(SELECT * FROM (VALUES
    ('alpha.beta.gamma.delta'),
    ('one.two.three'),
    ('solo'),
    (NULL)
) AS v(name))"""

# A JSON document column, same four-row shape.
DOCS = """(SELECT * FROM (VALUES
    ('{"city":"paris","tags":["p","q"]}'),
    ('{"city":"lima"}'),
    ('{"other":1}'),
    (NULL)
) AS v(doc))"""


def results(sql):
    session = opteryx.session()
    out: dict = {}
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        for key, values in morsel.to_arrow().to_pydict().items():
            out.setdefault(key, []).extend(values)
    return out


def rows(sql):
    """The single output column, as a list."""
    values = results(sql)
    assert len(values) == 1, f"expected one output column, got {list(values)}"
    return list(values.values())[0]


def row_tuples(sql):
    """Every output row as a tuple, column order preserved."""
    values = results(sql)
    return list(zip(*values.values())) if values else []


def assert_parity(direct, wrapped):
    """`direct` (filter on the expression) must answer exactly what `wrapped` (the
    subquery-wrapper rewrite) answers. Sorted: neither spelling promises an order."""
    got, want = row_tuples(direct), row_tuples(wrapped)
    assert sorted(got, key=repr) == sorted(want, key=repr), f"{got!r} != {want!r}"
    return got


# ---------------------------------------------------------------- subscript, WHERE

# Every one of these is a subscript over a COMPUTED array under a `centre`-linked
# node. Each was NotSupportedError; each is checked against the wrapper form.
@pytest.mark.parametrize(
    "predicate",
    [
        "SPLIT(name,'.')[1] IS NOT NULL",
        "SPLIT(name,'.')[1] IS NULL",
        "SPLIT(name,'.')[-1] IS NOT NULL",
        "SPLIT(name,'.')[9] IS NULL",
        "NOT (SPLIT(name,'.')[1] IS NULL)",
        "(SPLIT(name,'.')[0] = 'alpha') IS TRUE",
        "(SPLIT(name,'.')[0] = 'alpha') IS NOT TRUE",
        "(SPLIT(name,'.')[0] = 'alpha') IS FALSE",
    ],
)
def test_subscript_under_a_unary_operator_matches_the_wrapper(predicate):
    inner = predicate.replace("SPLIT(name,'.')", "k")
    assert_parity(
        f"SELECT name FROM {NAMES} WHERE {predicate}",
        f"SELECT name FROM (SELECT name, SPLIT(name,'.') AS k FROM {NAMES}) AS s WHERE {inner}",
    )


def test_subscript_under_a_comparison_was_never_broken():
    """The shape that always ran — the control that proved the kernel set was not
    the problem. It must still run, and still agree with the wrapper."""
    assert_parity(
        f"SELECT name FROM {NAMES} WHERE SPLIT(name,'.')[0] = 'alpha'",
        f"SELECT name FROM (SELECT name, SPLIT(name,'.') AS k FROM {NAMES}) AS s WHERE k[0] = 'alpha'",
    )


def test_negative_and_out_of_range_subscripts_answer_the_same_in_a_filter():
    """Zero-based with Python-style negative indexing, and out of range is NULL, not
    an error — the convention now stated in SPLIT's and `[]`'s documentation. A
    filter must not disagree with a projection about which element it read."""
    assert rows(f"SELECT SPLIT(name,'.')[-1] AS e FROM {NAMES}") == [
        "delta", "three", "solo", None,
    ]
    assert rows(f"SELECT name FROM {NAMES} WHERE SPLIT(name,'.')[-1] = 'delta'") == [
        "alpha.beta.gamma.delta",
    ]
    # [9] is past the end of every row: NULL everywhere, so IS NULL keeps every
    # non-NULL-input row and the predicate never raises.
    assert rows(f"SELECT COUNT(*) AS n FROM {NAMES} WHERE SPLIT(name,'.')[9] IS NULL") == [4]


# ----------------------------------------------- other array-consuming ops, WHERE

def test_length_of_a_computed_array_under_is_not_null():
    """The err_op=-97 case: not a refusal, a program that reached the engine with a
    stale out_child. LENGTH needs the hoist even though its kernel needs no child."""
    assert_parity(
        f"SELECT name FROM {NAMES} WHERE LENGTH(SPLIT(name,'.')) IS NOT NULL",
        f"SELECT name FROM (SELECT name, SPLIT(name,'.') AS k FROM {NAMES}) AS s WHERE LENGTH(k) IS NOT NULL",
    )


def test_sort_of_a_computed_array_subscripted_under_is_not_null():
    """Nested: SORT's operand hoists first, then the subscript's. Depth-first order
    is what makes one pass enough."""
    assert_parity(
        f"SELECT name FROM {NAMES} WHERE SORT(SPLIT(name,'.'))[0] IS NOT NULL",
        f"SELECT name FROM (SELECT name, SPLIT(name,'.') AS k FROM {NAMES}) AS s WHERE SORT(k)[0] IS NOT NULL",
    )


def test_any_over_a_computed_array_under_is_true():
    """`= ANY` is hoisted by side (right), and sat under a `centre` node here."""
    assert_parity(
        f"SELECT name FROM {NAMES} WHERE ('beta' = ANY(SPLIT(name,'.'))) IS TRUE",
        f"SELECT name FROM (SELECT name, SPLIT(name,'.') AS k FROM {NAMES}) AS s WHERE ('beta' = ANY(k)) IS TRUE",
    )


# ------------------------------------------------------- JSON extraction, WHERE

@pytest.mark.parametrize(
    "predicate",
    [
        "doc->>'city' IS NOT NULL",
        "doc->>'city' IS NULL",
        "doc->'city' IS NOT NULL",
        "doc->>'city' = 'paris'",
        "NOT (doc->>'city' IS NULL)",
        "(doc->>'city' = 'paris') IS TRUE",
    ],
)
def test_json_extraction_in_a_filter_matches_the_wrapper(predicate):
    inner = predicate.replace("doc->>'city'", "c").replace("doc->'city'", "r")
    assert_parity(
        f"SELECT doc FROM {DOCS} WHERE {predicate}",
        f"SELECT doc FROM (SELECT doc, doc->>'city' AS c, doc->'city' AS r FROM {DOCS}) AS s WHERE {inner}",
    )


def test_two_extractions_on_one_column_under_a_unary_still_answer():
    """The shared-parse fusion groups `->`/`->>` on the same column so the document
    is parsed once. It walks `centre` now too; the assertion here is the ANSWER,
    which the fusion must not change either way."""
    assert_parity(
        f"SELECT doc FROM {DOCS} WHERE doc->>'city' IS NOT NULL AND doc->>'other' IS NULL",
        f"SELECT doc FROM (SELECT doc, doc->>'city' AS a, doc->>'other' AS b FROM {DOCS}) AS s "
        "WHERE a IS NOT NULL AND b IS NULL",
    )


# -------------------------------------------------------------------- HAVING

def test_subscript_group_key_filtered_in_having_by_alias():
    """The user-facing shape: group on a subscript, then drop the NULL group. The
    alias is the spelling that resolves to the already-computed key column; parity
    is against the subquery wrapper, which is the rewrite the docs recommended."""
    assert_parity(
        f"SELECT org FROM (SELECT SPLIT(name,'.')[1] AS org, COUNT(*) AS n FROM {NAMES} "
        "GROUP BY org HAVING org IS NOT NULL) AS h",
        f"SELECT org FROM (SELECT SPLIT(name,'.')[1] AS org, COUNT(*) AS n FROM {NAMES} "
        "GROUP BY org) AS t WHERE t.org IS NOT NULL",
    )


# NO ROLLUP TEST HERE, DELIBERATELY. `GROUP BY ROLLUP(...)` with a filter on a group
# key currently returns the rollup SUBTOTAL rows that the filter should have removed —
# a silent wrong answer, and one that predates all of this (it reproduces on plain
# columns, and on the subquery-wrapper spelling too). An earlier version of this file
# asserted the HAVING form against the wrapper form and PASSED, because both are wrong
# in the same way: parity is only an oracle when at least one side is right. The shape
# is reported in the session notes; assertions belong here once the pushdown is fixed,
# not before.


def test_json_extraction_group_key_filtered_in_having_by_alias():
    assert_parity(
        f"SELECT city FROM (SELECT doc->>'city' AS city, COUNT(*) AS n FROM {DOCS} "
        "GROUP BY city HAVING city IS NOT NULL) AS h",
        f"SELECT city FROM (SELECT doc->>'city' AS city, COUNT(*) AS n FROM {DOCS} "
        "GROUP BY city) AS t WHERE t.city IS NOT NULL",
    )


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
