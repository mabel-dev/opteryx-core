"""The quantified LIKE forms — `LIKE ANY`, `LIKE ALL`, `NOT LIKE ALL` — and the
`NOT LIKE ANY` spelling that is refused.

The three runnable forms mean:

    x LIKE ANY (a, b)      =  x LIKE a  OR  x LIKE b
    x LIKE ALL (a, b)      =  x LIKE a  AND x LIKE b
    x NOT LIKE ALL (a, b)  =  (NOT x LIKE a) AND (NOT x LIKE b)  =  NOT(x LIKE ANY (a, b))

`NOT LIKE ALL` is the De Morgan dual of `LIKE ANY`, NOT the negation of `LIKE ALL`,
and that is the whole trap this file exists to pin. The kernel carries one `negate`
flag that inverts the ANY verdict; because that is exactly NOT-LIKE-ALL, the
operator `AnyOpNotLike` used to be wired to it, and so `x NOT LIKE ANY (...)`
silently returned the ALL answer — for 'Mars' against ('M%','V%') the engine said
FALSE where the compositional reading says TRUE.

`NOT LIKE ANY (a, b)` decomposes to `(NOT a) OR (NOT b)` = `NOT(a AND b)`, which is
true for every subject unless EVERY pattern matches. Rather than pick between that
near-vacuous reading and the useful one people mean, the planner REFUSES the
spelling and names both unambiguous alternatives.

⛔ ORACLE NOTE: the expected values below are computed by `_like`, a glob matcher
written here from the LIKE definition, and combined with plain Python `and`/`or`.
They are NOT taken from the engine's own AND/OR chain of scalar LIKEs. Comparing a
quantified form against the chain the fusion rewrite BUILDS FROM would be circular —
`rewrite_anded_not_like_to_all` turns `x NOT LIKE a AND x NOT LIKE b` into exactly
the `AllOpNotLike` node under test, so the two sides would be one implementation
agreeing with itself.
"""

import os
import re
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError

# $planets is the one relation every install has. Spelled out so the expected
# values below are computed against a fixed list, not against a second query.
PLANETS = [
    "Mercury",
    "Venus",
    "Earth",
    "Mars",
    "Jupiter",
    "Saturn",
    "Uranus",
    "Neptune",
    "Pluto",
]


def _like(subject: str, pattern: str) -> bool:
    """SQL LIKE, written from the definition: `%` is any run of characters, `_` is
    exactly one, everything else is literal. Independent of the engine's matcher —
    this is the oracle, so it must not call into opteryx."""
    expanded = "".join(
        ".*" if ch == "%" else ("." if ch == "_" else re.escape(ch)) for ch in pattern
    )
    return re.match(f"^{expanded}$", subject, re.DOTALL) is not None


def verdicts(expression: str) -> dict:
    """{planet name: value of `name <expression>`} for every row of $planets."""
    session = opteryx.session()
    out: dict = {}
    for morsel in session.execute_to_morsels(
        f"SELECT name, name {expression} AS v FROM $planets"
    ):
        if morsel is None:
            continue
        table = morsel.to_arrow().to_pydict()
        for name, value in zip(table["name"], table["v"]):
            out[name] = value
    return out


def test_like_any_is_a_disjunction():
    assert verdicts("LIKE ANY ('M%', 'V%')") == {
        n: _like(n, "M%") or _like(n, "V%") for n in PLANETS
    }


def test_like_all_is_a_conjunction():
    # Two patterns that overlap on some rows and not others, so the answer is
    # neither all-true nor all-false.
    assert verdicts("LIKE ALL ('%a%', '%r%')") == {
        n: _like(n, "%a%") and _like(n, "%r%") for n in PLANETS
    }


def test_like_all_disjoint_patterns_match_nothing():
    # No planet starts with both M and V, so the conjunction is empty. This is the
    # case an ANY/ALL mix-up gets loudly wrong: LIKE ANY here matches three rows.
    assert verdicts("LIKE ALL ('M%', 'V%')") == {n: False for n in PLANETS}
    assert any(verdicts("LIKE ANY ('M%', 'V%')").values())


def test_not_like_all_is_the_negation_of_like_any():
    # The identity the kernel's `negate` flag actually implements.
    expected = {n: (not _like(n, "M%")) and (not _like(n, "V%")) for n in PLANETS}
    assert verdicts("NOT LIKE ALL ('M%', 'V%')") == expected
    assert verdicts("NOT LIKE ALL ('M%', 'V%')") == {
        n: not v for n, v in verdicts("LIKE ANY ('M%', 'V%')").items()
    }


def test_not_like_all_is_not_the_negation_of_like_all():
    # Guards the collapse that would make the two ALL forms complements. They are
    # not: 'Earth' matches neither 'M%' nor 'V%', so NOT LIKE ALL is TRUE for it,
    # and LIKE ALL is FALSE for it — both false-negatives of each other's negation
    # would go unnoticed if only one of them were asserted.
    like_all = verdicts("LIKE ALL ('M%', 'V%')")
    not_like_all = verdicts("NOT LIKE ALL ('M%', 'V%')")
    assert like_all["Mercury"] is False
    assert not_like_all["Mercury"] is False  # NOT the complement of the line above
    assert like_all["Earth"] is False
    assert not_like_all["Earth"] is True


# Each entry is a pattern SHAPE the plan-time compiler buckets differently. The
# contains case matters most: the ANY path answers it with an Aho-Corasick
# automaton, which cannot say whether EVERY needle occurred, so require_all
# re-routes those needles to the per-pattern glob matcher.
LIKE_ALL_SHAPES = [
    ("%r%", "%u%", "%n%"),  # three contains-needles: the require_all glob route
    ("_a%", "%s"),  # `_` single-char wildcard, and a suffix
    ("Mars",),  # a bare literal, no wildcards at all
    ("%",),  # matches everything: constrains nothing in a conjunction
]


def test_like_all_shapes():
    for patterns in LIKE_ALL_SHAPES:
        rendered = ", ".join(f"'{p}'" for p in patterns)
        expected = {n: all(_like(n, p) for p in patterns) for n in PLANETS}
        assert verdicts(f"LIKE ALL ({rendered})") == expected, rendered


def test_ilike_all_folds_case_on_both_sides():
    assert verdicts("ILIKE ALL ('m%', '%Y')") == {
        n: _like(n.lower(), "m%") and _like(n.lower(), "%y") for n in PLANETS
    }


def test_not_ilike_all_folds_case_on_both_sides():
    assert verdicts("NOT ILIKE ALL ('m%', 'v%')") == {
        n: (not _like(n.lower(), "m%")) and (not _like(n.lower(), "v%"))
        for n in PLANETS
    }


# --- three-valued logic -----------------------------------------------------
# A NULL pattern is an UNKNOWN term. OR and AND absorb it on OPPOSITE sides, so
# the two quantifiers must NOT share one null rule:
#   ANY (OR):  TRUE dominates  -> a match is TRUE even alongside NULL; a miss is NULL
#   ALL (AND): FALSE dominates -> a miss is FALSE even alongside NULL; a match is NULL


def test_like_any_null_pattern_softens_only_misses():
    got = verdicts("LIKE ANY ('M%', NULL)")
    assert got["Mercury"] is True  # TRUE OR UNKNOWN = TRUE
    assert got["Mars"] is True
    assert got["Venus"] is None  # FALSE OR UNKNOWN = UNKNOWN


def test_like_all_null_pattern_softens_only_matches():
    got = verdicts("LIKE ALL ('M%', NULL)")
    assert got["Mercury"] is None  # TRUE AND UNKNOWN = UNKNOWN
    assert got["Mars"] is None
    assert got["Venus"] is False  # FALSE AND UNKNOWN = FALSE


def test_not_like_all_null_pattern_negates_the_any_verdict():
    # NOT LIKE ALL negates LIKE ANY, so it inherits the ANY null rule with TRUE and
    # FALSE swapped and NULL left alone.
    got = verdicts("NOT LIKE ALL ('M%', NULL)")
    assert got["Mercury"] is False
    assert got["Venus"] is None


# --- the refused spelling ---------------------------------------------------


REFUSED = [
    "SELECT name NOT LIKE ANY ('M%', 'V%') FROM $planets",
    "SELECT name NOT ILIKE ANY ('m%', 'v%') FROM $planets",
    "SELECT name FROM $planets WHERE name NOT LIKE ANY ('M%', 'V%')",
    "SELECT name NOT LIKE ANY ('M%') FROM $planets",  # single pattern, still refused
    "SELECT name NOT LIKE ANY '%M%' FROM $planets",  # unbracketed spelling
]


def test_not_like_any_is_refused():
    """It used to return the NOT-LIKE-ALL answer. Refusing beats guessing: the two
    readings differ on real data, so a silent choice is a silent wrong answer."""
    session = opteryx.session()
    for sql in REFUSED:
        with pytest.raises(UnsupportedSyntaxError) as raised:
            list(session.execute_to_morsels(sql))
        # The message has to name the way out, or it just moves the confusion.
        assert "ALL" in str(raised.value), sql


def test_not_like_any_refusal_names_the_ilike_spelling():
    session = opteryx.session()
    with pytest.raises(UnsupportedSyntaxError) as raised:
        list(session.execute_to_morsels("SELECT name NOT ILIKE ANY ('m%') FROM $planets"))
    assert "NOT ILIKE ALL" in str(raised.value)


def test_the_original_defect_row():
    """The exact repro from the P0 report. 'Mars' against ('M%','V%'): the engine
    answered FALSE for `NOT LIKE ANY` where the compositional reading says TRUE.
    That spelling is now refused, and the answer people wanted — match none of the
    patterns — is FALSE for Mars under the correctly named operator."""
    assert verdicts("NOT LIKE ALL ('M%', 'V%')")["Mars"] is False
    assert verdicts("NOT LIKE ALL ('M%', 'V%')")["Earth"] is True


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
