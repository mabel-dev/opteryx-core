"""Tests for `MATCH (column) AGAINST (string)`.

MATCH was declared in the catalog but never implemented — the callable raised
NotImplementedError and the native engine refused the predicate outright. It is now
`draken__match_against_2` (draken/ops/kernels/function_vector_distance.cpp), defined as::

    COSINE_SIMILARITY(column, string) >= @@match_threshold

The kernel runs the text-cosine body itself rather than scoring independently, so MATCH
and COSINE_SIMILARITY cannot drift apart. `test_match_agrees_with_cosine_similarity`
pins that: the same split-brain already happened once, when the text cosine overload had
its own embedder and answered COSINE_SIMILARITY('dog','puppy') as 0.0 lexically while
COSINE_SIMILARITY(EMBED('dog'),EMBED('puppy')) answered 0.80 under MiniLM.

The threshold is a session variable, not a constant, because a similarity score is only
meaningful against the ACTIVE embedder. Under the core (lexical, static-hash) EMBED the
scores are bimodal — measured against 'Earth' over $planets: Earth 1.0, Mars 0.043, the
rest <= 0.018 and some negative — so at the 0.5 default MATCH is a case-insensitive exact
match. Under a semantic capability the same 0.5 is a real similarity cut. These tests run
on the core embedder and therefore assert the lexical behaviour, which is the honest
description of what a default build does.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError


def _rows(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        for row in morsel:
            out.append(tuple(row))
    return out


def test_match_runs_over_a_column():
    """The headline case: MATCH is a working filter over a real column."""
    rows = _rows("SELECT name FROM $planets WHERE MATCH (name) AGAINST ('Earth')")
    assert [r[0] for r in rows] == ["Earth"], rows


def test_match_is_case_insensitive_under_the_lexical_embedder():
    """The tokenizer lowercases, so 'earth' and 'Earth' embed identically (score 1.0)."""
    rows = _rows("SELECT name FROM $planets WHERE MATCH (name) AGAINST ('earth')")
    assert [r[0] for r in rows] == ["Earth"], rows


def test_match_returns_no_rows_when_nothing_clears_the_threshold():
    rows = _rows("SELECT name FROM $planets WHERE MATCH (name) AGAINST ('Vulcan')")
    assert rows == []


def test_match_is_a_boolean_expression_not_only_a_predicate():
    """The catalog declares BOOLEAN; it must be selectable, not just filterable."""
    rows = _rows("SELECT name, MATCH (name) AGAINST ('Earth') AS m FROM $planets")
    matched = {name: m for name, m in rows}
    assert matched["Earth"] is True
    assert matched["Mars"] is False
    assert len(matched) == 9


def _threshold():
    """The live default threshold — never hardcode it in an agreement assertion.

    MATCH is defined RELATIVE to this value, so a test that hardcodes 0.5 is asserting
    the default rather than the relationship, and silently stops testing the relationship
    the moment the default moves.
    """
    from opteryx.variables import SystemVariables

    return SystemVariables["match_threshold"]


def test_match_agrees_with_cosine_similarity():
    """MATCH must BE `COSINE_SIMILARITY >= threshold` — the split-brain guard.

    Compares row-for-row against the function it is defined in terms of, rather than
    against hardcoded scores, so this keeps holding if the embedder is ever replaced.
    """
    threshold = _threshold()
    rows = _rows(
        """
        SELECT MATCH (name) AGAINST ('Earth')          AS matched,
               COSINE_SIMILARITY(name, 'Earth')        AS score
          FROM $planets
        """
    )
    assert len(rows) == 9
    for matched, score in rows:
        assert matched == (score >= threshold), (matched, score, threshold)


def test_match_honours_the_match_threshold_variable():
    """The threshold is tunable, and tuning it must actually change the answer."""
    high = _rows(
        "SET match_threshold = 0.9;"
        "SELECT name FROM $planets WHERE MATCH (name) AGAINST ('Earth')"
    )
    assert [r[0] for r in high] == ["Earth"]

    # Below Mars' 0.043 lexical score against 'Earth', so Mars joins the result. This is
    # the point: nothing about MATCH is a fixed constant.
    low = _rows(
        "SET match_threshold = 0.04;"
        "SELECT name FROM $planets WHERE MATCH (name) AGAINST ('Earth')"
    )
    assert set(r[0] for r in low) == {"Earth", "Mars"}, low


def test_match_threshold_defaults():
    """A session that never SETs it gets the configured default.

    Read through the variables container rather than `SELECT @@match_threshold`: the
    morsel shim builds a namedtuple from column names, and `@@match_threshold` is not a
    valid Python identifier, so selecting a system variable raises regardless of MATCH.
    """
    from opteryx.variables import SystemVariables

    assert SystemVariables["match_threshold"] == 0.5


def test_match_threshold_rejects_a_non_float():
    """The variable is FLOAT64; an INTEGER is refused rather than silently coerced."""
    with pytest.raises(Exception):
        _rows("SET match_threshold = 1; SELECT 1")


def test_zero_magnitude_operand_does_not_match():
    """Stopword-only/empty text embeds to a zero vector -> NaN similarity.

    `NaN >= threshold` is false, so the row does not match. Intended: an undefined
    direction is not a match. (Asserted through MATCH rather than by reading the NaN,
    because 'does not match' is the contract.)
    """
    rows = _rows("SELECT name FROM $planets WHERE MATCH (name) AGAINST ('')")
    assert rows == []


def test_match_over_a_larger_string_column():
    """Not just $planets — 357 rows of multi-word values, off the virtual-dataset path."""
    rows = _rows(
        "SELECT name FROM testdata.astronauts WHERE MATCH (name) AGAINST ('Neil A. Armstrong')"
    )
    assert [r[0] for r in rows] == ["Neil A. Armstrong"], rows


def test_match_agrees_with_cosine_similarity_over_a_real_column():
    """The split-brain guard again, over 357 rows rather than 9."""
    threshold = _threshold()
    rows = _rows(
        """
        SELECT MATCH (name) AGAINST ('Neil A. Armstrong')       AS matched,
               COSINE_SIMILARITY(name, 'Neil A. Armstrong')     AS score
          FROM testdata.astronauts
        """
    )
    assert len(rows) == 357
    seen_match = seen_miss = False
    for matched, score in rows:
        assert matched == (score >= threshold), (matched, score, threshold)
        seen_match |= matched
        seen_miss |= not matched
    # Guard against the assertion passing vacuously (e.g. every row False): the
    # relationship is only really exercised if both outcomes occur.
    assert seen_match and seen_miss


def test_match_against_a_non_string_column_is_refused():
    from opteryx.exceptions import IncompatibleTypesError

    with pytest.raises(IncompatibleTypesError):
        _rows("SELECT name FROM $planets WHERE MATCH (id) AGAINST ('Earth')")


def test_multi_column_match_is_refused_not_silently_dropped():
    """Pre-existing: only columns[0] was ever read, so `b` vanished silently.

    _MATCH_AGAINST's arity is 2, so a second column cannot reach the kernel at all.
    """
    with pytest.raises(UnsupportedSyntaxError):
        _rows("SELECT name FROM $planets WHERE MATCH (name, id) AGAINST ('Earth')")


@pytest.mark.parametrize("modifier", ["IN BOOLEAN MODE", "IN NATURAL LANGUAGE MODE"])
def test_search_modifiers_are_refused_not_silently_dropped(modifier):
    """MySQL's full-text strategies have no counterpart in cosine similarity.

    They used to parse and be ignored — a silently different query from the one written.
    """
    with pytest.raises(UnsupportedSyntaxError):
        _rows(f"SELECT name FROM $planets WHERE MATCH (name) AGAINST ('Earth' {modifier})")


def test_internal_name_is_not_callable_directly():
    with pytest.raises(UnsupportedSyntaxError):
        _rows("SELECT _MATCH_AGAINST(name, 'Earth') FROM $planets")


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
