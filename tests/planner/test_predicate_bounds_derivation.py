# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
`predicate_bounds` — SOUNDNESS, by oracle.

Every rule in that module claims: "a row matching this predicate lies inside
this interval". The tests here check that claim the only way it can honestly be
checked — by evaluating the real predicate over a domain of values, and
asserting that EVERY value it matches is admitted by the derived bounds.

The asymmetry is the whole point and the assertions are written to it:

  * A derived interval that is WIDER than the truth costs a file read. Allowed.
  * A derived interval that is NARROWER drops rows. Never allowed, and that is
    what `assert_sound` fails on.

Tightness is asserted separately, and only where a loose answer would make the
rule pointless (an IN list that derives no bound at all, a prefix that derives
the whole key space). A rule that is soundly loose is a missed optimisation; a
rule that is unsoundly tight is a wrong answer, so they are not tested as if
they were the same kind of failure.
"""

import datetime
import os
import random
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.compiled.structures.node import Node
from opteryx.expression import NodeType
from opteryx.planner import build_literal_node
from opteryx.planner.optimizer.predicate_bounds import derive_bound_conjuncts
from opteryx.planner.optimizer.predicate_bounds import derive_case_fold_conjuncts
from opteryx.planner.optimizer.predicate_bounds import derive_null_terms
from opteryx.types.logical_type import DATE
from opteryx.types.logical_type import FLOAT64
from opteryx.types.logical_type import INT64
from opteryx.types.logical_type import TIMESTAMP
from opteryx.types.logical_type import VARCHAR

# ---------------------------------------------------------------------------
# Node builders — the shapes the binder produces, built by hand so a test can
# state one predicate without standing a whole plan up behind it.
# ---------------------------------------------------------------------------


def ident(name="col"):
    return Node(NodeType.IDENTIFIER, source_column=name, value=name)


def lit(value, column_type=None):
    return build_literal_node(value, suggested_type=column_type)


def compare(left, operator, right):
    return Node(NodeType.COMPARISON_OPERATOR, value=operator, left=left, right=right)


def function(name, *parameters):
    return Node(NodeType.FUNCTION, value=name, parameters=list(parameters))


def arithmetic(left, operator, right):
    return Node(NodeType.BINARY_OPERATOR, value=operator, left=left, right=right)


def types_of(**mapping):
    """A `column_type_for` callback over a literal name->ColumnType mapping."""
    return lambda name: mapping.get(name)


# ---------------------------------------------------------------------------
# The oracle
# ---------------------------------------------------------------------------

_ADMITS = {
    "Eq": lambda value, bound: value == bound,
    "NotEq": lambda value, bound: value != bound,
    "Gt": lambda value, bound: value > bound,
    "GtEq": lambda value, bound: value >= bound,
    "Lt": lambda value, bound: value < bound,
    "LtEq": lambda value, bound: value <= bound,
}


def derived_terms(conjunct, column_type_for=None):
    """Just the DERIVED conjuncts — the originals come back first and unchanged,
    which is itself asserted in `test_original_conjuncts_pass_through`."""
    produced = derive_bound_conjuncts([conjunct], column_type_for)
    return [term for term in produced if term is not conjunct]


def admits(terms, value) -> bool:
    """Would the derived bounds let a row holding `value` through?"""
    for term in terms:
        if not _ADMITS[term.value](value, term.right.value):
            return False
    return True


def assert_sound(conjunct, domain, matches, column_type_for=None, expect_terms=True):
    """THE test. Every value the predicate matches must be admitted.

    `matches` is the predicate's real meaning, written independently of the
    module under test — a Python lambda, not a second copy of the derivation.
    """
    terms = derived_terms(conjunct, column_type_for)
    if expect_terms:
        assert terms, "derived no bounds at all"
    for value in domain:
        if matches(value):
            assert admits(terms, value), (
                f"UNSOUND: {value!r} matches the predicate but the derived bounds "
                f"{[(t.value, t.right.value) for t in terms]} exclude it"
            )
    return terms


def assert_excludes_something(terms, domain, matches):
    """The rule is worth having: at least one non-matching value is eliminated.

    Guards against a rule that is sound because it derived `-inf..+inf`.
    """
    assert any(not matches(value) and not admits(terms, value) for value in domain), (
        "derived bounds exclude nothing — sound, but useless"
    )


# ---------------------------------------------------------------------------
# Pass-through and canonical shapes
# ---------------------------------------------------------------------------


def test_original_conjuncts_pass_through_untouched():
    """A caller swapping its splitter for this must not lose anything it had."""
    first = compare(ident("a"), "Gt", lit(5))
    second = compare(ident("b"), "Eq", lit("x"))
    produced = derive_bound_conjuncts([first, second])
    assert produced[0] is first
    assert produced[1] is second


def test_canonical_comparison_is_not_re_derived():
    """`col > 5` is already the shape the pruners read; deriving a copy would
    only cost plan time."""
    assert derived_terms(compare(ident(), "Gt", lit(5))) == []


def test_and_tree_is_split():
    left = compare(ident("a"), "Gt", lit(5))
    right = compare(ident("b"), "Lt", lit(9))
    produced = derive_bound_conjuncts([Node(NodeType.AND, left=left, right=right)])
    assert left in produced and right in produced


# ---------------------------------------------------------------------------
# IN lists
# ---------------------------------------------------------------------------


def test_in_list_derives_the_hull():
    values = [30, 10, 20]
    conjunct = compare(ident(), "InList", lit(values))
    domain = list(range(0, 41))
    terms = assert_sound(conjunct, domain, lambda v: v in values)
    assert_excludes_something(terms, domain, lambda v: v in values)
    assert sorted((t.value, t.right.value) for t in terms) == [("GtEq", 10), ("LtEq", 30)]


def test_single_member_in_list_derives_an_equality():
    """Equality, not a degenerate range: `Eq` is the one operator the min-k
    membership sketch can eliminate a file on outright."""
    terms = derived_terms(compare(ident(), "InList", lit([7])))
    assert [(t.value, t.right.value) for t in terms] == [("Eq", 7)]


def test_string_in_list_derives_the_hull():
    values = ["pear", "apple", "fig"]
    conjunct = compare(ident(), "InList", lit(values))
    domain = ["aardvark", "apple", "fig", "pear", "zebra"]
    terms = assert_sound(conjunct, domain, lambda v: v in values)
    assert sorted((t.value, t.right.value) for t in terms) == [
        ("GtEq", "apple"),
        ("LtEq", "pear"),
    ]


def test_mixed_type_in_list_declines():
    """Ordering 5 against 'apple' is Python's answer, not the engine's."""
    assert derived_terms(compare(ident(), "InList", lit([5, "apple"]))) == []


def test_in_list_with_a_null_declines():
    assert derived_terms(compare(ident(), "InList", lit([5, None]))) == []


def test_empty_in_list_declines():
    assert derived_terms(compare(ident(), "InList", lit([]))) == []


# ---------------------------------------------------------------------------
# LIKE
# ---------------------------------------------------------------------------

_LIKE_DOMAIN = [
    "", "a", "ab", "abc", "abc!", "abcz", "abd", "abz", "ac", "b", "zzz",
    "ABC", "abc def", "abcabc",
]


def _like_matches(pattern):
    """SQL LIKE, as a regex — written from the SQL definition, not from the
    module under test."""
    import re

    expression = "".join(
        ".*" if char == "%" else "." if char == "_" else re.escape(char) for char in pattern
    )
    return lambda value: re.fullmatch(expression, value) is not None


def test_like_prefix_derives_a_range():
    conjunct = compare(ident(), "Like", lit("abc%"))
    matches = _like_matches("abc%")
    terms = assert_sound(conjunct, _LIKE_DOMAIN, matches)
    assert_excludes_something(terms, _LIKE_DOMAIN, matches)
    assert sorted((t.value, t.right.value) for t in terms) == [("GtEq", "abc"), ("Lt", "abd")]


def test_like_underscore_wildcard_derives_the_prefix_before_it():
    conjunct = compare(ident(), "Like", lit("ab_"))
    assert_sound(conjunct, _LIKE_DOMAIN, _like_matches("ab_"))


def test_like_without_a_wildcard_is_an_equality():
    terms = derived_terms(compare(ident(), "Like", lit("abc")))
    assert [(t.value, t.right.value) for t in terms] == [("Eq", "abc")]


def test_leading_wildcard_declines():
    """`%abc` constrains nothing — every value in the domain can match."""
    assert derived_terms(compare(ident(), "Like", lit("%abc"))) == []


def test_backslash_in_a_like_pattern_declines():
    """Whether `\\` escapes the next character decides where the prefix ENDS.
    Guessing wrong lengthens the prefix, which drops rows."""
    assert derived_terms(compare(ident(), "Like", lit("ab\\%c%"))) == []


def test_ilike_is_not_derived():
    """Case-insensitive matching is the LOWER problem: case folding is not
    order-preserving, so no useful interval exists."""
    assert derived_terms(compare(ident(), "ILike", lit("abc%"))) == []


def test_not_like_is_not_derived():
    assert derived_terms(compare(ident(), "NotLike", lit("abc%"))) == []


# ---------------------------------------------------------------------------
# Same-column OR
# ---------------------------------------------------------------------------


def test_same_column_or_derives_the_hull():
    arms = Node(
        NodeType.OR,
        left=compare(ident("a"), "Eq", lit(3)),
        right=compare(ident("a"), "Eq", lit(9)),
    )
    domain = list(range(0, 15))
    matches = lambda v: v in (3, 9)
    terms = assert_sound(arms, domain, matches)
    assert_excludes_something(terms, domain, matches)
    assert sorted((t.value, t.right.value) for t in terms) == [("GtEq", 3), ("LtEq", 9)]


def test_or_over_different_columns_declines():
    """A row satisfying the `b` arm can hold ANY value of `a`."""
    arms = Node(
        NodeType.OR,
        left=compare(ident("a"), "Eq", lit(3)),
        right=compare(ident("b"), "Eq", lit(9)),
    )
    assert derived_terms(arms) == []


def test_or_with_an_unbounded_arm_loses_that_side():
    arms = Node(
        NodeType.OR,
        left=compare(ident("a"), "Gt", lit(100)),
        right=compare(ident("a"), "Eq", lit(3)),
    )
    domain = list(range(0, 200))
    matches = lambda v: v > 100 or v == 3
    terms = assert_sound(arms, domain, matches)
    assert [(t.value, t.right.value) for t in terms] == [("GtEq", 3)]


def test_or_with_an_unevaluable_arm_declines():
    arms = Node(
        NodeType.OR,
        left=compare(ident("a"), "Eq", lit(3)),
        right=compare(ident("a"), "NotEq", lit(9)),
    )
    assert derived_terms(arms) == []


def test_nary_or_derives_the_hull():
    arms = Node(
        NodeType.CNF,
        parameters=[
            compare(ident("a"), "Eq", lit(5)),
            compare(ident("a"), "Eq", lit(1)),
            compare(ident("a"), "Eq", lit(4)),
        ],
    )
    domain = list(range(0, 10))
    assert_sound(arms, domain, lambda v: v in (1, 4, 5))


# ---------------------------------------------------------------------------
# Arithmetic around the column
# ---------------------------------------------------------------------------

_NUMERIC_DOMAIN = [
    value / 4 for value in range(-200, 201)
] + [-10**9, -1000, -1, 0, 1, 1000, 10**9]
_OPERATORS = ("Eq", "Gt", "GtEq", "Lt", "LtEq")


def test_arithmetic_preimage_is_sound_across_operators_and_constants():
    """`col <op> k` for every op, both operand orders, and constants of both
    signs — checked against the arithmetic itself."""
    for operator, evaluate in (
        ("Plus", lambda x, k: x + k),
        ("Minus", lambda x, k: x - k),
        ("Multiply", lambda x, k: x * k),
    ):
        for constant in (-7, -2.5, -1, 1, 2, 2.5, 7, 1000):
            for comparison in _OPERATORS:
                for threshold in (-12, 0, 3.5, 40):
                    conjunct = compare(
                        arithmetic(ident(), operator, lit(constant)), comparison, lit(threshold)
                    )
                    assert_sound(
                        conjunct,
                        _NUMERIC_DOMAIN,
                        lambda value: _ADMITS[comparison](evaluate(value, constant), threshold),
                    )


def test_constant_on_the_left_of_a_subtraction_reverses_the_axis():
    """`k - col` DECREASES in col, so the bound crosses over. Getting this
    backwards would prune exactly the files that hold the answer."""
    conjunct = compare(arithmetic(lit(100), "Minus", ident()), "Gt", lit(40))
    domain = list(range(-50, 200))
    terms = assert_sound(conjunct, domain, lambda value: 100 - value > 40)
    assert_excludes_something(terms, domain, lambda value: 100 - value > 40)
    assert [(t.value, t.right.value) for t in terms] == [("Lt", 60)]


def test_literal_on_the_left_of_the_comparison_flips_the_operator():
    conjunct = compare(lit(40), "Lt", arithmetic(ident(), "Plus", lit(10)))
    assert_sound(conjunct, _NUMERIC_DOMAIN, lambda value: 40 < value + 10)


def test_multiply_by_zero_declines():
    """`col * 0` is constant — it confines col to nothing at all."""
    assert derived_terms(compare(arithmetic(ident(), "Multiply", lit(0)), "Eq", lit(0))) == []


def test_division_declines():
    """Integer division and true division have different pre-images, and which
    `Divide` means is not settled here."""
    assert derived_terms(compare(arithmetic(ident(), "Divide", lit(10)), "Eq", lit(5))) == []


def test_modulo_declines():
    assert derived_terms(compare(arithmetic(ident(), "Modulo", lit(10)), "Eq", lit(5))) == []


def test_huge_integer_arithmetic_stays_exact():
    """Float division of large integers can land on the wrong side of the exact
    quotient. Integer operands must go through integer arithmetic."""
    big = 10**18 + 1
    conjunct = compare(arithmetic(ident(), "Multiply", lit(3)), "GtEq", lit(big))
    domain = [big // 3 - 2, big // 3 - 1, big // 3, big // 3 + 1, big // 3 + 2]
    assert_sound(conjunct, domain, lambda value: value * 3 >= big)


# ---------------------------------------------------------------------------
# FLOOR / CEILING / ROUND / TRUNC
# ---------------------------------------------------------------------------

_STEP_DOMAIN = [value / 8 for value in range(-160, 161)]


def test_floor_preimage_is_exact_and_sound():
    import math

    for comparison in _OPERATORS:
        for threshold in (-3, -2.5, 0, 2, 2.5, 7):
            conjunct = compare(function("FLOOR", ident(), lit(0)), comparison, lit(threshold))
            terms = assert_sound(
                conjunct,
                _STEP_DOMAIN,
                lambda value: _ADMITS[comparison](math.floor(value), threshold),
            )
            assert_excludes_something(
                terms,
                _STEP_DOMAIN,
                lambda value: _ADMITS[comparison](math.floor(value), threshold),
            )


def test_ceiling_preimage_is_exact_and_sound():
    import math

    for comparison in _OPERATORS:
        for threshold in (-3, -2.5, 0, 2, 2.5, 7):
            conjunct = compare(function("CEILING", ident(), lit(0)), comparison, lit(threshold))
            terms = assert_sound(
                conjunct,
                _STEP_DOMAIN,
                lambda value: _ADMITS[comparison](math.ceil(value), threshold),
            )
            assert_excludes_something(
                terms,
                _STEP_DOMAIN,
                lambda value: _ADMITS[comparison](math.ceil(value), threshold),
            )


def test_round_is_sound_under_both_rounding_conventions():
    """The module widens by a whole unit precisely so it does not have to know
    whether the kernel rounds half-even or half-away-from-zero. Both are
    asserted, so a change of convention cannot make the bound unsound."""
    import math

    def half_away(value):
        return math.floor(value + 0.5) if value >= 0 else math.ceil(value - 0.5)

    for rounder in (lambda v: round(v), half_away):
        for comparison in _OPERATORS:
            for threshold in (-3, 0, 2, 7):
                conjunct = compare(function("ROUND", ident()), comparison, lit(threshold))
                assert_sound(
                    conjunct,
                    _STEP_DOMAIN,
                    lambda value: _ADMITS[comparison](rounder(value), threshold),
                )


def test_numeric_trunc_is_sound_at_the_sign_change():
    import math

    for comparison in _OPERATORS:
        for threshold in (-3, -1, 0, 1, 7):
            conjunct = compare(function("TRUNC", ident()), comparison, lit(threshold))
            assert_sound(
                conjunct,
                _STEP_DOMAIN,
                lambda value: _ADMITS[comparison](math.trunc(value), threshold),
            )


def test_temporal_trunc_is_left_to_the_existing_plan_rewrite():
    """`TRUNC(ts, 'month')` is already turned into an EXACT range on the raw
    column by `rewrite_date_trunc_to_range`, in the plan. A second, looser bound
    derived here would be a second dialect for one function."""
    conjunct = compare(function("TRUNC", ident("ts"), lit("month")), "Eq", lit(0))
    assert derived_terms(conjunct, types_of(ts=TIMESTAMP())) == []


def test_negative_scale_declines():
    """ROUND(col, -2) moves a value by up to 50, which the ±1 widening does not
    cover."""
    assert derived_terms(compare(function("ROUND", ident(), lit(-2)), "Eq", lit(300))) == []


def test_nonzero_floor_scale_declines():
    assert derived_terms(compare(function("FLOOR", ident(), lit(2)), "Eq", lit(1.25))) == []


# ---------------------------------------------------------------------------
# ABS / SIGN
# ---------------------------------------------------------------------------


def test_abs_upper_bound_derives_a_symmetric_interval():
    for comparison in ("Eq", "Lt", "LtEq"):
        for threshold in (0, 3, 7.5):
            conjunct = compare(function("ABS", ident()), comparison, lit(threshold))
            terms = assert_sound(
                conjunct,
                _NUMERIC_DOMAIN,
                lambda value: _ADMITS[comparison](abs(value), threshold),
            )
            assert_excludes_something(
                terms,
                _NUMERIC_DOMAIN,
                lambda value: _ADMITS[comparison](abs(value), threshold),
            )


def test_abs_lower_bound_only_declines():
    """`ABS(col) > 5` excludes a hole in the MIDDLE of the axis — the complement
    of an interval, which the bounds pruners cannot express."""
    assert derived_terms(compare(function("ABS", ident()), "Gt", lit(5))) == []
    assert derived_terms(compare(function("ABS", ident()), "GtEq", lit(5))) == []


def test_sign_preimage_is_sound_for_every_target():
    def sign(value):
        return (value > 0) - (value < 0)

    for comparison in _OPERATORS:
        for threshold in (-1, 0, 1):
            conjunct = compare(function("SIGN", ident()), comparison, lit(threshold))
            assert_sound(
                conjunct,
                _NUMERIC_DOMAIN,
                lambda value: _ADMITS[comparison](sign(value), threshold),
                expect_terms=False,
            )


def test_sign_equals_one_means_strictly_positive():
    terms = derived_terms(compare(function("SIGN", ident()), "Eq", lit(1)))
    assert [(t.value, t.right.value) for t in terms] == [("Gt", 0)]


# ---------------------------------------------------------------------------
# String prefixes — LEFT / SUBSTRING
# ---------------------------------------------------------------------------


def _ascii_corpus(count=400, seed=20260914):
    generator = random.Random(seed)
    alphabet = "ab~ !0Az{|}"
    corpus = ["", "a", "ab", "abc"]
    for _ in range(count):
        length = generator.randint(0, 6)
        corpus.append("".join(generator.choice(alphabet) for _ in range(length)))
    return sorted(set(corpus))


_STRING_DOMAIN = _ascii_corpus()


def test_left_prefix_is_sound_for_every_operator_and_width():
    for width in (1, 2, 3, 5):
        for comparison in _OPERATORS:
            for threshold in ("a", "ab", "abc", "b", "~", "{"):
                conjunct = compare(
                    function("LEFT", ident(), lit(width)), comparison, lit(threshold)
                )
                assert_sound(
                    conjunct,
                    _STRING_DOMAIN,
                    lambda value: _ADMITS[comparison](value[:width], threshold),
                    expect_terms=False,
                )


def test_left_equality_derives_the_prefix_range():
    conjunct = compare(function("LEFT", ident(), lit(3)), "Eq", lit("abc"))
    matches = lambda value: value[:3] == "abc"
    terms = assert_sound(conjunct, _STRING_DOMAIN, matches)
    assert_excludes_something(terms, _STRING_DOMAIN, matches)
    assert sorted((t.value, t.right.value) for t in terms) == [("GtEq", "abc"), ("Lt", "abd")]


def test_left_upper_bound_truncates_the_bound_first():
    """With width 2 and bound 'abc', no 2-character prefix sits between 'ab' and
    'abc', so the real constraint is `x[:2] <= 'ab'` and the bound is 'ac'. Using
    successor('abc') would have been sound but looser."""
    conjunct = compare(function("LEFT", ident(), lit(2)), "LtEq", lit("abc"))
    terms = assert_sound(conjunct, _STRING_DOMAIN, lambda value: value[:2] <= "abc")
    assert [(t.value, t.right.value) for t in terms] == [("Lt", "ac")]


def test_substring_from_one_behaves_as_left():
    conjunct = compare(function("SUBSTRING", ident(), lit(1), lit(3)), "Eq", lit("abc"))
    assert_sound(conjunct, _STRING_DOMAIN, lambda value: value[:3] == "abc")


def test_substring_from_any_other_offset_declines():
    """A substring from offset 2 has no order relationship with the whole
    string: 'zb' and 'ab' share it and sort at opposite ends."""
    assert derived_terms(compare(function("SUBSTRING", ident(), lit(2), lit(3)), "Eq", lit("bc"))) == []


def test_substring_without_a_length_is_the_identity():
    conjunct = compare(function("SUBSTRING", ident(), lit(1)), "GtEq", lit("abc"))
    terms = assert_sound(conjunct, _STRING_DOMAIN, lambda value: value >= "abc")
    assert [(t.value, t.right.value) for t in terms] == [("GtEq", "abc")]


def test_non_ascii_bound_emits_no_upper_bound_but_stays_sound():
    """Incrementing the last byte of arbitrary UTF-8 can leave the encoding
    invalid, so the successor step is ASCII-only. The lower bound still passes
    through — half the evidence, none of the risk."""
    conjunct = compare(function("LEFT", ident(), lit(3)), "GtEq", lit("é"))
    terms = derived_terms(conjunct)
    assert [(t.value, t.right.value) for t in terms] == [("GtEq", "é")]


def test_prefix_of_all_high_bytes_emits_no_upper_bound():
    conjunct = compare(function("LEFT", ident(), lit(1)), "LtEq", lit("\x7f"))
    assert derived_terms(conjunct) == []


# ---------------------------------------------------------------------------
# Temporal
# ---------------------------------------------------------------------------

_EPOCH = datetime.datetime(1970, 1, 1)
_DAY_DOMAIN = list(range(-3000, 22000, 37))
_MICROS_DOMAIN = [day * 86_400 * 10**6 + offset for day in range(-40, 22000, 311) for offset in (0, 3_600_000_000)]


def test_extract_year_from_a_date_column_is_sound():
    for comparison in _OPERATORS:
        for year in (1965, 1970, 1999, 2024, 2030):
            conjunct = compare(
                function("EXTRACT", lit("year"), ident("d")), comparison, lit(year)
            )
            assert_sound(
                conjunct,
                _DAY_DOMAIN,
                lambda days: _ADMITS[comparison](
                    (_EPOCH + datetime.timedelta(days=days)).year, year
                ),
                column_type_for=types_of(d=DATE),
            )


def test_extract_year_equality_derives_exactly_that_year():
    conjunct = compare(function("EXTRACT", lit("year"), ident("d")), "Eq", lit(2024))
    terms = derived_terms(conjunct, types_of(d=DATE))
    start = (datetime.datetime(2024, 1, 1) - _EPOCH).days
    end = (datetime.datetime(2025, 1, 1) - _EPOCH).days
    assert sorted((t.value, t.right.value) for t in terms) == [("GtEq", start), ("Lt", end)]


def test_extract_year_from_a_timestamp_column_is_sound():
    for comparison in _OPERATORS:
        for year in (1970, 2024):
            conjunct = compare(
                function("EXTRACT", lit("year"), ident("ts")), comparison, lit(year)
            )
            assert_sound(
                conjunct,
                _MICROS_DOMAIN,
                lambda micros: _ADMITS[comparison](
                    (_EPOCH + datetime.timedelta(microseconds=micros)).year, year
                ),
                column_type_for=types_of(ts=TIMESTAMP()),
            )


def test_derived_temporal_literal_carries_the_column_type():
    """A pushed temporal literal is a PHYSICAL INT; consumers render and compare
    it from the type tag. An untagged one reads as a plain integer against
    temporal bounds, which is the class of bug `_temporal_domain_mismatch`
    exists to catch."""
    conjunct = compare(function("EXTRACT", lit("year"), ident("ts")), "Eq", lit(2024))
    for term in derived_terms(conjunct, types_of(ts=TIMESTAMP())):
        assert term.right.type == TIMESTAMP()
        assert isinstance(term.right.value, int)


def test_cyclic_extract_parts_decline():
    """MONTH/DAY/HOUR are cyclic: the pre-image of a month is a union of one
    interval per year, which is not an interval."""
    for part in ("month", "day", "hour", "minute", "dow", "doy"):
        conjunct = compare(function("EXTRACT", lit(part), ident("ts")), "Eq", lit(3))
        assert derived_terms(conjunct, types_of(ts=TIMESTAMP())) == [], part


def test_extract_year_on_a_non_temporal_column_declines():
    conjunct = compare(function("EXTRACT", lit("year"), ident("n")), "Eq", lit(2024))
    assert derived_terms(conjunct, types_of(n=INT64)) == []


def test_unixtime_is_sound_under_floor_and_truncation():
    import math

    for to_seconds in (lambda m: m // 10**6, lambda m: math.trunc(m / 10**6)):
        for comparison in _OPERATORS:
            for seconds in (0, 1_700_000_000, -86_400):
                conjunct = compare(function("UNIXTIME", ident("ts")), comparison, lit(seconds))
                assert_sound(
                    conjunct,
                    _MICROS_DOMAIN,
                    lambda micros: _ADMITS[comparison](to_seconds(micros), seconds),
                    column_type_for=types_of(ts=TIMESTAMP()),
                )


def test_unixtime_equality_still_excludes_almost_everything():
    conjunct = compare(function("UNIXTIME", ident("ts")), "Eq", lit(1_700_000_000))
    terms = derived_terms(conjunct, types_of(ts=TIMESTAMP()))
    assert_excludes_something(
        terms, _MICROS_DOMAIN, lambda micros: micros // 10**6 == 1_700_000_000
    )


def test_from_unixtime_inverts_onto_the_seconds_column():
    """The comparison is against a TIMESTAMP[US] literal — raw microseconds —
    while the column holds whole seconds."""
    for comparison in _OPERATORS:
        for micros in (0, 1_700_000_000_000_000, -86_400_000_000):
            conjunct = compare(
                function("FROM_UNIXTIME", ident("epoch")), comparison, lit(micros)
            )
            assert_sound(
                conjunct,
                list(range(-100_000, 1_800_000_000, 7_000_011)),
                lambda seconds: _ADMITS[comparison](seconds * 10**6, micros),
                column_type_for=types_of(epoch=INT64),
                expect_terms=False,
            )


def test_time_bucket_is_sound_for_any_bucket_ORIGIN():
    """The rule uses only "the label never exceeds the value it labels" and "the
    value is under one width past its label", so it must hold wherever the
    buckets are anchored. Three different anchors are asserted."""
    width = 86_400 * 10**6
    for anchor in (0, 13 * 3_600 * 10**6, -5 * 86_400 * 10**6):
        def bucket(micros, anchor=anchor):
            return anchor + ((micros - anchor) // width) * width

        for comparison in _OPERATORS:
            for target in (0, 1_700_000_000_000_000):
                conjunct = compare(
                    function("TIME_BUCKET", lit(1), lit("day"), ident("ts")),
                    comparison,
                    lit(target),
                )
                assert_sound(
                    conjunct,
                    _MICROS_DOMAIN,
                    lambda micros: _ADMITS[comparison](bucket(micros), target),
                    column_type_for=types_of(ts=TIMESTAMP()),
                    expect_terms=False,
                )


def test_time_bucket_with_an_unknown_unit_declines():
    conjunct = compare(
        function("TIME_BUCKET", lit(1), lit("fortnight"), ident("ts")), "Eq", lit(0)
    )
    assert derived_terms(conjunct, types_of(ts=TIMESTAMP())) == []


# ---------------------------------------------------------------------------
# Composition, refusals, null terms
# ---------------------------------------------------------------------------


def test_transforms_compose():
    """One recursion, not a rule per combination."""
    import math

    conjunct = compare(
        function("FLOOR", arithmetic(ident(), "Multiply", lit(2)), lit(0)), "Eq", lit(7)
    )
    matches = lambda value: math.floor(value * 2) == 7
    terms = assert_sound(conjunct, _STEP_DOMAIN, matches)
    assert_excludes_something(terms, _STEP_DOMAIN, matches)
    bounds = sorted((t.value, t.right.value) for t in terms)
    # The lower bound is nudged one ulp DOWN of the exact 3.5 — every derived
    # quotient widens, and 3.5 itself is a value the predicate matches.
    assert [t[0] for t in bounds] == ["GtEq", "Lt"]
    assert 3.4999999 < bounds[0][1] <= 3.5
    assert bounds[1][1] == 4


def test_an_inverted_interval_is_declined_not_emitted():
    """A lower above the upper says the predicate is unsatisfiable — which may be
    true, but is also exactly what a derivation BUG looks like. Proving a
    predicate false is constant folding's job; a bounds bug drops rows."""
    conjunct = Node(
        NodeType.BETWEEN,
        value=(True, True),
        left=arithmetic(ident(), "Plus", lit(1)),
        right=lit(100),
        centre=lit(1),
    )
    assert derived_terms(conjunct) == []


def test_between_over_a_transform_derives_both_ends():
    conjunct = Node(
        NodeType.BETWEEN,
        value=(True, True),
        left=arithmetic(ident(), "Plus", lit(10)),
        right=lit(20),
        centre=lit(30),
    )
    terms = assert_sound(conjunct, _NUMERIC_DOMAIN, lambda v: 20 <= v + 10 <= 30)
    assert sorted((t.value, t.right.value) for t in terms) == [("GtEq", 10), ("LtEq", 20)]


def test_unknown_functions_decline():
    for name in ("MD5", "SOUNDEX", "REVERSE", "LOWER", "UPPER", "LENGTH", "INITCAP", "TRIM"):
        assert derived_terms(compare(function(name, ident()), "Eq", lit("x"))) == [], name


def test_null_terms_are_extracted():
    is_null = Node(NodeType.UNARY_OPERATOR, value="IsNull", centre=ident("a"))
    is_not_null = Node(NodeType.UNARY_OPERATOR, value="IsNotNull", centre=ident("b"))
    assert derive_null_terms([is_null, is_not_null]) == [("a", True), ("b", False)]


def test_null_terms_ignore_expressions():
    """`f(col) IS NULL` is not a statement about col's null count — a strict
    function is null for a null input, but so is a failing cast."""
    wrapped = Node(
        NodeType.UNARY_OPERATOR, value="IsNull", centre=function("ABS", ident("a"))
    )
    assert derive_null_terms([wrapped]) == []


def test_null_terms_are_found_inside_a_conjunction():
    is_null = Node(NodeType.UNARY_OPERATOR, value="IsNull", centre=ident("a"))
    other = compare(ident("b"), "Gt", lit(1))
    assert derive_null_terms([Node(NodeType.AND, left=is_null, right=other)]) == [("a", True)]


def test_no_predicates_derives_nothing():
    assert derive_bound_conjuncts([]) == []
    assert derive_bound_conjuncts(None) == []
    assert derive_null_terms(None) == []


# ---------------------------------------------------------------------------
# Case folding — a SEPARATE channel, because these bounds are conditional
# ---------------------------------------------------------------------------


def test_case_fold_terms_never_leak_into_the_ordinary_conjuncts():
    """THE containment rule. These bounds are valid only where the fold is the
    identity; one reaching `derive_bound_conjuncts` would be applied to every
    file, and a file holding 'CAA' would be dropped from `LOWER(col) = 'caa'`."""
    conjunct = compare(function("LOWER", ident("label")), "Eq", lit("caa"))
    assert derived_terms(conjunct) == []


def test_lower_equality_derives_a_conditional_point_bound():
    conjunct = compare(function("LOWER", ident("label")), "Eq", lit("caa"))
    derived = derive_case_fold_conjuncts([conjunct])
    assert len(derived) == 1
    column, fold, conjuncts = derived[0]
    assert (column, fold) == ("label", "LOWER")
    assert [(t.value, t.right.value) for t in conjuncts] == [("Eq", "caa")]


def test_upper_equality_derives_against_the_other_fold():
    conjunct = compare(function("UPPER", ident("label")), "Eq", lit("CAA"))
    column, fold, conjuncts = derive_case_fold_conjuncts([conjunct])[0]
    assert (column, fold) == ("label", "UPPER")
    assert [(t.value, t.right.value) for t in conjuncts] == [("Eq", "CAA")]


def test_case_fold_covers_every_comparison_operator():
    """Under identity the fold vanishes entirely, so ranges work as well as
    equality — not just the `=` case."""
    conjunct = compare(function("LOWER", ident("label")), "GtEq", lit("c"))
    _, _, conjuncts = derive_case_fold_conjuncts([conjunct])[0]
    assert [(t.value, t.right.value) for t in conjuncts] == [("GtEq", "c")]


def test_ci_starts_with_folds_the_pattern_before_deriving():
    """The ILIKE lowering does NOT fold the pattern. Under identity the column
    holds no uppercase, so only the folded pattern can match."""
    conjunct = function("_CI_STARTS_WITH", ident("label"), lit(b"Ca"))
    column, fold, conjuncts = derive_case_fold_conjuncts([conjunct])[0]
    assert (column, fold) == ("label", "LOWER")
    assert sorted((t.value, t.right.value) for t in conjuncts) == [
        ("GtEq", b"ca"),
        ("Lt", b"cb"),
    ]


def test_ilike_derives_the_folded_prefix():
    conjunct = compare(ident("label"), "ILike", lit("Ca%"))
    column, fold, conjuncts = derive_case_fold_conjuncts([conjunct])[0]
    assert (column, fold) == ("label", "LOWER")
    assert sorted((t.value, t.right.value) for t in conjuncts) == [("GtEq", "ca"), ("Lt", "cb")]


def test_non_ascii_case_fold_declines():
    """Outside ASCII the two folds disagree, and a bound that depends on which
    one ran is a wrong answer on the type it guessed wrong."""
    assert derive_case_fold_conjuncts([compare(ident("label"), "ILike", lit("Café%"))]) == []


def test_case_fold_of_a_non_identifier_declines():
    conjunct = compare(function("LOWER", function("TRIM", ident("label"))), "Eq", lit("caa"))
    assert derive_case_fold_conjuncts([conjunct]) == []


def test_ordinary_predicates_contribute_no_case_fold_terms():
    assert derive_case_fold_conjuncts([compare(ident("a"), "Eq", lit(5))]) == []
    assert derive_case_fold_conjuncts([function("_STARTS_WITH", ident("a"), lit(b"ab"))]) == []


# ---------------------------------------------------------------------------
# The second consumer — row-group zone maps
# ---------------------------------------------------------------------------


def _ordinal_manifest():
    """A bounds-are-ordinal Manifest over one INT64 column and no files — enough
    to exercise `ordinal_zone_map_terms`, which reads only types and predicates."""
    from opteryx.models.manifest import Manifest

    column = type("_Column", (), {"name": "seq", "column_type": INT64})()
    schema = type("_Schema", (), {"columns": [column]})()
    manifest = Manifest.__new__(Manifest)
    Manifest.__init__(manifest, files=[], schema=schema, bounds_are_ordinal=True)
    return manifest


def test_row_group_zone_terms_gain_the_same_shapes_as_file_pruning():
    """Both pruners read the derivation, so a shape added for files reaches row
    groups too. A second derivation for the finer grain would be the second
    dialect `bounds_are_ordinal` exists to prevent."""
    from opteryx.models.manifest import Manifest

    manifest = _ordinal_manifest()
    gt_eq, lt_eq = Manifest.ZONE_OP_GTEQ, Manifest.ZONE_OP_LTEQ

    in_list = compare(ident("seq"), "InList", lit([12, 31]))
    assert manifest.ordinal_zone_map_terms([in_list]) == [
        ("seq", gt_eq, 12),
        ("seq", lt_eq, 31),
    ]

    scaled = compare(arithmetic(ident("seq"), "Multiply", lit(2)), "GtEq", lit(42))
    assert manifest.ordinal_zone_map_terms([scaled]) == [("seq", gt_eq, 21)]

    disjunction = Node(
        NodeType.OR,
        left=compare(ident("seq"), "Eq", lit(3)),
        right=compare(ident("seq"), "Eq", lit(9)),
    )
    assert manifest.ordinal_zone_map_terms([disjunction]) == [
        ("seq", gt_eq, 3),
        ("seq", lt_eq, 9),
    ]
