# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression tests for the two LIKE-set fusions in PredicateRewriter.

  OR'd  LIKE     -> AnyOpLike / AnyOpILike        (rewrite_ored_like_to_any)
  AND'd NOT LIKE -> AllOpNotLike / AllOpNotILike  (rewrite_anded_not_like_to_all)

Both exist for the same reason: `draken_like_any` buckets the pattern set and
stays O(1) in pattern count, where N separate terms are N passes over the column.

Two properties are pinned here.

1. The fusion fires over an EXTRACTION operand (`dict->>'string'`), not just a
   plain column. Grouping is keyed on the bound schema_column identity, which
   repeated occurrences of one extraction already share; the operand node type
   was the only thing rejecting them. `$planets` covers the plain-column arm.

2. The fused node returns what the unfused terms returned. `$planets` has nine
   known rows, so the expectations here are written out rather than derived from
   a second query that might fuse the same way and compare a rewrite to itself.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "..", "..", "..", ".."))

import pytest

import opteryx
from opteryx.expression.formatter import format_expression
from opteryx.managers.execution import compiler as _compiler

STRUCTS = "testdata.flat.struct"   # one VARBINARY JSON column, `dict`
EXTRACT = "dict->>'string'"
# The fixture is six rows; `dict.string` is present ('string') on the first two
# and ABSENT on the other four, so the extraction is NULL there. That makes this
# table a null-bearing operand for free, which is the case a fused matcher could
# most easily get wrong: SQL three-valued logic drops a NULL subject from both
# `LIKE ANY` and the AND-chain of NOT LIKEs, and the counts below pin that.
_ROWS_WITH_STRING = 2


def _compiled_filters(sql: str) -> list:
    """The filter predicates as the compiler receives them — i.e. after every
    optimizer strategy has run, which is the only place the fused node is
    observable as an operator rather than as an OR/AND tree."""
    captured = []
    original = _compiler._Compiler._lower_expression

    def capture(self, expr, what):
        if what == "a filter predicate":
            captured.append(format_expression(expr))
        return original(self, expr, what)

    _compiler._Compiler._lower_expression = capture
    try:
        session = opteryx.session()
        for _ in session.execute_to_morsels(sql):
            pass
    finally:
        _compiler._Compiler._lower_expression = original
    return captured


def _names(sql: str) -> set:
    session = opteryx.session()
    found = set()
    for morsel in session.execute_to_morsels(sql):
        found.update(v.decode() if isinstance(v, bytes) else v for v in morsel.column("name"))
    return found


# --- OR'd LIKE -> LIKE ANY -------------------------------------------------


@pytest.mark.parametrize(
    "operand, table",
    [("name", "$planets"), (EXTRACT, STRUCTS)],
)
def test_ored_likes_fuse_to_one_any_node(operand, table):
    predicate = " OR ".join(f"{operand} LIKE '{p}%'" for p in ("M", "V", "E"))
    filters = _compiled_filters(f"SELECT * FROM {table} WHERE {predicate}")
    assert len(filters) == 1, filters
    assert "ANYOPLIKE" in filters[0], filters[0]
    # one fused node, and no surviving LIKE term beside it
    assert filters[0].count("ANYOPLIKE") == 1, filters[0]
    assert " LIKE " not in filters[0], filters[0]


def _rows(sql: str) -> int:
    session = opteryx.session()
    return sum(m.num_rows for m in session.execute_to_morsels(sql))


def test_ored_likes_over_an_extraction_keep_their_answer():
    sql = f"SELECT * FROM {STRUCTS} WHERE {EXTRACT} LIKE 's%' OR {EXTRACT} LIKE 'z%'"
    assert "ANYOPLIKE" in _compiled_filters(sql)[0]
    # 's%' matches the two rows that HAVE the key; the four NULL rows match neither
    # pattern and are not rescued by the fusion.
    assert _rows(sql) == _ROWS_WITH_STRING


def test_ored_likes_keep_their_answer():
    assert _names(
        "SELECT name FROM $planets WHERE name LIKE 'M%' OR name LIKE 'V%' OR name LIKE 'E%'"
    ) == {"Mercury", "Mars", "Venus", "Earth"}


# --- AND'd NOT LIKE -> negated LIKE ANY ------------------------------------


@pytest.mark.parametrize(
    "operand, table",
    [("name", "$planets"), (EXTRACT, STRUCTS)],
)
def test_anded_not_likes_fuse_to_one_any_node(operand, table):
    predicate = " AND ".join(f"{operand} NOT LIKE '{p}%'" for p in ("M", "V", "E"))
    filters = _compiled_filters(f"SELECT * FROM {table} WHERE {predicate}")
    # the conjuncts collapse into ONE filter node, not three
    assert len(filters) == 1, filters
    assert filters[0].count("ALLOPNOTLIKE") == 1, filters[0]
    assert "NOT LIKE" not in filters[0], filters[0]


def test_anded_not_likes_keep_their_answer():
    assert _names(
        "SELECT name FROM $planets "
        "WHERE name NOT LIKE 'M%' AND name NOT LIKE 'V%' AND name NOT LIKE 'E%'"
    ) == {"Jupiter", "Saturn", "Uranus", "Neptune", "Pluto"}


def test_anded_not_likes_over_an_extraction_keep_their_answer():
    # 'string' matches neither pattern, so both non-null rows survive; the four
    # NULL rows evaluate to NULL, not TRUE, and must NOT be admitted.
    assert _rows(
        f"SELECT * FROM {STRUCTS} WHERE {EXTRACT} NOT LIKE 'q%' AND {EXTRACT} NOT LIKE 'z%'"
    ) == _ROWS_WITH_STRING

    # 's%' excludes the only rows that could have qualified
    assert _rows(
        f"SELECT * FROM {STRUCTS} WHERE {EXTRACT} NOT LIKE 's%' AND {EXTRACT} NOT LIKE 'z%'"
    ) == 0


def test_single_not_like_is_not_fused():
    filters = _compiled_filters("SELECT * FROM $planets WHERE name NOT LIKE 'M%'")
    assert "ALLOPNOTLIKE" not in "".join(filters), filters


def test_not_likes_on_different_operands_do_not_share_a_node():
    """Grouping is per operand identity — two columns must yield two nodes, never
    one node holding both columns' patterns."""
    filters = _compiled_filters(
        "SELECT p1.name FROM $planets p1 INNER JOIN $planets p2 ON p1.id = p2.id "
        "WHERE p1.name NOT LIKE 'M%' AND p1.name NOT LIKE 'V%' "
        "AND p2.name NOT LIKE 'E%' AND p2.name NOT LIKE 'J%'"
    )
    joined = " ".join(filters)
    assert joined.count("ALLOPNOTLIKE") == 2, filters


def test_case_sensitivity_is_not_mixed_into_one_node():
    """A NOT LIKE and a NOT ILIKE on one operand must not land in the same
    matcher — the blob carries a single case-folding flag."""
    filters = _compiled_filters(
        "SELECT * FROM $planets "
        "WHERE name NOT LIKE 'M%' AND name NOT LIKE 'V%' "
        "AND name NOT ILIKE 'e%' AND name NOT ILIKE 'j%'"
    )
    joined = " ".join(filters)
    assert "ALLOPNOTLIKE" in joined, filters
    assert "ALLOPNOTILIKE" in joined, filters


def test_not_ilike_fusion_keeps_its_answer():
    assert _names(
        "SELECT name FROM $planets WHERE name NOT ILIKE 'm%' AND name NOT ILIKE 'v%'"
    ) == {"Earth", "Jupiter", "Saturn", "Uranus", "Neptune", "Pluto"}


def test_positive_and_negative_terms_are_not_conflated():
    """`x NOT LIKE a AND x LIKE b` has one term of each kind: neither fusion has
    two members, so nothing should fuse."""
    filters = _compiled_filters(
        "SELECT * FROM $planets WHERE name NOT LIKE 'M%' AND name LIKE '%s'"
    )
    joined = " ".join(filters)
    assert "ALLOPNOTLIKE" not in joined, filters
    assert "ANYOPLIKE" not in joined, filters
    assert _names(
        "SELECT name FROM $planets WHERE name NOT LIKE 'M%' AND name LIKE '%s'"
    ) == {"Venus", "Uranus"}


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
