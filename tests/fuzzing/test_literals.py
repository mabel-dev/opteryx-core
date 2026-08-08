"""
Fuzzing allows us to test a lot more variations than we would if we were to write
all test cases by hand.

Covers the string-literal path: what the rewriter and parser do to an arbitrary
literal on its way to a kernel. Two of the three checks are real oracles rather
than "did not raise" — a literal that survives the parser must come back out
unchanged, and a value put through BASE64_ENCODE must decode back to itself.

This absorbed `fuzz_literals.py`, an uncollected near-duplicate that fuzzed
BASE64_ENCODE instead of HASH; both functions are covered here, in a file pytest
actually collects.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import hypothesis.strategies as st
import pytest
from hypothesis import given, settings

import opteryx

# allows us to run short CI and longer scheduled tests
TEST_ITERATIONS = int(os.environ.get("TEST_ITERATIONS", 100))

literals = st.text(min_size=1)


def _sanitise(literal: str) -> str:
    """Make `literal` embeddable in a single-quoted SQL string.

    Only the delimiter is substituted. A quote mid-literal ending the literal is
    correct behaviour, not a defect, so there is nothing to test there.

    Nothing else is filtered. Earlier versions of this file also stripped `\\`
    and `--`, and its sibling caught and ignored a null-byte rejection; all three
    now pass, so the filters were suppressing coverage rather than a defect.
    """
    return literal.replace("'", "#")


def _row(statement: str):
    """Execute a single-row query and return that row."""
    session = opteryx.session()
    rows = [morsel[i] for morsel in session.execute_to_morsels(statement) for i in range(len(morsel))]
    assert len(rows) == 1, f"expected one row from {statement!r}, got {len(rows)}"
    return rows[0]


@settings(deadline=None, max_examples=TEST_ITERATIONS)
@given(literal=literals)
def test_fuzz_literal_survives_the_parser(literal):
    """A literal must come back out of the parser byte-for-byte."""
    literal = _sanitise(literal)
    statement = f"SELECT '{literal}' AS value;"
    assert _row(statement)[0] == literal, statement


@settings(deadline=None, max_examples=TEST_ITERATIONS)
@given(literal=literals)
def test_fuzz_literal_base64_round_trip(literal):
    """BASE64_DECODE(BASE64_ENCODE(x)) must be x."""
    literal = _sanitise(literal)
    statement = f"SELECT BASE64_DECODE(BASE64_ENCODE('{literal}')) AS value;"
    assert _row(statement)[0] == literal.encode("utf-8"), statement


@settings(deadline=None, max_examples=TEST_ITERATIONS)
@given(literal=literals)
def test_fuzz_literal_hash(literal):
    """HASH accepts any literal, and is deterministic for a given one."""
    literal = _sanitise(literal)
    statement = f"SELECT HASH('{literal}') AS value;"
    assert _row(statement)[0] == _row(statement)[0], statement


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__])
