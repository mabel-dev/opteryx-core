"""
Fuzzing allows us to test a lot more variations than we would if we were to write
all test cases by hand.

Parameterization is the most likely place to introduce security weaknesses, so this
is one of the initial targets for fuzzing.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import string
import pytest

import hypothesis.strategies as st
from hypothesis import given, settings

from tests.helpers import execute_and_get_arrow

# allows us to run short CI and longer scheduled tests
TEST_ITERATIONS = int(os.environ.get("TEST_ITERATIONS", 100))

names = st.text(alphabet=string.ascii_letters, min_size=1)


@settings(deadline=None, max_examples=TEST_ITERATIONS)
@given(name=names, value=st.text(alphabet=string.printable))
def test_fuzz_variables(name, value):
    # This module was disabled (as `_test_variables.py`, which pytest does not
    # collect) rather than fixed. Two separate defects kept it red; both are now
    # fixed, and nothing here is filtered to keep it green:
    #
    #  * VALUES containing \\ \r \n \t \x0b \x0c or `--` -- the set named in the
    #    since-removed skip list. All pass now.
    #  * NAMES that collide with a keyword -- `@OR`, `@WHERE`, `@SELECT` and the
    #    other 14 single-word SQL_PARTS entries. The rewriter's keyword regex
    #    split the sigil off the name and emitted `@ OR`, so the parser saw a
    #    bare `@`. Fixed by the `(?<![@$])` guard in opteryx/planner/sql_rewriter.
    #
    # Single quote stays substituted: it is the string delimiter, so a quote
    # mid-literal ending the literal is correct behaviour, not a defect.
    value = value.replace("'", "#")
    if len(value) == 0:
        value = "default"

    statement = f"SET @{name} = '{value}'; SELECT @{name};"
    #    print(statement.encode())

    result = execute_and_get_arrow(statement).to_pylist()

    # This is a real round-trip assertion, not a "did not raise": whatever was
    # SET must come back out of the SELECT byte-for-byte.
    assert next(iter(result[0].values())) == value, statement


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__])
