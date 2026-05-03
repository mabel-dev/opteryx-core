"""
Fuzzing allows us to test a lot more variations than we would if we were to write
all test cases by hand.

We're testing string functions (in this particular case, BASE64_ENCODE) can accept fuzzed
inputs, BASE64_ENCODE was chosen because it's a single parameter function.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import argparse

import hypothesis.strategies as st
from hypothesis import given, settings

from tests.helpers import execute_and_get_morsels

# allows us to run short CI and longer scheduled tests
TEST_ITERATIONS = int(os.environ.get("TEST_ITERATIONS", 100))

literals = st.text(min_size=1)


def _test_fuzz_literals_impl(literal):
    # single quote is the delimiter, it's not a bug that we think a delimeter
    # mid string indicates the end of the string
    literal = literal.replace("'", "#")

    # null bytes are not supported in SQL strings
    literal = literal.replace("\x00", "")

    statement = f"SELECT BASE64_ENCODE('{literal}') as base64;"

    try:
        for _ in execute_and_get_morsels(statement):
            pass
    except Exception as e:
        # Null bytes are rejected at SQL rewriter stage with clear error
        if "null bytes" in str(e).lower():
            return
        raise


@settings(deadline=None, max_examples=TEST_ITERATIONS)
@given(literal=literals)
def test_fuzz_literals(literal):
    _test_fuzz_literals_impl(literal)


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description="Fuzz test string literals")
    parser.add_argument(
        "--iterations", type=int, default=TEST_ITERATIONS, help="Number of test iterations"
    )
    args = parser.parse_args()

    @settings(deadline=None, max_examples=args.iterations)
    @given(literal=literals)
    def run_fuzz(literal):
        _test_fuzz_literals_impl(literal)

    run_fuzz()
    print("✅ okay")
