"""A VARBINARY `LIKE` anchored at one end is rewritten to _STARTS_WITH /
_ENDS_WITH (and their case-insensitive twins), under a NOT for the negated forms.

Regression: the VARBINARY branch of the predicate rewriter carried on into the
ANY/IN rewrites after lowering a negated pattern to NOT(function), and read the
comparison operator off the NOT — every `bin NOT LIKE b'x%'` failed to plan with
an AttributeError. The VARCHAR branch has always stopped once the predicate is no
longer a comparison.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx

_NAME = "CAST(name AS VARBINARY)"


@pytest.mark.parametrize(
    "predicate, expected",
    [
        (f"{_NAME} LIKE b'M%'", 2),  # Mercury, Mars
        (f"{_NAME} NOT LIKE b'M%'", 7),
        (f"{_NAME} ILIKE b'm%'", 2),
        (f"{_NAME} NOT ILIKE b'm%'", 7),
        (f"{_NAME} LIKE b'%s'", 3),  # Venus, Mars, Uranus
        (f"{_NAME} NOT LIKE b'%s'", 6),
        (f"{_NAME} NOT ILIKE b'%S'", 6),
    ],
)
def test_anchored_binary_like(predicate, expected):
    session = opteryx.session()
    rows = 0
    for morsel in session.execute_to_morsels(f"SELECT name FROM $planets WHERE {predicate}"):
        rows += morsel.num_rows
    assert rows == expected, predicate


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
