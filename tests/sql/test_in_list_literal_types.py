"""
IN lists and the OR-of-equalities rewrites respect literal types.

* `id = 0.0 OR id = 3` crashed with a raw TypeError ("Literal of type ARRAY<FLOAT64>
  holds a tuple"): three rewrites (OR->IN, CNF->IN, disjunctive domain pushdown) fused
  the two point tests into one IN list mixing an INT64 and a FLOAT64 literal — the very
  list the front door refuses (ArrayWithMixedTypesError). Point tests now fuse only when
  their literals share a type.
* `int_col IN (3.0, 4.5)` was refused ("outside the c-native kernel set"): the IN-list
  lowering had no arm for float literals against an integer column. They now match the
  way `int_col = 3.0` does (compared as doubles): an integral float is that int, a
  non-integral, non-finite or beyond-INT64 one can never match. Only the band between
  2**53 and 2**63, where double and integer equality disagree, still declines.
Expected counts checked against DuckDB on $planets (ids 1..9).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _count(predicate):
    morsels = list(opteryx.session().execute_to_morsels(f"SELECT COUNT(*) FROM $planets WHERE {predicate}"))
    return morsels[0][0][0]


@pytest.mark.parametrize(
    "predicate,expected",
    [
        ("id = 0.0 OR id = 3", 1),
        ("name = 'x' OR id = 0.0 OR id = 3", 1),
        ("gravity = 3.7 OR gravity = 9", 3),
        ("id = 1 OR id = 2 OR id = 3.0", 3),
        ("id IN (3.0, 4.5)", 1),
        ("id NOT IN (3.0, 4.5)", 8),
        ("id IN (2.0, 9.0)", 2),
        ("id IN (CAST('NaN' AS FLOAT64), 2.0)", 1),
        ("id NOT IN (CAST('Infinity' AS FLOAT64))", 9),
        ("id IN (1e300, 2.0)", 1),
    ],
)
def test_in_list_literal_types(predicate, expected):
    assert _count(predicate) == expected


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
