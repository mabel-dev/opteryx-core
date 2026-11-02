"""
IN-list elements written as expressions — TPC-DS writes `d_year IN (1999, 1999+1, 1999+2)`.
The list is built at parse time, before the binder runs, so each non-literal element is
bound and folded natively there. When folding went native-only these elements were
unbound and every such query was refused (TPC-DS Q23, Q29, Q34, Q46, Q53, Q63, Q68,
Q73, Q74, Q79).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.exceptions import ArrayWithMixedTypesError


def _column(sql):
    session = opteryx.session()
    return sorted(v for m in session.execute_to_morsels(sql) for v in m.column(m.column_names[0]).to_pylist())


@pytest.mark.parametrize(
    "predicate,expected",
    [
        ("id IN (1, 1 + 1)", [1, 2]),
        ("id IN (2 * 2, 10 - 1)", [4, 9]),
        ("id IN (2 * 2, 10 - 1, NULL)", [4, 9]),
        ("id NOT IN (1 + 1, 3)", [1, 4, 5, 6, 7, 8, 9]),
        ("name IN ('Earth', 'Ma' || 'rs')", ["Earth", "Mars"]),
    ],
)
def test_expression_elements_fold(predicate, expected):
    column = "name" if predicate.startswith("name") else "id"
    assert _column(f"SELECT {column} FROM $planets WHERE {predicate}") == expected


def test_folded_element_of_another_type_is_mixed():
    with pytest.raises(ArrayWithMixedTypesError):
        _column("SELECT id FROM $planets WHERE id IN (1, 'a' || 'b')")
