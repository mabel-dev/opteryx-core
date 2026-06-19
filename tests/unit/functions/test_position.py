"""Regression tests for POSITION(needle IN haystack).

Bug: a constant needle (e.g. ``POSITION('a' IN name)``) reached the native
``vector_position`` kernel as a Cython shim Vector (``draken.vectors.vector``)
rather than the native ``draken.draken_native.Vector`` the kernel unwraps,
raising::

    TypeError: draken_vector_unwrap: expected draken.draken_native.Vector,
               got draken.vectors.vector.Vector

The fix routes POSITION through a wrapper that unwraps the shims to ``._nb``
before calling the kernel, mirroring the convention used by every other non-nb
function (see ``implementations.logical.if_null``).

Semantics verified against DuckDB: 1-based, 0 when not found, empty needle -> 1,
NULL input -> NULL.

The non-standard ``STRPOS`` alias was dropped at the same time: it has the
opposite argument order (``STRPOS(haystack, needle)``) and so cannot share
POSITION's overload. It is now an unknown function rather than a silently wrong
answer.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import FunctionNotFoundError


def _column(sql, column):
    session = opteryx.session()
    values = []
    for morsel in session.execute_to_morsels(sql):
        values.extend(morsel.column(column).to_pylist())
    return values


@pytest.mark.parametrize(
    "sql, expected",
    [
        # constant needle over a column (the reported repro)
        ("SELECT POSITION('a' IN name) AS p FROM $planets WHERE name = 'Mars'", [2]),
        # not found -> 0
        ("SELECT POSITION('z' IN name) AS p FROM $planets WHERE name = 'Mars'", [0]),
        # empty needle -> 1
        ("SELECT POSITION('' IN name) AS p FROM $planets WHERE name = 'Mars'", [1]),
        # multi-byte needle (exercises the Boyer-Moore-Horspool path)
        ("SELECT POSITION('ar' IN name) AS p FROM $planets WHERE name = 'Mars'", [2]),
        ("SELECT POSITION('ur' IN name) AS p FROM $planets WHERE name = 'Mercury'", [5]),
        # NULL haystack -> NULL
        ("SELECT POSITION('a' IN CAST(NULL AS VARCHAR)) AS p FROM $planets WHERE name = 'Mars'", [None]),
        # literal IN literal (constant haystack and needle)
        ("SELECT POSITION('e' IN 'barge') AS p FROM $planets WHERE name = 'Mars'", [5]),
    ],
)
def test_position_constant_needle(sql, expected):
    assert _column(sql, b"p") == expected


def test_strpos_alias_dropped():
    with pytest.raises(FunctionNotFoundError):
        list(opteryx.session().execute_to_morsels("SELECT STRPOS(name, 'a') FROM $planets"))


if __name__ == "__main__":  # pragma: no cover
    test_position_constant_needle(
        "SELECT POSITION('a' IN name) AS p FROM $planets WHERE name = 'Mars'", [2]
    )
    test_strpos_alias_dropped()
    print("✅ okay")
