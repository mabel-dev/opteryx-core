# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression: a CTE (or view) body holding a NESTED derived table whose projection
is a function over a column must bind.

A derived table's Subquery boundary carries the same expression objects as the
Project beneath it. `rename_relations` (run when a CTE/view body is spliced in)
used to rewrite each holder separately, splitting one expression into two; the
boundary's copy was never bound and failed with
`Ambiguous function call: TRUNC(?, VARCHAR)` over an untyped argument.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx

SOURCE = "testdata.flat.space_missions"


def _row_count(sql):
    session = opteryx.session()
    return sum(morsel.num_rows for morsel in session.execute_to_morsels(sql))


@pytest.mark.parametrize(
    "sql",
    [
        f"WITH v AS (SELECT s.h FROM (SELECT TRUNC(e.Lauched_at, 'HOUR') AS h FROM {SOURCE} AS e) AS s) SELECT * FROM v",
        f"WITH v AS (SELECT s.h FROM (SELECT TRUNC(e.Lauched_at::TIMESTAMP, 'HOUR') AS h FROM {SOURCE} AS e) AS s) SELECT * FROM v",
        f"WITH v AS (SELECT s.h, s.h AS h2 FROM (SELECT TRUNC(e.Lauched_at, 'DAY') AS h FROM {SOURCE} AS e) AS s) "
        "SELECT a.h FROM v AS a INNER JOIN v AS b ON a.h = b.h2",
    ],
)
def test_nested_derived_table_function_inside_cte(sql):
    assert _row_count(sql) > 0


def test_nested_derived_table_matches_unnested():
    nested = _row_count(
        f"WITH v AS (SELECT s.h FROM (SELECT TRUNC(e.Lauched_at, 'HOUR') AS h FROM {SOURCE} AS e) AS s) SELECT * FROM v"
    )
    plain = _row_count(f"SELECT TRUNC(e.Lauched_at, 'HOUR') AS h FROM {SOURCE} AS e")
    assert nested == plain


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
