# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
`FLOAT64 IN (literal, ...)` / `NOT IN` run on draken_in_list's kind-2 (float64) arm.

They used to be refused at plan time ("outside the c-native kernel set"): the IN
lowering packed no FLOAT64 list and the kernel had no float arm. The membership test
must mean exactly the engine's float `=` (fp_total_eq — NaN = NaN, -0.0 = 0.0), so
every case here is asserted as a RELATIONSHIP: IN agrees with the OR of `=` it
abbreviates, NOT IN with the AND of `<>`, and a NULL operand stays NULL.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

NAN = "CAST('nan' AS FLOAT64)"
INF = "CAST('inf' AS FLOAT64)"
VALUES = (
    f"(VALUES (1.5), ({NAN}), (-0.0), (0.0), (NULL), (2.0), (-3.25), ({INF})) AS t(x)"
)


def _rows(where):
    rows = []
    for morsel in opteryx.session().execute_to_morsels(f"SELECT x FROM {VALUES} WHERE {where}"):
        rows.extend(morsel.column("x").to_pylist())
    return sorted(repr(row) for row in rows)


@pytest.mark.parametrize(
    "membership, expanded",
    [
        ("x IN (0.0, 1.5)", "x = 0.0 OR x = 1.5"),
        ("x IN (-0.0, 2.0)", "x = -0.0 OR x = 2.0"),
        (f"x IN ({NAN}, 2.0)", f"x = {NAN} OR x = 2.0"),
        (f"x IN ({INF}, 7.0)", f"x = {INF} OR x = 7.0"),
        ("x IN (7.0, 8.0)", "x = 7.0 OR x = 8.0"),
        ("x IN (1, 2)", "x = 1 OR x = 2"),
        ("x NOT IN (0.0, 1.5)", "x <> 0.0 AND x <> 1.5"),
        (f"x NOT IN ({NAN}, -3.25)", f"x <> {NAN} AND x <> -3.25"),
    ],
)
def test_float64_in_list_means_the_expanded_comparison(membership, expanded):
    assert _rows(membership) == _rows(expanded)


def test_float64_in_list_matches_both_zeros_and_nan():
    assert _rows(f"x IN (0.0, {NAN})") == ["0.0", "0.0", "nan"]


def test_float64_in_list_of_null_operand_is_null():
    assert _rows("(x IN (0.0, 1.5)) IS NULL") == ["None"]
    assert _rows("(x NOT IN (0.0, 1.5)) IS NULL") == ["None"]
