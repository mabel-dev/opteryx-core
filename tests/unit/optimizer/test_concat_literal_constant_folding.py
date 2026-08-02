# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression test: an all-literal CONCAT/CONCAT_WS call crashed constant folding.

CONCAT and CONCAT_WS have no kernel/callable_ref of their own (see
opteryx/expression/functions/registrar/text.pyx) -- they are rewrite-only
functions, meant to reach execution only after PredicateRewriteStrategy /
FunctionRewriteStrategy desugar them to StringConcat ('||') chains. Those
strategies run AFTER ConstantFoldingStrategy in the optimizer pipeline
(opteryx/planner/optimizer/__init__.py), so an all-literal call such as
CONCAT('x', 'a', 'y') satisfied constant folding's fold-eligibility check
(no identifiers/aggregators) and reached execute_bytecode() with a NULL
callable_ref, crashing with `TypeError: 'NoneType' object is not callable`.

A CONCAT call with a column operand (e.g. CONCAT(name, 'x')) never hit this,
because the presence of an identifier makes it ineligible for folding, so it
survived untouched to FunctionRewriteStrategy as before.

Fixed in opteryx/planner/optimizer/strategies/constant_folding.py by applying
the same CONCAT/CONCAT_WS -> StringConcat desugaring (predicate_rewriter.py's
_rewrite_function) before attempting to fold an eligible FUNCTION node.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def _col(sql, name="c"):
    rows = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        rows.extend(morsel.column(name).to_pylist())
    return rows


def test_concat_all_literal_two_args():
    assert _col("SELECT CONCAT('x', 'a') AS c") == ["xa"]


def test_concat_all_literal_three_args():
    assert _col("SELECT CONCAT('x', 'a', 'y') AS c") == ["xay"]


def test_concat_ws_all_literal():
    assert _col("SELECT CONCAT_WS('-', 'a', 'b', 'c') AS c") == ["a-b-c"]


def test_concat_ws_all_literal_single_value():
    # The 2-parameter degenerate form (separator + one value): x || ''
    assert _col("SELECT CONCAT_WS('-', 'a') AS c") == ["a"]


def test_concat_with_column_operand_still_works():
    rows = _col("SELECT CONCAT(name, 'x') AS c FROM $planets WHERE id = 1")
    assert rows == ["Mercuryx"]


def test_concat_ws_with_column_operand_still_works():
    rows = _col("SELECT CONCAT_WS('-', name, 'x') AS c FROM $planets WHERE id = 1")
    assert rows == ["Mercury-x"]
