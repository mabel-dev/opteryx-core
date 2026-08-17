# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression test: a CONCAT/CONCAT_WS call nested inside another CONCAT/CONCAT_WS
call's own arguments reached the bytecode builder undesugared.

CONCAT and CONCAT_WS have no kernel/callable_ref of their own (see
opteryx/expression/functions/registrar/text.pyx) -- they are rewrite-only
functions, meant to reach execution only after PredicateRewriteStrategy /
FunctionRewriteStrategy desugar them to StringConcat ('||') chains
(_rewrite_function in predicate_rewriter.py).

_rewrite_predicate's generic recursion only walks an already-parsed `||`
(a BINARY_OPERATOR tree) into its left/right operands -- it never descends
into a FUNCTION node's own .parameters. The CONCAT/CONCAT_WS branches of
_rewrite_function built their StringConcat chain directly from
function.parameters without first rewriting them, so a nested CONCAT/CONCAT_WS
call used as one of THIS call's own arguments -- e.g.
CONCAT(CONCAT(a, b), c) -- survived unrewritten, embedded inside the new
StringConcat tree, and was refused at compile time: "a function call ...
outside the c-native kernel set, is not supported."

This is the exact shape TPC-DS Q84 hits:
    CONCAT(CONCAT(COALESCE(c_last_name, ''), ', '), COALESCE(c_first_name, ''))

A CONCAT call with only column/literal operands (no nested CONCAT/CONCAT_WS)
never hit this, because there was nothing left undesugared inside the chain.

Fixed in opteryx/planner/optimizer/strategies/predicate_rewriter.py by
recursively rewriting each CONCAT/CONCAT_WS operand (_rewrite_predicate)
before consuming it to build the StringConcat chain.
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


def test_concat_wrapping_concat_of_coalesce_tpcds_q84_shape():
    # The exact nesting from tests/performance/tpcds/opteryx/queries/query84.sql:
    # CONCAT(...) as the left operand of the desugared `||`, with COALESCE
    # calls as CONCAT's own arguments.
    rows = _col(
        "SELECT CONCAT(CONCAT(COALESCE(name, ''), ', '), COALESCE(name, '')) AS c "
        "FROM $planets WHERE id = 1"
    )
    assert rows == ["Mercury, Mercury"]


def test_concat_wrapping_concat_with_null_coalesce_operand():
    rows = _col(
        "SELECT CONCAT(CONCAT(COALESCE(NULL, 'x'), '-'), COALESCE(NULL, 'y')) AS c"
    )
    assert rows == ["x-y"]


def test_concat_ws_wrapping_concat():
    rows = _col("SELECT CONCAT_WS('-', CONCAT('a', 'b'), 'c') AS c")
    assert rows == ["ab-c"]


def test_concat_wrapping_concat_ws():
    rows = _col("SELECT CONCAT(CONCAT_WS('-', 'a', 'b'), 'c') AS c")
    assert rows == ["a-bc"]


def test_concat_ws_two_arg_wrapping_concat():
    # The CONCAT_WS(sep, x) degenerate form (x || '') with a nested CONCAT as x.
    rows = _col("SELECT CONCAT_WS('-', CONCAT('a', 'b')) AS c")
    assert rows == ["ab"]
