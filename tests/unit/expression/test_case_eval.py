# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Unit tests for the lazy CASE evaluator in opteryx.expression.evaluator.case_eval.

Validates Phase-1 of the CASE rewrite: a node-aware evaluator that runs lazily
(conditions and result expressions are only evaluated on rows that need them)
and preserves dict encoding on string outputs when every part qualifies.

Tests construct synthetic NodeType.CASE nodes directly — nothing in production
emits these yet (PR 2 wires up the binder).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from draken.morsels.morsel import Morsel
from draken.interop.vector_sequence import vector_from_sequence
from draken.vectors.string_vector import StringVector

from opteryx.compiled.structures.node import Node
from opteryx.expression import NodeType
from opteryx.expression.evaluator.case_eval import evaluate_case


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


class _SC:
    """Minimal schema_column stand-in for tests."""

    __slots__ = ("identity", "name", "type")

    def __init__(self, name="c", type_=None):
        self.identity = name.encode() if isinstance(name, str) else name
        self.name = name if isinstance(name, str) else name.decode()
        self.type = type_


def _ident(name):
    return Node(NodeType.IDENTIFIER, value=name, schema_column=_SC(name))


def _lit(value, name=None):
    if name is None:
        name = f"_lit_{id(value)}"
    return Node(NodeType.LITERAL, value=value, schema_column=_SC(name))


def _eq(left, right, name=None):
    return Node(
        NodeType.COMPARISON_OPERATOR,
        value="Eq",
        left=left,
        right=right,
        schema_column=_SC(name or "_eq"),
    )


def _case(conditions, results, else_result=None, name="_case"):
    return Node(
        NodeType.CASE,
        conditions=list(conditions),
        results=list(results),
        else_result=else_result,
        schema_column=_SC(name),
    )


def _morsel(**columns):
    names = list(columns.keys())
    vecs = [
        v if not isinstance(v, list) else vector_from_sequence(v)
        for v in columns.values()
    ]
    return Morsel.from_vectors(names, vecs)


# ---------------------------------------------------------------------------
# Correctness — fixed-width output
# ---------------------------------------------------------------------------


def test_two_branch_safe_int():
    """CASE WHEN o = 0 THEN 0 ELSE 100 END."""
    morsel = _morsel(o=[1, 0, 5, 0, 7])
    case = _case(
        conditions=[_eq(_ident("o"), _lit(0))],
        results=[_lit(0)],
        else_result=_lit(100),
    )
    out = evaluate_case(case, morsel).to_pylist()
    assert out == [100, 0, 100, 0, 100]


def test_three_branch_priority():
    """First true wins. CASE WHEN o=0 THEN 0 WHEN o=1 THEN 1 ELSE 9 END."""
    morsel = _morsel(o=[0, 1, 2, 1, 0, 3])
    case = _case(
        conditions=[
            _eq(_ident("o"), _lit(0)),
            _eq(_ident("o"), _lit(1)),
        ],
        results=[_lit(0), _lit(1)],
        else_result=_lit(9),
    )
    out = evaluate_case(case, morsel).to_pylist()
    assert out == [0, 1, 9, 1, 0, 9]


def test_no_else_unmatched_is_null():
    """No ELSE: rows that match no condition come out NULL."""
    morsel = _morsel(o=[0, 1, 2, 0])
    case = _case(
        conditions=[_eq(_ident("o"), _lit(0))],
        results=[_lit(7)],
        else_result=None,
    )
    out = evaluate_case(case, morsel).to_pylist()
    assert out == [7, None, None, 7]


# ---------------------------------------------------------------------------
# Constant-condition shortcuts
# ---------------------------------------------------------------------------


def test_const_true_short_circuits():
    """A LITERAL True condition wins for every remaining row; later branches
    are unreachable (and must not be evaluated)."""
    morsel = _morsel(o=[1, 2, 3])
    # Reference a column that does NOT exist in the morsel — would raise on
    # eager evaluation. Lazy evaluator should never touch it.
    bad = _ident("absent_column")
    case = _case(
        conditions=[_lit(True), _eq(_ident("o"), _lit(0))],
        results=[_lit(7), bad],
        else_result=bad,
    )
    out = evaluate_case(case, morsel).to_pylist()
    assert out == [7, 7, 7]


def test_const_false_branch_dropped():
    """LITERAL False condition is dead — its result expr must not be evaluated."""
    morsel = _morsel(o=[1, 2, 3])
    bad = _ident("absent_column")
    case = _case(
        conditions=[_lit(False), _eq(_ident("o"), _lit(2))],
        results=[bad, _lit(20)],
        else_result=_lit(99),
    )
    out = evaluate_case(case, morsel).to_pylist()
    assert out == [99, 20, 99]


def test_const_null_treated_as_false():
    """LITERAL None condition is dead (SQL three-valued logic)."""
    morsel = _morsel(o=[1, 2])
    bad = _ident("absent_column")
    case = _case(
        conditions=[_lit(None), _eq(_ident("o"), _lit(1))],
        results=[bad, _lit(11)],
        else_result=_lit(0),
    )
    out = evaluate_case(case, morsel).to_pylist()
    assert out == [11, 0]


# ---------------------------------------------------------------------------
# Laziness — result expressions only evaluated on matched rows
# ---------------------------------------------------------------------------


def test_lazy_else_skipped_when_all_rows_match():
    """ELSE expression is not evaluated when no rows fall through."""
    morsel = _morsel(o=[0, 0, 0])
    bad_else = _ident("absent_column")
    case = _case(
        conditions=[_eq(_ident("o"), _lit(0))],
        results=[_lit(1)],
        else_result=bad_else,
    )
    out = evaluate_case(case, morsel).to_pylist()
    assert out == [1, 1, 1]


def test_lazy_branch_skipped_when_no_rows_match():
    """A branch with zero matching rows is not evaluated."""
    morsel = _morsel(o=[0, 0, 0])
    bad_then = _ident("absent_column")
    case = _case(
        conditions=[
            _eq(_ident("o"), _lit(99)),  # never true
            _eq(_ident("o"), _lit(0)),  # matches all
        ],
        results=[bad_then, _lit(5)],
        else_result=_lit(7),
    )
    out = evaluate_case(case, morsel).to_pylist()
    assert out == [5, 5, 5]


# ---------------------------------------------------------------------------
# String output — flat path
# ---------------------------------------------------------------------------


def test_two_branch_string_output():
    """String result: CASE WHEN o = 0 THEN 'zero' ELSE 'other' END."""
    morsel = _morsel(o=[0, 1, 0, 2])
    case = _case(
        conditions=[_eq(_ident("o"), _lit(0))],
        results=[_lit(b"zero")],
        else_result=_lit(b"other"),
    )
    result = evaluate_case(case, morsel)
    out = result.to_pylist()
    assert out == [b"zero", b"other", b"zero", b"other"]


# ---------------------------------------------------------------------------
# String output — dict-encoded path
# ---------------------------------------------------------------------------


def test_dict_string_inputs_correct_output():
    """CASE over a dict-encoded column produces correct values.

    Encoding preservation is verified end-to-end against Q40-shaped Parquet
    inputs in PR 2 — the Python `StringVector.from_dict` constructor used here
    produces a hybrid (dict + dense) form which `take()` materialises.  Only
    the C++ Parquet pipeline (`make_string_dict_only`) creates true dict-only
    vectors that `take()` can preserve.  This test validates correctness; PR 2
    validates encoding preservation through the full Q40 path.
    """
    referer = StringVector.from_dict(
        [0, 1, 0, 2, 1], [b"a.com", b"b.com", b"c.com"]
    )
    morsel = Morsel.from_vectors(
        ["o", "ref"], [vector_from_sequence([0, 1, 0, 1, 0]), referer]
    )

    # CASE WHEN o = 0 THEN ref ELSE '' END
    case = _case(
        conditions=[_eq(_ident("o"), _lit(0))],
        results=[_ident("ref")],
        else_result=_lit(b""),
    )
    result = evaluate_case(case, morsel)
    assert isinstance(result, StringVector)
    # referer = [a.com, b.com, a.com, c.com, b.com], o = [0,1,0,1,0]
    # Rows 0/2/4 match → take from referer; rows 1/3 unmatched → take ''.
    out = result.to_pylist()
    assert out == [b"a.com", b"", b"a.com", b"", b"b.com"]


# ---------------------------------------------------------------------------
# Empty morsel
# ---------------------------------------------------------------------------


def test_empty_morsel_int():
    """Zero-row morsel returns a zero-length vector of the right family."""
    morsel = _morsel(o=vector_from_sequence([1, 2]).take(__import__("array").array("i", [])))
    assert morsel.num_rows == 0
    case = _case(
        conditions=[_eq(_ident("o"), _lit(0))],
        results=[_lit(1)],
        else_result=_lit(2),
    )
    result = evaluate_case(case, morsel)
    # Should be a length-0 vector
    out = result.to_pylist()
    assert out == []


# ---------------------------------------------------------------------------
# Nested CASE
# ---------------------------------------------------------------------------


def test_nested_case():
    """CASE within a result expression must recurse correctly."""
    morsel = _morsel(o=[0, 1, 2, 3])
    inner = _case(
        conditions=[_eq(_ident("o"), _lit(1))],
        results=[_lit(11)],
        else_result=_lit(99),
        name="_inner",
    )
    # outer: WHEN o=0 THEN 0 ELSE inner END
    outer = _case(
        conditions=[_eq(_ident("o"), _lit(0))],
        results=[_lit(0)],
        else_result=inner,
        name="_outer",
    )
    out = evaluate_case(outer, morsel).to_pylist()
    assert out == [0, 11, 99, 99]


# ---------------------------------------------------------------------------
# Bool output
# ---------------------------------------------------------------------------


def test_bool_output():
    """CASE returning bools."""
    morsel = _morsel(o=[0, 1, 2, 0])
    case = _case(
        conditions=[_eq(_ident("o"), _lit(0))],
        results=[_lit(True)],
        else_result=_lit(False),
    )
    out = evaluate_case(case, morsel).to_pylist()
    assert out == [True, False, False, True]


if __name__ == "__main__":  # pragma: no cover
    import sys as _sys
    pytest.main([__file__, "-v"] + _sys.argv[1:])
