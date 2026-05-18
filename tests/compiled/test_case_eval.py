"""Unit tests for the lazy CASE WHEN evaluator (PR 1 — pure addition).

Tests each component in isolation using synthetic Draken vectors and
Morsels, plus end-to-end smoke tests via evaluate_case() with LITERAL nodes.

No SQL execution, no PyArrow in the hot path — morsels are built directly
from Draken vectors via Morsel.from_vectors().
"""

import sys
from array import array as pyarray
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest

from draken.morsels.morsel import Morsel
from draken.vectors.bool_vector import BoolVector
from draken.vectors.integer64_vector import Integer64Vector
from draken.vectors.null_vector import NullVector
from draken.vectors.string_vector import StringVector
from opteryx.compiled.vector_ops import (
    assemble_bool,
    assemble_dict_string,
    assemble_fixed,
    assemble_flat_string,
    decide_one_branch,
    group_indices_and_perm,
)
from opteryx.compiled.vector_ops.vector_ops import (
    _make_const_int16,
    _make_range_int32,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _int64_vec(values):
    """Build an Integer64Vector from a list (None → null)."""
    import pyarrow as pa
    arr = pa.array(values, type=pa.int64())
    return Integer64Vector.from_arrow(arr)


def _bool_vec(values):
    """Build a BoolVector from a list of True/False/None."""
    import pyarrow as pa
    arr = pa.array(values, type=pa.bool_())
    return BoolVector.from_arrow(arr)


def _str_vec(values):
    """Build a flat StringVector from a list of str/None."""
    import pyarrow as pa
    arr = pa.array(values, type=pa.large_utf8())
    return StringVector.from_arrow(arr)


def _dict_str_vec(values):
    """Build a dict-encoded StringVector from a list of str/None."""
    # Build unique entries and codes directly, avoiding PyArrow dict limitations
    unique = []
    seen = {}
    codes = []
    for v in values:
        if v is None:
            codes.append(-1)
        else:
            if v not in seen:
                seen[v] = len(unique)
                unique.append(v)
            codes.append(seen[v])
    # row_validity: 1 for non-null, 0 for null
    validity = bytearray(1 if c >= 0 else 0 for c in codes)
    safe_codes = [c if c >= 0 else 0 for c in codes]
    return StringVector.from_dict(safe_codes, unique, validity)


def _morsel(**cols):
    """Build a Morsel from keyword args mapping col_name → Draken vector."""
    names = [k.encode() for k in cols]
    return Morsel.from_vectors(names, list(cols.values()))


# ---------------------------------------------------------------------------
# Array initialisation helpers
# ---------------------------------------------------------------------------

def test_make_range_int32_basic():
    r = _make_range_int32(5)
    assert list(r) == [0, 1, 2, 3, 4]


def test_make_range_int32_empty():
    r = _make_range_int32(0)
    assert list(r) == []


def test_make_const_int16_minus_one():
    a = _make_const_int16(4, -1)
    assert list(a) == [-1, -1, -1, -1]


def test_make_const_int16_zero():
    a = _make_const_int16(3, 0)
    assert list(a) == [0, 0, 0]


# ---------------------------------------------------------------------------
# decide_one_branch
# ---------------------------------------------------------------------------

def test_decide_one_branch_all_true():
    bv = _bool_vec([True, True, True])
    live = pyarray("i", [0, 1, 2])
    branch_id = _make_const_int16(3, -1)
    new_live = decide_one_branch(bv, live, branch_id, 0)
    assert list(new_live) == []
    assert list(branch_id) == [0, 0, 0]


def test_decide_one_branch_all_false():
    bv = _bool_vec([False, False, False])
    live = pyarray("i", [0, 1, 2])
    branch_id = _make_const_int16(3, -1)
    new_live = decide_one_branch(bv, live, branch_id, 0)
    assert list(new_live) == [0, 1, 2]
    assert list(branch_id) == [-1, -1, -1]


def test_decide_one_branch_mixed():
    # rows 0, 2 are True; row 1 is False; row 3 is NULL (treated as not-won)
    bv = _bool_vec([True, False, True, None])
    live = pyarray("i", [0, 1, 2, 3])
    branch_id = _make_const_int16(4, -1)
    new_live = decide_one_branch(bv, live, branch_id, 1)
    assert list(new_live) == [1, 3]
    assert branch_id[0] == 1
    assert branch_id[1] == -1
    assert branch_id[2] == 1
    assert branch_id[3] == -1


def test_decide_one_branch_sparse_live():
    # Only rows 2 and 4 are live
    bv = _bool_vec([True, False])  # evaluated on the 2 live rows
    live = pyarray("i", [2, 4])
    branch_id = _make_const_int16(5, -1)
    new_live = decide_one_branch(bv, live, branch_id, 0)
    assert list(new_live) == [4]
    assert branch_id[2] == 0
    assert branch_id[4] == -1


# ---------------------------------------------------------------------------
# group_indices_and_perm
# ---------------------------------------------------------------------------

def test_group_indices_and_perm_basic():
    branch_id = pyarray("h", [0, 1, 0, -1, 1])
    rpb, unmatched, pib = group_indices_and_perm(branch_id, 2)
    assert list(rpb[0]) == [0, 2]
    assert list(rpb[1]) == [1, 4]
    assert list(unmatched) == [3]
    # pos_in_branch: row 0 → pos 0 in branch 0; row 2 → pos 1 in branch 0;
    #               row 1 → pos 0 in branch 1; row 4 → pos 1 in branch 1;
    #               row 3 → pos 0 in unmatched
    assert pib[0] == 0
    assert pib[2] == 1
    assert pib[1] == 0
    assert pib[4] == 1
    assert pib[3] == 0


def test_group_indices_and_perm_all_unmatched():
    branch_id = pyarray("h", [-1, -1, -1])
    rpb, unmatched, pib = group_indices_and_perm(branch_id, 1)
    assert list(rpb[0]) == []
    assert list(unmatched) == [0, 1, 2]


def test_group_indices_and_perm_all_matched():
    branch_id = pyarray("h", [0, 0, 0])
    rpb, unmatched, pib = group_indices_and_perm(branch_id, 1)
    assert list(rpb[0]) == [0, 1, 2]
    assert list(unmatched) == []


# ---------------------------------------------------------------------------
# assemble_fixed
# ---------------------------------------------------------------------------

def test_assemble_fixed_two_branches_no_else():
    # N=4: branch 0 → rows 0,2; branch 1 → rows 1,3; no unmatched
    branch_id = pyarray("h", [0, 1, 0, 1])
    rpb, unmatched, _ = group_indices_and_perm(branch_id, 2)

    parts = [
        _int64_vec([10, 20]),  # branch 0: rows 0,2 → values 10,20
        _int64_vec([30, 40]),  # branch 1: rows 1,3 → values 30,40
    ]
    result = assemble_fixed(parts, None, branch_id, rpb, unmatched)
    assert result.to_pylist() == [10, 30, 20, 40]


def test_assemble_fixed_with_else():
    # row 2 is unmatched → ELSE value 99
    branch_id = pyarray("h", [0, -1, 1])
    rpb, unmatched, _ = group_indices_and_perm(branch_id, 2)

    parts = [_int64_vec([10]), _int64_vec([30])]
    else_part = _int64_vec([99])
    result = assemble_fixed(parts, else_part, branch_id, rpb, unmatched)
    assert result.to_pylist() == [10, 99, 30]


def test_assemble_fixed_null_from_unmatched():
    # row 1 unmatched, no else_part → NULL
    branch_id = pyarray("h", [0, -1, 0])
    rpb, unmatched, _ = group_indices_and_perm(branch_id, 1)

    parts = [_int64_vec([10, 20])]
    result = assemble_fixed(parts, None, branch_id, rpb, unmatched)
    vals = result.to_pylist()
    assert vals[0] == 10
    assert vals[1] is None
    assert vals[2] == 20


def test_assemble_fixed_null_source_value():
    # Branch produces a null value for one row
    branch_id = pyarray("h", [0, 0])
    rpb, unmatched, _ = group_indices_and_perm(branch_id, 1)

    parts = [_int64_vec([10, None])]
    result = assemble_fixed(parts, None, branch_id, rpb, unmatched)
    vals = result.to_pylist()
    assert vals[0] == 10
    assert vals[1] is None


# ---------------------------------------------------------------------------
# assemble_bool
# ---------------------------------------------------------------------------

def test_assemble_bool_basic():
    branch_id = pyarray("h", [0, 1, 0, 1])
    rpb, unmatched, _ = group_indices_and_perm(branch_id, 2)

    parts = [
        _bool_vec([True, False]),
        _bool_vec([False, True]),
    ]
    result = assemble_bool(parts, None, branch_id, rpb, unmatched)
    assert result.to_pylist() == [True, False, False, True]


def test_assemble_bool_null_unmatched():
    branch_id = pyarray("h", [0, -1])
    rpb, unmatched, _ = group_indices_and_perm(branch_id, 1)
    parts = [_bool_vec([True])]
    result = assemble_bool(parts, None, branch_id, rpb, unmatched)
    vals = result.to_pylist()
    assert vals[0] is True
    assert vals[1] is None


# ---------------------------------------------------------------------------
# assemble_flat_string
# ---------------------------------------------------------------------------

def test_assemble_flat_string_basic():
    n = 4
    branch_id = pyarray("h", [0, 1, 0, 1])
    rpb, unmatched, pib = group_indices_and_perm(branch_id, 2)

    parts = [_str_vec(["hello", "world"]), _str_vec(["foo", "bar"])]
    result = assemble_flat_string(parts, None, branch_id, pib, n)
    assert result.to_pylist() == [b"hello", b"foo", b"world", b"bar"]


def test_assemble_flat_string_with_else():
    n = 3
    branch_id = pyarray("h", [0, -1, 0])
    rpb, unmatched, pib = group_indices_and_perm(branch_id, 1)

    parts = [_str_vec(["hello", "world"])]
    else_part = _str_vec(["fallback"])
    result = assemble_flat_string(parts, else_part, branch_id, pib, n)
    assert result.to_pylist() == [b"hello", b"fallback", b"world"]


def test_assemble_flat_string_null_unmatched():
    n = 3
    branch_id = pyarray("h", [0, -1, 0])
    rpb, unmatched, pib = group_indices_and_perm(branch_id, 1)

    parts = [_str_vec(["hello", "world"])]
    result = assemble_flat_string(parts, None, branch_id, pib, n)
    vals = result.to_pylist()
    assert vals[0] == b"hello"
    assert vals[1] is None
    assert vals[2] == b"world"


def test_assemble_flat_string_dict_input():
    # dict-encoded input → should be transparently handled (decoded in output)
    n = 4
    branch_id = pyarray("h", [0, 1, 0, 1])
    rpb, unmatched, pib = group_indices_and_perm(branch_id, 2)

    parts = [_dict_str_vec(["cat", "dog"]), _dict_str_vec(["bird", "fish"])]
    result = assemble_flat_string(parts, None, branch_id, pib, n)
    vals = result.to_pylist()
    assert vals == [b"cat", b"bird", b"dog", b"fish"]


# ---------------------------------------------------------------------------
# assemble_dict_string
# ---------------------------------------------------------------------------

def test_assemble_dict_string_basic():
    n = 4
    branch_id = pyarray("h", [0, 1, 0, 1])
    rpb, unmatched, pib = group_indices_and_perm(branch_id, 2)

    parts = [_dict_str_vec(["cat", "dog"]), _dict_str_vec(["bird", "fish"])]
    result = assemble_dict_string(parts, None, branch_id, pib, n)
    assert result.dictionary_size > 0  # preserved dict encoding
    assert result.to_pylist() == [b"cat", b"bird", b"dog", b"fish"]


def test_assemble_dict_string_shared_values():
    # Both branches share some dict values — unified dict should deduplicate
    n = 4
    branch_id = pyarray("h", [0, 1, 0, 1])
    rpb, unmatched, pib = group_indices_and_perm(branch_id, 2)

    parts = [_dict_str_vec(["cat", "dog"]), _dict_str_vec(["dog", "cat"])]
    result = assemble_dict_string(parts, None, branch_id, pib, n)
    assert result.dictionary_size == 2  # deduplicated
    vals = result.to_pylist()
    assert vals == [b"cat", b"dog", b"dog", b"cat"]


def test_assemble_dict_string_with_else():
    n = 3
    branch_id = pyarray("h", [0, -1, 0])
    rpb, unmatched, pib = group_indices_and_perm(branch_id, 1)

    parts = [_dict_str_vec(["hello", "world"])]
    else_part = _dict_str_vec(["fallback"])
    result = assemble_dict_string(parts, else_part, branch_id, pib, n)
    assert result.dictionary_size > 0
    assert result.to_pylist() == [b"hello", b"fallback", b"world"]


# ---------------------------------------------------------------------------
# evaluate_case — end-to-end with LITERAL nodes
# ---------------------------------------------------------------------------

def _make_literal_node(value):
    from opteryx.expression import NodeType
    from opteryx.planner.logical_planner.logical_planner_builders import Node
    return Node(NodeType.LITERAL, value=value)


def _make_case_node(conditions, results, else_result=None):
    """Minimal node object matching the NodeType.CASE contract."""
    class _Node:
        pass
    n = _Node()
    n.conditions = conditions
    n.results = results
    n.else_result = else_result
    return n


def test_evaluate_case_literal_true_condition():
    """CASE WHEN TRUE THEN 42 END — every row gets 42."""
    from opteryx.expression.evaluator.case_eval import evaluate_case

    morsel = _morsel(x=Integer64Vector.from_constant(0, 5))
    node = _make_case_node(
        conditions=[_make_literal_node(True)],
        results=[_make_literal_node(42)],
        else_result=None,
    )
    result = evaluate_case(node, morsel)
    assert result.to_pylist() == [42, 42, 42, 42, 42]


def test_evaluate_case_literal_false_with_else():
    """CASE WHEN FALSE THEN 1 ELSE 99 END — every row gets 99."""
    from opteryx.expression.evaluator.case_eval import evaluate_case

    morsel = _morsel(x=Integer64Vector.from_constant(0, 3))
    node = _make_case_node(
        conditions=[_make_literal_node(False)],
        results=[_make_literal_node(1)],
        else_result=_make_literal_node(99),
    )
    result = evaluate_case(node, morsel)
    assert result.to_pylist() == [99, 99, 99]


def test_evaluate_case_no_match_no_else_is_null():
    """CASE WHEN FALSE THEN 1 END — every row is NULL."""
    from opteryx.expression.evaluator.case_eval import evaluate_case

    morsel = _morsel(x=Integer64Vector.from_constant(0, 3))
    node = _make_case_node(
        conditions=[_make_literal_node(False)],
        results=[_make_literal_node(1)],
        else_result=None,
    )
    result = evaluate_case(node, morsel)
    assert result.to_pylist() == [None, None, None]


def test_evaluate_case_first_branch_wins():
    """CASE WHEN TRUE THEN 'a' WHEN TRUE THEN 'b' END — first branch wins."""
    from opteryx.expression.evaluator.case_eval import evaluate_case

    morsel = _morsel(x=Integer64Vector.from_constant(0, 2))
    node = _make_case_node(
        conditions=[_make_literal_node(True), _make_literal_node(True)],
        results=[_make_literal_node("a"), _make_literal_node("b")],
        else_result=None,
    )
    result = evaluate_case(node, morsel)
    vals = result.to_pylist()
    assert all(v == b"a" for v in vals)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
