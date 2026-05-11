# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Selection Node

This is a SQL Query Execution Plan Node.

This node is responsible for applying filters to datasets.
"""

from typing import Generator, Optional
from opteryx.expression import NodeType
from opteryx.expression import format_expression
from opteryx.expression import get_all_nodes_of_type
from opteryx.models import QueryProperties
from opteryx import EOS

from draken.vectors.vector cimport Vector
from draken.encoding import DRAKEN_ENCODING_CONSTANT as _CONSTANT_ENCODING

from . import BasePlanNode


def _extract_constant_replacements(filter_expr):
    """Find IDENTIFIER == LITERAL predicates that force a column to be constant
    in all rows surviving the filter.

    Descends through AND and NESTED only — OR, NOT, function calls, etc.
    terminate the walk on that branch. Returns a list of (identity, value)
    tuples. The identity is the bytes used as the morsel column key.
    """
    if filter_expr is None:
        return []

    preds = []
    stack = [filter_expr]
    while stack:
        n = stack.pop()
        nt = n.node_type
        if nt == NodeType.NESTED:
            if n.centre is not None:
                stack.append(n.centre)
            continue
        if nt == NodeType.AND:
            if n.left is not None:
                stack.append(n.left)
            if n.right is not None:
                stack.append(n.right)
            continue
        if nt == NodeType.DNF:
            # Despite the name, DNF here is a flat AND-list of sub-predicates
            # (see opteryx.expression.evaluator.evaluation: each parameter is
            # combined via and_vector). It is the planner's normalized form for
            # multi-predicate filters.
            params = getattr(n, "parameters", None) or []
            for sub in params:
                if sub is not None:
                    stack.append(sub)
            continue
        if nt != NodeType.COMPARISON_OPERATOR or n.value != "Eq":
            continue
        left = n.left
        right = n.right
        if left is None or right is None:
            continue
        if (left.node_type == NodeType.IDENTIFIER
                and right.node_type == NodeType.LITERAL):
            ident_node, lit_node = left, right
        elif (right.node_type == NodeType.IDENTIFIER
                and left.node_type == NodeType.LITERAL):
            ident_node, lit_node = right, left
        else:
            continue
        sc = getattr(ident_node, "schema_column", None)
        if sc is None:
            continue
        lit_val = lit_node.value
        if lit_val is None:
            continue
        preds.append((sc.identity, lit_val))
    return preds


cdef Vector _build_constant_vector(Vector cur, object value, Py_ssize_t length):
    """Produce a constant-encoded vector matching cur's concrete type.

    Returns None for vector types we don't yet handle (temporal, decimal, etc.)
    or when the literal's Python type can't safely map onto the column dtype.
    """
    from draken.vectors.int64_vector import Int64Vector
    from draken.vectors.float64_vector import Float64Vector
    from draken.vectors.bool_vector import BoolVector
    from draken.vectors.string_vector import StringVector

    if isinstance(cur, BoolVector):
        if not isinstance(value, bool):
            return None
        return BoolVector.from_constant(value, length)
    if isinstance(cur, Int64Vector):
        if isinstance(value, bool) or not isinstance(value, int):
            return None
        return Int64Vector.from_constant(value, length)
    if isinstance(cur, Float64Vector):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        return Float64Vector.from_constant(float(value), length)
    if isinstance(cur, StringVector):
        if not isinstance(value, (str, bytes)):
            return None
        return StringVector.from_constant(value, length)
    return None


cdef void _apply_constant_replacements(Morsel morsel, list replacements):
    cdef Py_ssize_t length = morsel.ptr.num_rows
    cdef Py_ssize_t idx
    cdef Vector cur
    cdef Vector new_vec
    cdef dict mapping
    cdef object py_idx

    if length == 0 or not replacements:
        return

    mapping = morsel._ensure_name_map()

    for identity, value in replacements:
        py_idx = mapping.get(identity)
        if py_idx is None:
            continue
        idx = <Py_ssize_t>py_idx
        cur = <Vector>morsel._columns[idx]
        if cur is None:
            continue
        if cur._encoding == _CONSTANT_ENCODING:
            continue
        new_vec = _build_constant_vector(cur, value, length)
        if new_vec is None:
            continue
        morsel._columns[idx] = new_vec
        morsel.ptr.columns[idx] = <void*>new_vec
        morsel.ptr.column_types[idx] = new_vec.dtype


class FilterNode(BasePlanNode):

    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.filter = parameters.get("filter")
        self.post_filter_columns = parameters.get("pre_update_columns")

        self.function_evaluations = get_all_nodes_of_type(
            self.filter,
            select_nodes=(NodeType.FUNCTION,),
        )

        self._const_replacements = _extract_constant_replacements(self.filter)

    @property
    def config(self):  # pragma: no cover
        return format_expression(self.filter)

    @property
    def name(self):  # pragma: no cover
        return "Filter"

    def execute(self, Morsel morsel):
        from opteryx.expression.evaluator import evaluate_draken

        if morsel is EOS:
            return

        mask = evaluate_draken(self.filter, morsel)
        filtered = morsel.filter_mask(mask)

        if self._const_replacements:
            _apply_constant_replacements(filtered, self._const_replacements)

        if self.post_filter_columns:
            keep = [c for c in filtered.column_names if c in self.post_filter_columns]
            if len(keep) < filtered.num_columns:
                filtered = filtered.select(keep)

        if filtered.num_rows > 0:
            yield filtered
        else:
            yield morsel.slice(0, 0)
