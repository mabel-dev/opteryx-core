# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

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
from opteryx.compiled.expression.compiled_expression import build_bytecode as _build_bytecode
from opteryx.compiled.expression.compiled_expression import lower as _lower_expr
from opteryx.expression import NodeType
from opteryx.expression import format_expression
from opteryx.expression import get_all_nodes_of_type
from opteryx.expression.evaluator import execute_bytecode as _execute_bytecode
from opteryx.expression.evaluator.evaluation import evaluate_c_native_cxx as _evaluate_c_native_cxx
from opteryx.models import QueryProperties

from opteryx.compiled.expression.compiled_expression cimport CompiledBytecode
from draken.vectors.vector cimport Vector
from draken.core.buffers cimport DrakenVector

# BasePlanNode is defined at the top of _operators.pyx (the umbrella unit) and
# is in scope here via textual include.


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

    Wraps the nanobind result in a Cython-shim Vector so callers that
    declare `cdef Vector new_vec` and downstream code that does
    `morsel._get_column(idx)` see a consistent type.
    """
    cdef DrakenType t = cur.unified().type
    if t == DRAKEN_BOOL:
        if not isinstance(value, bool):
            return None
        return Vector(_draken_native.vector_from_bool_constant(value, length))
    if t == DRAKEN_INT64:
        if isinstance(value, bool) or not isinstance(value, int):
            return None
        return Vector(_draken_native.vector_from_constant(value, length))
    if t == DRAKEN_FLOAT64:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        return Vector(_draken_native.vector_float64_from_constant(float(value), length))
    if t == DRAKEN_VARCHAR or t == DRAKEN_NVARCHAR:
        if not isinstance(value, (str, bytes)):
            return None
        # The string edge is bytes-only — encode str to bytes (str must not reach
        # the Draken edge). Bytes are stored verbatim (no decode).
        if isinstance(value, str):
            value = value.encode("utf-8")
        if t == DRAKEN_NVARCHAR:
            return Vector(_draken_native.vector_nvarchar_from_constant(value, length))
        return Vector(_draken_native.vector_varchar_from_constant(value, length))
    if t == DRAKEN_VARBINARY:
        if not isinstance(value, (str, bytes)):
            return None
        if isinstance(value, str):
            value = value.encode()
        return Vector(_draken_native.vector_varbinary_from_constant(value, length))
    return None


cdef void _apply_constant_replacements(Morsel morsel, list replacements) except *:
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
        cur = morsel._get_column(idx)
        if cur is None:
            continue
        new_vec = _build_constant_vector(cur, value, length)
        if new_vec is None:
            continue
        morsel._set_column(idx, new_vec)


cdef class FilterNode(BasePlanNode):
    cdef public object filter
    cdef public object post_filter_columns
    cdef public list function_evaluations
    cdef public list _const_replacements
    cdef CompiledBytecode _compiled_filter

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.filter = parameters.get("filter")
        self.post_filter_columns = parameters.get("pre_update_columns")

        self.function_evaluations = get_all_nodes_of_type(
            self.filter,
            select_nodes=(NodeType.FUNCTION,),
        )

        self._const_replacements = _extract_constant_replacements(self.filter)

        # Lower the filter predicate to a C++ arena and linearise it into a
        # typed CompiledBytecode at bind time.  Every morsel iterates a C
        # struct array — no Python Node tree traversal at execute time.
        if self.filter is not None:
            self._compiled_filter = _build_bytecode(_lower_expr(self.filter))
        else:
            self._compiled_filter = None

    @property
    def config(self):  # pragma: no cover
        return format_expression(self.filter)

    @property
    def name(self):  # pragma: no cover
        return "Filter"

    cdef void _dispatch_push(self, Morsel morsel) except *:
        cdef BoolVector mask
        cdef Morsel filtered
        cdef list keep
        if morsel is _EOS_SENTINEL:
            self._emit_cdef(morsel)
            return

        # S3: for all-C-native predicates, evaluate over the CxxMorsel substrate
        # (columns straight from columns[idx].view, nogil inner) — no per-column
        # Vector build. Other predicates stay on the Morsel VM path.
        if self._compiled_filter.is_all_c_native:
            mask = _evaluate_c_native_cxx(self._compiled_filter, morsel)
        else:
            mask = _execute_bytecode(self._compiled_filter, morsel)
        filtered = morsel.filter_mask(mask)

        if self._const_replacements:
            _apply_constant_replacements(filtered, self._const_replacements)

        if self.post_filter_columns:
            keep = [c for c in filtered.column_names if c in self.post_filter_columns]
            if len(keep) < filtered.num_columns:
                filtered = filtered.select(keep)

        if filtered.num_rows > 0:
            self._emit_cdef(filtered)
        # Empty-output filters: do nothing (drop the morsel). Previous code
        # emitted morsel.slice(0,0); under push semantics EMPTY-like outputs
        # are suppressed and the downstream sees fewer morsels.
