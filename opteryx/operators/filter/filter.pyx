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
Filter Node — a SQL Query Execution Plan Node.

WHERE execution on the native engine path is 100% native (src/cpp/engine/engine.hpp's
ExprFilterOperator — see opteryx/managers/execution/compiler.py's FilterNode
branch, which reads `.filter`/`._const_replacements` off this class and compiles
them straight into the engine); FilterNode.push is never called there.

The push-pipeline fallback (opteryx/managers/execution/pipeline_compiler.py,
used by EXPLAIN ANALYZE and INSERT INTO ... SELECT) DOES drive this class's
`_push_impl`, so it must apply the predicate for real. It reuses the exact same
bytecode compiler and executor the native path and ParquetReadNode's residual
filter use (`_Compiler._lower_bytecode`, `filter_morsel_c_native`,
`execute_bytecode`) — no second predicate evaluator.
"""

from opteryx.expression import NodeType
from opteryx.expression import format_expression
from opteryx.compiled.expression.compiled_expression cimport CompiledBytecode
from opteryx.expression.evaluator.evaluation import execute_bytecode, filter_morsel_c_native

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


cdef class FilterNode(BasePlanNode):
    cdef public object filter
    cdef public object post_filter_columns
    cdef public list _const_replacements
    cdef public CompiledBytecode _compiled_predicate

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.filter = parameters.get("filter")
        self.post_filter_columns = parameters.get("pre_update_columns")
        self._const_replacements = _extract_constant_replacements(self.filter)
        # Compiled lazily (first push) — the push pipeline is the only caller
        # that needs this; the native engine path never touches it.
        self._compiled_predicate = None

    @property
    def config(self):  # pragma: no cover
        return format_expression(self.filter)

    @property
    def name(self):  # pragma: no cover
        return "Filter"

    cdef CompiledBytecode _compile_predicate(self):
        # Same rewrite chain the native engine compiler runs before handing a
        # predicate to add_expr_filter (CASE→IF_THEN_ELSE, BETWEEN→compares,
        # decimal-literal rescale) — reused via _Compiler so this fallback path
        # and the native path lower identical bytecode from identical trees.
        from opteryx.managers.execution.compiler import _Compiler

        return _Compiler(None, None)._lower_bytecode(self.filter)

    cpdef void _push_impl(self, Morsel morsel) except *:
        # Body runs GIL-held: the base nogil `_dispatch_push` decodes the C++
        # carrier (recovering the EOS sentinel) and calls this; it catches any
        # exception and surfaces it via the ErrCtx status path.
        if morsel is _EOS_SENTINEL:
            self.emit(morsel)
            return

        if morsel.num_rows == 0:
            return

        if self._compiled_predicate is None:
            self._compiled_predicate = self._compile_predicate()

        cdef object filtered = filter_morsel_c_native(self._compiled_predicate, morsel)
        if filtered is None:
            filtered = morsel.filter_mask(
                execute_bytecode(self._compiled_predicate, morsel)
            )
        self.emit(filtered)
