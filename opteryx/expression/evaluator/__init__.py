"""Expression evaluation engine — package marker.

The evaluator implementation is compiled into _impl.so (this package).
Leaf .pyx files (type_coercion / function_execution / temporal_ops /
string_ops / json_ops / case_eval / arithmetic / comparisons / evaluation)
are textually included by _impl.pyx into a single extension module.
Phase 6: arithmetic_dispatch deleted — dispatch now at bind time via resolve_binary_op.

This file re-exports the public API and registers legacy submodule aliases
(`opteryx.expression.evaluator.case_eval`, `.evaluation`, etc.) so callers
doing `from opteryx.expression.evaluator.LEAF import name` keep working.
"""

from opteryx.expression.evaluator._impl import (
    draken_compare_int,
    evaluate_bitmap,
    execute_and_append,
    execute_bytecode,
    get_bytecode_worker_fn_ptr,
)
from opteryx.expression.evaluator._impl import _OP_CODE, _verify_node_type_constants


def compile_eval_nodes(nodes):
    """Compile expression nodes to (identity, CompiledBytecode) at bind time.

    Applies should_evaluate filtering, _PASSTHRU exclusion, and
    prioritize_evaluation ordering.  The result is a list of
    (identity_str, CompiledBytecode) pairs ready for execute_and_append().
    """
    from opteryx.expression import should_evaluate, prioritize_evaluation
    from opteryx.compiled.expression.compiled_expression import (
        lower as _lower,
        build_bytecode as _build_bc,
    )

    filtered = [n for n in nodes if n.value != "_PASSTHRU" and should_evaluate(n)]
    ordered = list(prioritize_evaluation(filtered))
    return [(n.schema_column.identity, _build_bc(_lower(n))) for n in ordered]

# Legacy submodule aliases — every evaluator leaf .pyx is textually included
# in _impl, so all their names live in _impl's namespace.
import sys as _sys
import opteryx.expression.evaluator._impl as _impl_module

for _leaf in (
    "arithmetic",
    "case_eval",
    "comparisons",
    "evaluation",
    "function_execution",
    "json_ops",
    "string_ops",
    "temporal_ops",
    "type_coercion",
):
    _sys.modules[f"{__name__}.{_leaf}"] = _impl_module
del _leaf


__all__ = [
    "compile_eval_nodes",
    "draken_compare_int",
    "evaluate_bitmap",
    "execute_and_append",
    "execute_bytecode",
    "get_bytecode_worker_fn_ptr",
]
