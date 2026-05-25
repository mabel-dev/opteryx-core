"""Expression evaluation engine — package marker.

The evaluator implementation is compiled into _impl.so (this package).
Leaf .pyx files (type_coercion / function_execution / arithmetic_dispatch /
temporal_ops / string_ops / json_ops / case_eval / arithmetic / comparisons /
evaluation) are textually included by _impl.pyx into a single extension module.

This file re-exports the public API and registers legacy submodule aliases
(`opteryx.expression.evaluator.case_eval`, `.evaluation`, etc.) so callers
doing `from opteryx.expression.evaluator.LEAF import name` keep working.
"""

from opteryx.expression.evaluator._impl import (
    apply_bounded_function,
    draken_compare,
    evaluate_and_append_draken,
    evaluate_draken,
    evaluate_bitmap,
    execute_bytecode,
    get_bytecode_worker_fn_ptr,
)
from opteryx.expression.evaluator._impl import _OP_CODE, _verify_node_type_constants

# Legacy submodule aliases — every evaluator leaf .pyx is textually included
# in _impl, so all their names live in _impl's namespace.
import sys as _sys
import opteryx.expression.evaluator._impl as _impl_module

for _leaf in (
    "arithmetic",
    "arithmetic_dispatch",
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
    "apply_bounded_function",
    "draken_compare",
    "evaluate_and_append_draken",
    "evaluate_bitmap",
    "evaluate_draken",
    "execute_bytecode",
    "get_bytecode_worker_fn_ptr",
]
