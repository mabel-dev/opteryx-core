"""Expression evaluation engine — package marker.

The evaluator implementation is compiled into opteryx.operators._operators
(textually included alongside all operator plan nodes so they can call
bytecode VM functions directly at C level with no .so-boundary overhead).

This file re-exports the public API and registers legacy submodule aliases
(`opteryx.expression.evaluator.case_eval`, `.evaluation`, etc.) so callers
doing `from opteryx.expression.evaluator.LEAF import name` keep working.
"""

from opteryx.operators._operators import (
    apply_bounded_function,
    draken_compare,
    evaluate_and_append_draken,
    evaluate_draken,
    evaluate_bitmap,
    execute_bytecode,
    get_bytecode_worker_fn_ptr,
)
from opteryx.operators._operators import _OP_CODE, _verify_node_type_constants

# Legacy submodule aliases — every evaluator leaf .pyx is textually included
# in _operators, so all their names live in _operators's namespace.
import sys as _sys
from opteryx.operators import _operators as _operators_module

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
    _sys.modules[f"{__name__}.{_leaf}"] = _operators_module
    _sys.modules[f"{__name__}._impl"] = _operators_module  # backward compat
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
