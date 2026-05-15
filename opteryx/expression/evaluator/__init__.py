"""Expression evaluation engine — package marker.

The real implementation is in `_impl.pyx`, compiled to `_impl.cpython-XXX.so`.
This Python file re-exports the public API and registers the legacy
submodule aliases (`opteryx.expression.evaluator.case_eval`,
`opteryx.expression.evaluator.evaluation`, etc.) so callers that do
`from opteryx.expression.evaluator.LEAF import name` keep working.

This indirection exists because Cython 3.x emits broken self-imports when
the compiled module is named with a `.__init__` suffix and uses typed
memoryviews. See _impl.pyx for the full story.
"""

from ._impl import (
    apply_bounded_function,
    draken_compare,
    evaluate_and_append_draken,
    evaluate_draken,
    execute_bytecode,
)
from ._impl import _OP_CODE, _verify_node_type_constants

# Legacy submodule aliases. Every leaf .pyx is textually included into
# _impl, so all their names live in _impl's namespace. Pointing each leaf
# alias at _impl makes `from opteryx.expression.evaluator.case_eval import
# evaluate_case` (and similar) resolve correctly.
import sys as _sys
from . import _impl as _impl_module

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
    "evaluate_draken",
    "execute_bytecode",
]
