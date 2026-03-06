"""Expression evaluator: hotpath for function execution.

The evaluator executes bound function expressions with minimal dispatch overhead.
Binder attaches a function reference (e.g., "ADD" or "ADD:integer_integer") to each node,
and the evaluator simply looks up and invokes the kernel.
"""

from typing import Any
from typing import Optional

from opteryx.expression.functions import get_catalog

# TODO: Implement apply_function and related hotpath dispatch
# Example:
#
# def apply_function(node, args) -> Any:
#     """Apply a bound function to arguments.
#
#     Args:
#         node: AST node with .function_ref attribute (e.g., "add" or "add:integer_integer")
#         args: List of evaluated argument values
#
#     Returns:
#         Result of applying the kernel to args.
#     """
#     func_ref = node.function_ref
#     catalog = get_catalog()
#
#     if ":" in func_ref:
#         func_name, kernel_id = func_ref.split(":")
#         kernel = catalog.get_kernel(func_name, kernel_id)
#     else:
#         kernel = catalog.get_default_kernel(func_ref)
#
#     if kernel is None:
#         raise ValueError(f"Unknown function: {func_ref}")
#
#     return kernel(args)
