# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Logical and control flow function kernels.

Includes:
- Null handling: COALESCE, IFNULL, IFNOTNULL, NULLIF
- Conditional logic: CASE, IIF
- Array membership: ARRAY_CONTAINS

Note: Binary logical operators (And, Or, Xor, Not) are handled as binary_operators and
logical operators respectively.
"""


def array_contains(array, item):
    """does array contain item"""
    if array is None:
        return False
    return item in set(array)


def null_if(col1, col2):
    """
    NULLIF is lowered to a native expression — IIF(col1 = col2, NULL, col1) — at
    plan-build time (see logical_planner_builders.function). It must never reach a
    Python kernel; this guard fails loud if the rewrite was bypassed rather than
    silently degrading to a row-wise Python implementation.
    """
    raise NotImplementedError(
        "NULLIF must be lowered to IIF(a = b, NULL, a) during planning; "
        "the null_if kernel should never be invoked."
    )
