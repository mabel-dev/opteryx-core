"""
Aggregate registrar module.

Aggregates in Opteryx are dispatched by the physical aggregate operators via
opteryx.operators.aggregate_helpers.AGGREGATORS rather than through the function
catalog. To keep the registrar package complete and discoverable we provide this
module, but it intentionally exposes no runtime FunctionDefinition objects.

Returning an empty list ensures aggregate names are not treated as regular
builtin functions by name-based checks in the binder/planner while keeping the
module structure consistent for future work.
"""

from typing import List

from opteryx.expression.functions import FunctionDefinition  # type: ignore


def get_builtin_aggregate_functions() -> List[FunctionDefinition]:
    """
    Aggregates are not registered via the function catalog.

    This function returns an empty list to indicate there are no builtin
    aggregate FunctionDefinition objects to load into the catalog.
    """
    return []
