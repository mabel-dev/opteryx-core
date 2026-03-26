"""
Array/misc registrar forwarder.

This module forwards the array/misc registrar getter to the merged utility module
so callers of the original `array_misc` module continue to work unchanged while
the actual implementations live in `registrar.utility`.
"""

from typing import List

from opteryx.expression.functions import FunctionDefinition
from opteryx.expression.functions.registrar.utility import (
    get_builtin_array_misc_functions as _utility_getter,
)


def get_builtin_array_misc_functions() -> List[FunctionDefinition]:
    """Return array/misc FunctionDefinition objects from the utility registrar."""
    return _utility_getter()


__all__ = ["get_builtin_array_misc_functions"]
