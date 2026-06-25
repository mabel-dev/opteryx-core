"""
Arithmetic function kernels.

Thin re-export layer from C++ nanobind implementations.
All computation logic lives in opteryx/compiled/nanobind/vector_math.cpp,
vector_string_misc.cpp, and vector_misc.cpp.

This module provides backward-compatible imports for the registrar.

Includes:
- Rounding: ROUND, FLOOR, CEILING, TRUNCATE
- Magnitude: ABS, SIGN, SQRT
- Exponentiation & logarithms: POWER, LOG
- Random: RANDOM, NORMAL, RANDOM_STRING
"""

from opteryx.compiled.nanobind.vectors import (
    round1,
    round2,
    ceiling,
    floor_dispatch as floor,
    trunc_dispatch as trunc,
    abs_value,
    sign_value,
    sqrt_value,
    random_number,
    random_normal,
    vector_power as safe_power,
)
from opteryx.compiled.nanobind.vectors import vector_log as log
from opteryx.compiled.nanobind.vectors import vector_random_strings as random_strings

__all__ = [
    "round1",
    "round2",
    "ceiling",
    "floor",
    "trunc",
    "abs_value",
    "sign_value",
    "sqrt_value",
    "random_number",
    "random_normal",
    "safe_power",
    "log",
    "random_strings",
]
