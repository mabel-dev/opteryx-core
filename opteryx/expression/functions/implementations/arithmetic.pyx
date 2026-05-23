# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Arithmetic function kernels.

Includes:
- Rounding: ROUND, FLOOR, CEILING, TRUNCATE
- Magnitude: ABS, SIGN, SQRT
- Exponentiation & logarithms: POWER, LOG
- Random: RANDOM, NORMAL, RANDOM_STRING

Note: Binary arithmetic operators (Plus, Minus, Multiply, Divide, Modulo) are handled as
binary_operators.
"""

from typing import List

from draken.vectors.vector import Vector


def round1(values):
    """ROUND(values)"""
    from opteryx.compiled.vector_ops import vector_round

    return vector_round(values)


def round2(values, digits):
    """ROUND(values, digits)"""
    from opteryx.compiled.vector_ops import vector_round_digits

    if isinstance(digits, Vector) and len(digits) > 0:
        d = int(digits[0]) if digits[0] is not None else 0
    else:
        d = int(digits)

    return vector_round_digits(values, d)


def random_number(size):
    from opteryx.compiled.vector_ops import vector_random

    return vector_random(size)


def random_normal(size):
    from opteryx.compiled.vector_ops import vector_random_normal

    return vector_random_normal(size)


def random_strings(items):
    if isinstance(items, int):
        row_count = items
        width = 16
    elif len(items) > 0:
        row_count = len(items)
        width = items[0]
    else:
        return []

    from opteryx.compiled.nanobind.vector_string_misc import vector_random_strings

    return vector_random_strings(row_count, width)


def safe_power(base_array, exponent_array):
    """
    Element-wise POWER using the Draken vector_power kernel.
    The exponent must be a constant (all identical values); the scalar
    exponent is extracted and passed directly to the C kernel.
    """
    from opteryx.compiled.vector_ops import vector_power

    # Validate: all exponents must be the same scalar value.
    exp_values = exponent_array.to_pylist()
    unique_exps = set(v for v in exp_values if v is not None)
    if len(unique_exps) != 1:
        raise ValueError("safe_power: exponent_array must contain identical values.")

    exponent = float(unique_exps.pop())
    return vector_power(base_array, exponent)


def log(values, bases):
    from opteryx.compiled.nanobind.vector_misc import vector_log

    return vector_log(values, bases)


def ceiling(values, scales=None) -> List:
    """Performs a 'ceiling' with a scale factor."""
    from opteryx.compiled.vector_ops import vector_ceil

    if scales is None or len(scales) == 0:
        scale = 0
    else:
        scale = int(scales[0]) if scales[0] is not None else 0

    return vector_ceil(values, scale)


def floor(values, scales=None) -> List:
    """Performs a 'floor' with a scale factor."""
    from opteryx.compiled.vector_ops import vector_floor

    if scales is None or len(scales) == 0:
        scale = 0
    else:
        scale = int(scales[0]) if scales[0] is not None else 0

    return vector_floor(values, scale)


def trunc(values, scales=None) -> List:
    """Performs a 'trunc' (truncate towards zero) with a scale factor."""
    from opteryx.compiled.vector_ops import vector_trunc

    if scales is None or len(scales) == 0:
        scale = 0
    else:
        scale = int(scales[0]) if scales[0] is not None else 0

    return vector_trunc(values, scale)


def abs_value(values):
    """ABS(values): element-wise absolute value."""
    from opteryx.compiled.vector_ops import vector_abs
    return vector_abs(values)


def sqrt_value(values):
    """SQRT(values): element-wise square root."""
    from opteryx.compiled.vector_ops import vector_sqrt
    return vector_sqrt(values)


def sign_value(values):
    """SIGN(values): sign of value (-1, 0, 1)."""
    from opteryx.compiled.vector_ops import vector_sign
    return vector_sign(values)
