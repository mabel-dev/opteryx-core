# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Arithmetic function kernels.

Includes:
- Rounding: ROUND, FLOOR, CEILING, TRUNCATE
- Magnitude: ABS, SIGN, SQRT
- Exponentiation & logarithms: POWER, LOG
- Random: RANDOM, NORMAL, RANDOM_STRING

Note: Binary arithmetic operators (Plus, Minus, Multiply, Divide, Modulo) are handled as
binary_operators.
"""

from typing import List

import numpy
from pyarrow import compute

_DRAKEN_ENCODING_CONSTANT = 3


def _is_constant_like(value) -> bool:
    return getattr(value, "encoding", None) == _DRAKEN_ENCODING_CONSTANT


def _constant_scalar(value):
    if getattr(value, "encoding", None) == _DRAKEN_ENCODING_CONSTANT:
        if len(value) == 0:
            return None
        return value[0]
    return value


def round1(values):
    """ROUND(values)"""
    from opteryx.compiled.vector_ops import vector_round
    from opteryx.compiled.vector_ops import vector_round_constant

    if _is_constant_like(values):
        return vector_round_constant(values, 0)
    return vector_round(values)


def round2(values, digits):
    """ROUND(values, digits)"""
    from opteryx.compiled.vector_ops import vector_round_constant
    from opteryx.compiled.vector_ops import vector_round_digits

    if _is_constant_like(digits):
        scalar = _constant_scalar(digits)
        d = int(scalar) if scalar is not None else 0
    else:
        d = int(digits)

    if _is_constant_like(values):
        return vector_round_constant(values, d)

    return vector_round_digits(values, d)


def random_number(size):
    return numpy.random.uniform(size=size)


def random_normal(size):
    from numpy.random import default_rng

    rng = default_rng(831835)  # 8 days, 3 hours, 18 minutes, 35 seconds
    return rng.standard_normal(size)


def random_strings(items):
    if isinstance(items, int):
        row_count = items
        width = 16
    elif len(items) > 0:
        row_count = len(items)
        width = items[0]
    else:
        return []

    from opteryx.compiled.vector_ops import vector_random_strings

    return vector_random_strings(row_count, width)


def safe_power(base_array, exponent_array):
    """
    Wrapper around pyarrow's compute.power function.
    If both base and exponent arrays are of int type, the result will be int.
    Otherwise, it'll return a float.
    """
    if len(numpy.unique(exponent_array)) != 1:
        raise ValueError("The exponent_array should have all identical values.")

    single_exponent = exponent_array[0]

    if base_array.dtype.kind == "i" and exponent_array.dtype.kind == "i" and single_exponent >= 0:
        result = compute.power(base_array, exponent_array)
    else:
        result = compute.power(base_array.astype(numpy.float64), exponent_array)

    return result


def log(values, bases):
    from opteryx.compiled.vector_ops import vector_log

    return vector_log(values, bases)


def ceiling(values, scales=None) -> List:
    """Performs a 'ceiling' with a scale factor."""
    if scales is None:
        scale = 0
    elif len(scales) == 0:
        return []
    else:
        scale = scales[0]
    if scale == 0:
        return numpy.ceil(values)

    if scale > 0:
        scale_factor = 10**scale
        return numpy.ceil(values * scale_factor) / scale_factor
    else:
        scale_factor = 10 ** (-scale)
        return numpy.ceil(values / scale_factor) * scale_factor


def floor(values, scales=None) -> List:
    """Performs a 'floor' with a scale factor."""
    if scales is None:
        scale = 0
    elif len(scales) == 0:
        return []
    else:
        scale = scales[0]
    if scale == 0:
        return numpy.floor(values)

    if scale > 0:
        scale_factor = 10**scale
        return numpy.floor(values * scale_factor) / scale_factor
    else:
        scale_factor = 10 ** (-scale)
        return numpy.floor(values / scale_factor) * scale_factor


def trunc(values, scales=None) -> List:
    """Performs a 'trunc' (truncate towards zero) with a scale factor."""
    if scales is None:
        scale = 0
    elif len(scales) == 0:
        return []
    else:
        scale = scales[0]
    if scale == 0:
        return numpy.trunc(values)

    if scale > 0:
        scale_factor = 10**scale
        return numpy.trunc(values * scale_factor) / scale_factor
    else:
        scale_factor = 10 ** (-scale)
        return numpy.trunc(values / scale_factor) * scale_factor
