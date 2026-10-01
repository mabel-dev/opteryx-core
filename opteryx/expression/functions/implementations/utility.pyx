# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Utility function kernels.

Includes:
- Array operations: ARRAY_CAST
- JSON operations: JSONB_OBJECT_KEYS
- Random generation: RANDOM, RAND, NORMAL, RANDOM_STRING
- Statistics: GREATEST, LEAST
- Sorting: SORT
- Access: GET_STRING
- Text formatting: HUMANIZE
"""


def generate_series(*args):
    """GENERATE_SERIES(stop | start, stop [, step]) — NATIVE ONLY.

    This is a fail-loud guard, not an implementation. The scalar GENERATE_SERIES
    is executed by `draken_generate_series`
    (draken/ops/kernels/function_array_json.cpp), and an ARRAY-returning function
    is never constant-folded, so nothing should ever reach here.

    It exists because the catalog reads a missing `callable_ref` as "this function
    is rewrite-only, desugar it" — so a None would send GENERATE_SERIES to a
    rewrite that does not exist. Answering in Python instead would be a silent
    fallback, which this engine does not have.
    """
    raise NotImplementedError(
        "GENERATE_SERIES is executed by a native kernel (draken_generate_series). "
        "Reaching this Python guard means the call was not lowered to it — a bug, "
        "not a supported path."
    )


def jsonb_object_keys(arr):
    """JSONB_OBJECT_KEYS(json) — NATIVE ONLY.

    This is a fail-loud guard, not an implementation. JSONB_OBJECT_KEYS is
    executed by `draken_jsonb_object_keys`
    (draken/ops/kernels/function_array_json.cpp), and an ARRAY-returning function
    is never constant-folded, so nothing should ever reach here.

    It exists because the catalog reads a missing `callable_ref` as "this function
    is rewrite-only, desugar it" — so a None would send JSONB_OBJECT_KEYS to a
    rewrite that does not exist. Answering in Python instead would be a silent
    fallback, which this engine does not have.
    """
    raise NotImplementedError(
        "JSONB_OBJECT_KEYS is executed by a native kernel (draken_jsonb_object_keys). "
        "Reaching this Python guard means the call was not lowered to it — a bug, "
        "not a supported path."
    )


def humanize(arr):
    def format_number(num: float) -> str:
        return f"{num:,.0f}" if isinstance(num, int) else f"{num:,.1f}"

    def humanize_number(value: float) -> str:
        thresholds = [
            (1_000_000_000_000, "trillion"),
            (1_000_000_000, "billion"),
            (1_000_000, "million"),
            (1_000, "thousand"),
        ]
        for threshold, label in thresholds:
            rounded = round(value / threshold, 1)
            if rounded >= 0.9:
                return f"{format_number(rounded)} {label}"
        return format_number(value)

    return [humanize_number(value) for value in arr]


def array_cast(array, element_type):
    from opteryx.types.logical_type import LogicalCategory
    from opteryx.types.scalars.value_parsing import parser_for

    array = array.tolist()
    result = [None] * len(array)
    parser = parser_for(LogicalCategory[element_type[0]])
    for i, row in enumerate(array):
        row_res = []
        if row is not None:
            for element in row:
                if element is None:
                    continue
                row_res.append(parser(element))
            result[i] = row_res
    return result


def array_cast_safe(array, element_type):
    from contextlib import suppress

    from opteryx.types.logical_type import LogicalCategory
    from opteryx.types.scalars.value_parsing import parser_for

    result = [None] * len(array)
    parser = parser_for(LogicalCategory[element_type[0]])
    for i, row in enumerate(array):
        row_res = []
        with suppress(Exception):
            if row is not None:
                for element in row:
                    if element is None:
                        continue
                    value = parser(element)
                    row_res.append(value)
        result[i] = row_res
    return result
