# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Utility function kernels.

Includes:
- Array operations: ARRAY_CONTAINS, ARRAY_CONTAINS_ANY, ARRAY_CONTAINS_ALL
- JSON operations: JSONB_OBJECT_KEYS
- Random generation: RANDOM, RAND, NORMAL, RANDOM_STRING
- Statistics: GREATEST, LEAST
- Sorting: SORT
- Access: GET_STRING
- Text formatting: HUMANIZE
- Vector operations: COSINE_SIMILARITY
"""

import numpy
import pyarrow

from opteryx.third_party.tktech import csimdjson as simdjson


def cosine_similarity(arr, val):
    """ad hoc cosine similarity function, slow."""
    from opteryx.compiled.functions.vectors import tokenize_and_remove_punctuation
    from opteryx.compiled.functions.vectors import vectorize
    from opteryx.virtual_datasets.stop_words import STOP_WORDS

    def _cosine_similarity(
        vec1: numpy.ndarray, vec2: numpy.ndarray, vec2_norm: numpy.float32
    ) -> float:
        vec1 = vec1.astype(numpy.float32)
        vec1_norm = numpy.linalg.norm(vec1)
        product = vec1_norm * vec2_norm
        if product == 0:
            return 0
        return numpy.dot(vec1, vec2) / product

    if len(val) == 0:
        return []
    literal = val[0]
    if isinstance(literal, bytes):
        literal = literal.decode("utf8", errors="ignore")
    tokenized_literal = tokenize_and_remove_punctuation(str(literal), STOP_WORDS)
    if len(tokenized_literal) == 0:
        return [0.0] * len(arr)

    def _to_text(value):
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode("utf8", errors="ignore")
        return str(value)

    tokenized_strings = [tokenize_and_remove_punctuation(_to_text(s), STOP_WORDS) for s in arr] + [
        tokenized_literal
    ]
    vectors = [vectorize(tokens) for tokens in tokenized_strings]
    comparison_vector = vectors[-1].astype(numpy.float32)
    comparison_vector_norm = numpy.linalg.norm(comparison_vector)

    if comparison_vector_norm == 0.0:
        return [0.0] * len(val)

    return [
        _cosine_similarity(vector, comparison_vector, comparison_vector_norm)
        for vector in vectors[:-1]
    ]


def jsonb_object_keys(arr: numpy.ndarray):
    """
    Extract the keys from a NumPy array of JSON objects or JSON strings/bytes.
    """
    if len(arr) == 0:
        return numpy.array([])

    if isinstance(arr, pyarrow.Array):
        arr = arr.to_numpy(zero_copy_only=False)

    result = numpy.empty(arr.shape, dtype=list)

    if isinstance(arr[0], dict):
        for i, row in enumerate(arr):
            result[i] = [str(key) for key in row.keys()]  # noqa: SIM118
    elif isinstance(arr[0], (str, bytes)):
        parser = simdjson.Parser()
        for i, row in enumerate(arr):
            result[i] = [str(key) for key in parser.parse(row).keys()]  # noqa: SIM118
    else:
        raise ValueError("Unsupported dtype for array elements. Expected dict, str, or bytes.")

    return result


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

    if hasattr(arr, "to_numpy"):
        arr = arr.to_numpy(zero_copy_only=False)
    if hasattr(arr, "tolist"):
        arr = arr.tolist()

    return [humanize_number(value) for value in arr]


def array_cast(array, element_type):
    from orso.types import OrsoTypes

    result = numpy.empty(len(array), dtype=list)
    parser = OrsoTypes[element_type[0]].parse
    if hasattr(array, "to_numpy"):
        array = array.to_numpy(zero_copy_only=False)
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

    from orso.types import OrsoTypes

    result = numpy.empty(len(array), dtype=list)
    parser = OrsoTypes[element_type[0]].parse
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
