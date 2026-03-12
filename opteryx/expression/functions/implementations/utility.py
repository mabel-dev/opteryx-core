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
- Vector operations: COSINE_SIMILARITY, COSINE_DISTANCE
"""

import numpy
import pyarrow

from opteryx.embeddings import embed_text_values
from opteryx.third_party.tktech import csimdjson as simdjson


def _sequence_rows(values):
    if isinstance(values, (str, bytes, bytearray)):
        return [values]
    if isinstance(values, pyarrow.Array):
        return values.to_pylist()
    if isinstance(values, numpy.ndarray):
        return values.tolist()
    if isinstance(values, (list, tuple)):
        return list(values)
    return list(values)


def _as_python_value(value):
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, pyarrow.Array):
        return value.to_pylist()
    return value


def _coerce_numeric_vector(value):
    value = _as_python_value(value)
    if value is None:
        return None
    if isinstance(value, numpy.ndarray):
        if value.ndim != 1:
            return None
        if value.dtype.kind not in {"b", "i", "u", "f"}:
            try:
                value = value.astype(numpy.float32)
            except (TypeError, ValueError):
                return None
        return numpy.asarray(value, dtype=numpy.float32)
    if isinstance(value, (list, tuple)):
        try:
            return numpy.asarray(value, dtype=numpy.float32)
        except (TypeError, ValueError):
            return None
    return None


def _coerce_text_scalar(value):
    value = _as_python_value(value)
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf8", errors="ignore")
    return str(value)


def _score_numeric_vectors(left_rows, right_rows):
    if len(right_rows) == 0:
        return []

    query_vector = _coerce_numeric_vector(right_rows[0])
    if len(right_rows) == 1 and query_vector is not None and query_vector.size > 0:
        dense_vectors = numpy.zeros((len(left_rows), query_vector.size), dtype=numpy.float32)
        for index, row in enumerate(left_rows):
            vector = _coerce_numeric_vector(row)
            if vector is None or vector.size != query_vector.size:
                continue
            dense_vectors[index, :] = vector

        try:
            from opteryx.nanobind import vector_search

            scores = numpy.asarray(vector_search.score_cosine(query_vector, dense_vectors), dtype=numpy.float32)
        except (ImportError, ValueError):
            scores = numpy.zeros(len(left_rows), dtype=numpy.float32)
            query_norm = numpy.linalg.norm(query_vector)
            if query_norm == 0.0:
                return scores.tolist()

            for index, row in enumerate(dense_vectors):
                row_norm = numpy.linalg.norm(row)
                if row_norm == 0.0:
                    continue
                scores[index] = numpy.dot(row, query_vector) / (row_norm * query_norm)

        scores = numpy.where(numpy.isfinite(scores), scores, 0.0)
        return scores.tolist()

    if len(right_rows) != len(left_rows):
        return [0.0] * len(left_rows)

    scores = numpy.zeros(len(left_rows), dtype=numpy.float32)
    for index, (left_row, right_row) in enumerate(zip(left_rows, right_rows, strict=True)):
        left_vector = _coerce_numeric_vector(left_row)
        right_vector = _coerce_numeric_vector(right_row)
        if left_vector is None or right_vector is None or left_vector.size != right_vector.size:
            continue
        left_norm = numpy.linalg.norm(left_vector)
        right_norm = numpy.linalg.norm(right_vector)
        if left_norm == 0.0 or right_norm == 0.0:
            continue
        scores[index] = numpy.dot(left_vector, right_vector) / (left_norm * right_norm)
    return scores.tolist()


def _cosine_similarity_text(arr, val):
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


def cosine_similarity(arr, val):
    """Cosine similarity over numeric vectors or the legacy lexical text path."""
    left_rows = _sequence_rows(arr)
    right_rows = _sequence_rows(val)

    if len(left_rows) == 0:
        return []

    sample_left = next((row for row in left_rows if row is not None), None)
    sample_right = next((row for row in right_rows if row is not None), None)
    if _coerce_numeric_vector(sample_left) is not None and _coerce_numeric_vector(sample_right) is not None:
        return _score_numeric_vectors(left_rows, right_rows)

    return _cosine_similarity_text(arr, val)


def cosine_distance(arr, val):
    """Cosine distance for numeric vectors, returned as 1 - cosine_similarity."""
    scores = numpy.asarray(cosine_similarity(arr, val), dtype=numpy.float32)
    if scores.size == 0:
        return []
    return (1.0 - numpy.clip(scores, -1.0, 1.0)).tolist()


def embed(arr):
    """Convert text values into numeric vectors using the configured embedding provider."""
    rows = _sequence_rows(arr)
    if len(rows) == 0:
        return []

    texts = []
    row_positions = []
    results = [None] * len(rows)
    for index, value in enumerate(rows):
        text_value = _coerce_text_scalar(value)
        if text_value is None:
            continue
        texts.append(text_value)
        row_positions.append(index)

    if not texts:
        return results

    embedded = embed_text_values(texts)
    for index, vector in zip(row_positions, embedded, strict=True):
        results[index] = vector
    return results


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
