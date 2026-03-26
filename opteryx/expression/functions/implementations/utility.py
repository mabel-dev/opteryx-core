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
from opteryx.embeddings import embed_text_matrix
from opteryx.embeddings import embed_text_values
from opteryx.embeddings import get_embedding_provider
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


def _normalize_array_row(value):
    value = _as_python_value(value)
    if value is None:
        return None
    if isinstance(value, numpy.ndarray):
        if value.ndim == 0:
            return [value.item()]
        return value.tolist()
    if isinstance(value, (list, tuple, set, frozenset)):
        return list(value)
    return [value]


def _normalize_membership_values(value):
    value = _as_python_value(value)
    if value is None:
        return []
    if isinstance(value, numpy.ndarray):
        if value.ndim == 0:
            return [value.item()]
        value = value.tolist()
    if isinstance(value, (list, tuple, set, frozenset)):
        if len(value) == 1 and isinstance(
            next(iter(value)), (list, tuple, set, frozenset, numpy.ndarray)
        ):
            return _normalize_array_row(next(iter(value))) or []
        return list(value)
    return [value]


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


def _as_text_vector(values):
    from opteryx.compiled.draken.interop.arrow import vector_from_arrow
    from opteryx.compiled.draken.vectors.string_vector import StringVector

    if isinstance(values, StringVector):
        return values
    if hasattr(values, "to_arrow"):
        values = values.to_arrow()
    elif isinstance(values, (numpy.ndarray, list, tuple)):
        values = pyarrow.array(values)
    elif not isinstance(values, pyarrow.Array):
        return None

    vector = vector_from_arrow(values)
    return vector if isinstance(vector, StringVector) else None


def _coerce_numeric_matrix(rows, width=None):
    dense_rows = []
    valid_positions = []

    for index, row in enumerate(rows):
        vector = _coerce_numeric_vector(row)
        if vector is None or vector.size == 0:
            continue
        if width is None:
            width = vector.size
        if vector.size != width:
            continue
        dense_rows.append(vector)
        valid_positions.append(index)

    if width is None:
        width = 0

    if not dense_rows:
        return numpy.empty((0, width), dtype=numpy.float32), numpy.empty(0, dtype=numpy.int64)

    return (
        numpy.vstack(dense_rows).astype(numpy.float32, copy=False),
        numpy.asarray(valid_positions, dtype=numpy.int64),
    )


def _coerce_aligned_numeric_matrices(left_rows, right_rows):
    left_dense_rows = []
    right_dense_rows = []
    valid_positions = []
    width = None

    for index, (left_row, right_row) in enumerate(zip(left_rows, right_rows, strict=True)):
        left_vector = _coerce_numeric_vector(left_row)
        right_vector = _coerce_numeric_vector(right_row)
        if left_vector is None or right_vector is None:
            continue
        if left_vector.size == 0 or right_vector.size == 0 or left_vector.size != right_vector.size:
            continue
        if width is None:
            width = left_vector.size
        if left_vector.size != width or right_vector.size != width:
            continue
        left_dense_rows.append(left_vector)
        right_dense_rows.append(right_vector)
        valid_positions.append(index)

    if width is None:
        width = 0

    if not valid_positions:
        empty = numpy.empty((0, width), dtype=numpy.float32)
        return empty, empty.copy(), numpy.empty(0, dtype=numpy.int64)

    return (
        numpy.vstack(left_dense_rows).astype(numpy.float32, copy=False),
        numpy.vstack(right_dense_rows).astype(numpy.float32, copy=False),
        numpy.asarray(valid_positions, dtype=numpy.int64),
    )


def _score_numeric_vectors(left_rows, right_rows):
    if len(right_rows) == 0:
        return []

    query_vector = _coerce_numeric_vector(right_rows[0])
    if len(right_rows) == 1 and query_vector is not None and query_vector.size > 0:
        dense_vectors, valid_positions = _coerce_numeric_matrix(left_rows, query_vector.size)
        scores = numpy.zeros(len(left_rows), dtype=numpy.float32)
        if dense_vectors.shape[0] == 0:
            return scores.tolist()

        try:
            from opteryx.compiled.nanobind import vector_search

            valid_scores = numpy.asarray(
                vector_search.score_cosine(query_vector, dense_vectors), dtype=numpy.float32
            )
        except (ImportError, ValueError):
            query_norm = numpy.linalg.norm(query_vector)
            if query_norm == 0.0:
                return scores.tolist()

            valid_scores = numpy.zeros(dense_vectors.shape[0], dtype=numpy.float32)
            row_norms = numpy.linalg.norm(dense_vectors, axis=1)
            valid_mask = row_norms != 0.0
            if numpy.any(valid_mask):
                valid_scores[valid_mask] = (dense_vectors[valid_mask] @ query_vector) / (
                    row_norms[valid_mask] * query_norm
                )

        valid_scores = numpy.where(numpy.isfinite(valid_scores), valid_scores, 0.0)
        scores[valid_positions] = valid_scores
        return scores.tolist()

    if len(right_rows) != len(left_rows):
        return [0.0] * len(left_rows)

    left_vectors, right_vectors, valid_positions = _coerce_aligned_numeric_matrices(
        left_rows, right_rows
    )
    scores = numpy.zeros(len(left_rows), dtype=numpy.float32)
    if valid_positions.size == 0:
        return scores.tolist()

    left_norms = numpy.linalg.norm(left_vectors, axis=1)
    right_norms = numpy.linalg.norm(right_vectors, axis=1)
    valid_mask = (left_norms != 0.0) & (right_norms != 0.0)
    if numpy.any(valid_mask):
        numerators = numpy.einsum("ij,ij->i", left_vectors[valid_mask], right_vectors[valid_mask])
        scores[valid_positions[valid_mask]] = numerators / (
            left_norms[valid_mask] * right_norms[valid_mask]
        )
    return scores.tolist()


def _cosine_similarity_text(arr, val):
    if len(val) == 0:
        return []

    literal = _coerce_text_scalar(val[0])
    if literal is None:
        return [0.0] * len(arr)
    query_text = literal.strip()
    if not query_text:
        return [0.0] * len(arr)

    provider = get_embedding_provider()
    if provider is not None and getattr(provider, "prefer_score_string_vector", False):
        text_vector = _as_text_vector(arr)
        scorer = getattr(provider, "score_string_vector", None)
        if text_vector is not None and scorer is not None:
            positions, scores = scorer(query_text, text_vector)
            positions = numpy.asarray(positions, dtype=numpy.int64)
            scores = numpy.asarray(scores, dtype=numpy.float32)
            result = numpy.zeros(len(arr), dtype=numpy.float32)
            if positions.ndim == 1 and scores.ndim == 1 and positions.shape[0] == scores.shape[0]:
                valid = (positions >= 0) & (positions < len(arr))
                result[positions[valid]] = numpy.where(
                    numpy.isfinite(scores[valid]), scores[valid], 0.0
                )
            return result.tolist()

    result = [0.0] * len(arr)
    active_positions = []
    active_texts = []
    for index, value in enumerate(arr):
        text = _coerce_text_scalar(value)
        if text is None:
            continue
        text = text.strip()
        if not text:
            continue
        active_positions.append(index)
        active_texts.append(text)

    if not active_texts:
        return result

    embedded = embed_text_matrix([query_text, *active_texts])
    query_vector = embedded[0]
    row_vectors = embedded[1:]

    try:
        from opteryx.compiled.nanobind import vector_search

        scores = numpy.asarray(
            vector_search.score_cosine(query_vector, row_vectors), dtype=numpy.float32
        )
    except (ImportError, ValueError):
        scores = numpy.zeros(len(active_texts), dtype=numpy.float32)
        query_norm = numpy.linalg.norm(query_vector)
        if query_norm != 0.0:
            row_norms = numpy.linalg.norm(row_vectors, axis=1)
            valid_mask = row_norms != 0.0
            if numpy.any(valid_mask):
                scores[valid_mask] = numpy.dot(row_vectors[valid_mask], query_vector) / (
                    row_norms[valid_mask] * query_norm
                )

    scores = numpy.where(numpy.isfinite(scores), scores, 0.0)
    for index, score in zip(active_positions, scores.tolist(), strict=True):
        result[index] = score
    return result


def cosine_similarity(arr, val):
    """Cosine similarity over numeric vectors or semantic text embeddings."""
    left_rows = _sequence_rows(arr)
    right_rows = _sequence_rows(val)

    if len(left_rows) == 0:
        return []

    sample_left = next((row for row in left_rows if row is not None), None)
    sample_right = next((row for row in right_rows if row is not None), None)
    if (
        _coerce_numeric_vector(sample_left) is not None
        and _coerce_numeric_vector(sample_right) is not None
    ):
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


def array_contains(arr, val):
    needle = _as_python_value(val)
    rows = _sequence_rows(arr)
    return pyarrow.array(
        [False if row is None else needle in set(_normalize_array_row(row) or []) for row in rows],
        type=pyarrow.bool_(),
    )


def array_contains_any(arr, val):
    needles = frozenset(_normalize_membership_values(val))
    rows = _sequence_rows(arr)
    return pyarrow.array(
        [
            False
            if row is None
            else bool(set(_normalize_array_row(row) or []).intersection(needles))
            for row in rows
        ],
        type=pyarrow.bool_(),
    )


def array_contains_all(arr, val):
    needles = frozenset(_normalize_membership_values(val))
    rows = _sequence_rows(arr)
    return pyarrow.array(
        [
            False if row is None else needles.issubset(set(_normalize_array_row(row) or []))
            for row in rows
        ],
        type=pyarrow.bool_(),
    )


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
