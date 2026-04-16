# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Utility function kernels.

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

import math

from opteryx.third_party.tktech import csimdjson as simdjson
from opteryx.vectors.embeddings import embed_text_matrix, embed_text_values, get_embedding_provider

# ============================================================================
# Math utility functions for vector operations
# ============================================================================


def _vec_norm(vec: list) -> float:
    """Compute L2 norm of a vector."""
    sum_sq = sum(x * x for x in vec)
    return math.sqrt(sum_sq) if sum_sq > 0 else 0.0


def _dot_product(a: list, b: list) -> float:
    """Compute dot product of two vectors."""
    return sum(x * y for x, y in zip(a, b))


def _is_finite(x: float) -> bool:
    """Check if a float value is finite (not NaN or inf)."""
    return x == x and abs(x) != float("inf")  # x==x is False for NaN


def _normalize_membership_values(value):
    """Normalize membership test values. Keep Draken vectors as-is."""
    if value is None:
        return []
    if isinstance(value, (list, tuple, set, frozenset)):
        if len(value) == 1:
            first_elem = next(iter(value))
            if isinstance(first_elem, (list, tuple, set, frozenset)):
                return _normalize_array_row(first_elem) or []
        return list(value)
    # For Draken vectors, return as-is
    return value




def _coerce_text_scalar(value):
    """Coerce value to text scalar. Assumes Python values or Draken scalars."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf8", errors="ignore")
    return str(value)


def _as_text_vector(values):
    from opteryx.compiled.draken.interop.vector_sequence import vector_from_sequence
    from opteryx.compiled.draken.vectors.string_vector import StringVector

    if isinstance(values, StringVector):
        return values

    # Convert to StringVector from Python list
    if isinstance(values, (list, tuple)):
        vector = vector_from_sequence(values)
        return vector if isinstance(vector, StringVector) else None

    return None


def _coerce_numeric_matrix(rows, width=None):
    dense_rows = []
    valid_positions = []

    for index, row in enumerate(rows):
        vector = row
        if vector is None or len(vector) == 0:
            continue
        if width is None:
            width = len(vector)
        if len(vector) != width:
            continue
        dense_rows.append(vector)
        valid_positions.append(index)

    if width is None:
        width = 0

    # Return Python lists instead of NumPy arrays
    return (dense_rows, valid_positions)


def _coerce_aligned_numeric_matrices(left_rows, right_rows):
    left_dense_rows = []
    right_dense_rows = []
    valid_positions = []
    width = None

    for index, (left_row, right_row) in enumerate(zip(left_rows, right_rows, strict=True)):
        left_vector = left_row
        right_vector = right_row
        if left_vector is None or right_vector is None:
            continue
        if len(left_vector) == 0 or len(right_vector) == 0 or len(left_vector) != len(right_vector):
            continue
        if width is None:
            width = len(left_vector)
        if len(left_vector) != width or len(right_vector) != width:
            continue
        left_dense_rows.append(left_vector)
        right_dense_rows.append(right_vector)
        valid_positions.append(index)

    if width is None:
        width = 0

    # Return Python lists instead of NumPy arrays
    return (left_dense_rows, right_dense_rows, valid_positions)


def _score_numeric_vectors(left_rows, right_rows):
    if len(right_rows) == 0:
        return []

    query_vector = right_rows[0]
    if len(right_rows) == 1 and query_vector is not None and len(query_vector) > 0:
        dense_vectors, valid_positions = _coerce_numeric_matrix(left_rows, len(query_vector))
        scores = [0.0] * len(left_rows)
        if len(dense_vectors) == 0:
            return scores

        try:
            from opteryx.compiled.nanobind import vector_search

            valid_scores = vector_search.score_cosine(query_vector, dense_vectors)
            valid_scores = [float(s) for s in valid_scores]
        except (ImportError, ValueError, TypeError):
            # Fallback: compute cosine similarity in Python
            query_norm = _vec_norm(query_vector)
            if query_norm == 0.0:
                return scores

            valid_scores = []
            for row_vec in dense_vectors:
                row_norm = _vec_norm(row_vec)
                if row_norm == 0.0:
                    valid_scores.append(0.0)
                else:
                    dot = _dot_product(row_vec, query_vector)
                    similarity = dot / (row_norm * query_norm)
                    valid_scores.append(similarity if _is_finite(similarity) else 0.0)

        for pos, score in zip(valid_positions, valid_scores):
            scores[pos] = score
        return scores

    if len(right_rows) != len(left_rows):
        return [0.0] * len(left_rows)

    left_vectors, right_vectors, valid_positions = _coerce_aligned_numeric_matrices(
        left_rows, right_rows
    )
    scores = [0.0] * len(left_rows)
    if len(valid_positions) == 0:
        return scores

    # Compute pairwise cosine similarities
    for i, (left_vec, right_vec) in enumerate(zip(left_vectors, right_vectors)):
        left_norm = _vec_norm(left_vec)
        right_norm = _vec_norm(right_vec)
        if left_norm == 0.0 or right_norm == 0.0:
            score = 0.0
        else:
            dot = _dot_product(left_vec, right_vec)
            score = dot / (left_norm * right_norm)
            score = score if _is_finite(score) else 0.0
        scores[valid_positions[i]] = score
    return scores


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
            result = [0.0] * len(arr)
            for pos, score in zip(positions, scores):
                if 0 <= pos < len(arr) and _is_finite(float(score)):
                    result[pos] = float(score)
            return result

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

        scores = vector_search.score_cosine(query_vector, row_vectors)
    except (ImportError, ValueError, TypeError):
        # Fallback: compute in Python
        scores = []
        query_norm = _vec_norm(query_vector)
        if query_norm == 0.0:
            scores = [0.0] * len(row_vectors)
        else:
            for row_vec in row_vectors:
                row_norm = _vec_norm(row_vec)
                if row_norm == 0.0:
                    scores.append(0.0)
                else:
                    dot = _dot_product(row_vec, query_vector)
                    similarity = dot / (row_norm * query_norm)
                    scores.append(similarity if _is_finite(similarity) else 0.0)

    for index, score in zip(active_positions, scores, strict=True):
        result[index] = float(score)
    return result


def cosine_similarity(arr, val):
    """Cosine similarity over numeric vectors or semantic text embeddings."""
    if len(arr) == 0:
        return []

    sample_left = next((row for row in arr if row is not None), None)
    sample_right = next((row for row in val if row is not None), None)
    if sample_left is not None and sample_right is not None:
        return _score_numeric_vectors(arr, val)

    return _cosine_similarity_text(arr, val)


def cosine_distance(arr, val):
    """Cosine distance for numeric vectors, returned as 1 - cosine_similarity."""
    scores = cosine_similarity(arr, val)
    if not scores:
        return []
    # Clip each score to [-1, 1] and compute 1 - similarity
    return [1.0 - max(-1.0, min(float(s), 1.0)) for s in scores]


def embed(arr):
    """Convert text values into numeric vectors using the configured embedding provider."""
    if len(arr) == 0:
        return []

    texts = []
    row_positions = []
    results = [None] * len(arr)
    for index, value in enumerate(arr):
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


def jsonb_object_keys(arr):
    """
    Extract the keys from an array of JSON objects or JSON strings/bytes.
    """
    if len(arr) == 0:
        return []

    # Assume arr is already a list/tuple or has to_pylist()
    if not isinstance(arr, (list, tuple)):
        arr = arr.to_pylist()

    result = []
    if len(arr) == 0:
        return result

    first_elem = arr[0]
    if isinstance(first_elem, dict):
        for row in arr:
            result.append([str(key) for key in row.keys()])
    elif isinstance(first_elem, (str, bytes)):
        parser = simdjson.Parser()
        for row in arr:
            result.append([str(key) for key in parser.parse(row).keys()])
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

    # Convert to Python list if needed
    if not isinstance(arr, (list, tuple)):
        if hasattr(arr, "tolist"):
            arr = arr.tolist()
        else:
            arr = list(arr)

    return [humanize_number(value) for value in arr]


def array_contains(arr, val):
    """Check if array contains value. Assumes Draken vectors."""
    from opteryx.compiled.draken.interop.vector_sequence import vector_from_sequence

    needle = val[0] if hasattr(val, "__getitem__") else val
    bool_list = []
    for row in arr:
        if row is None:
            bool_list.append(False)
        else:
            try:
                bool_list.append(needle in set(row))
            except TypeError:
                bool_list.append(needle in row)
    return vector_from_sequence(bool_list)


def array_contains_any(arr, val):
    from opteryx.compiled.draken.interop.vector_sequence import vector_from_sequence

    needles = frozenset(_normalize_membership_values(val))
    bool_list = []
    for row in arr:
        if row is None:
            bool_list.append(False)
        else:
            try:
                bool_list.append(bool(set(row).intersection(needles)))
            except TypeError:
                bool_list.append(any(n in row for n in needles))
    return vector_from_sequence(bool_list)


def array_contains_all(arr, val):
    from opteryx.compiled.draken.interop.vector_sequence import vector_from_sequence

    needles = frozenset(_normalize_membership_values(val))
    bool_list = []
    for row in arr:
        if row is None:
            bool_list.append(False)
        else:
            try:
                bool_list.append(needles.issubset(set(row)))
            except TypeError:
                bool_list.append(all(n in row for n in needles))
    return vector_from_sequence(bool_list)


def array_cast(array, element_type):
    from opteryx.types import OrsoTypes

    # Convert to list if needed
    if hasattr(array, "tolist"):
        array = array.tolist()

    result = [None] * len(array)
    parser = OrsoTypes[element_type[0]].parse
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

    from opteryx.types import OrsoTypes

    result = [None] * len(array)
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
