import numpy
import pytest

vector_search = pytest.importorskip("opteryx.compiled.nanobind.vector_search")


def _reference_exact_search(query_vector, row_ids, vectors, k):
    query_norm = numpy.linalg.norm(query_vector)
    scores = []
    for row_id, row in zip(row_ids, vectors, strict=True):
        row_norm = numpy.linalg.norm(row)
        if row_norm == 0.0:
            score = float("-inf")
        else:
            score = float(numpy.dot(query_vector, row) / (query_norm * row_norm))
        scores.append((int(row_id), score))
    scores.sort(key=lambda item: (-item[1], item[0]))
    top = scores[:k]
    return [item[0] for item in top], [item[1] for item in top]


def test_exact_search_cosine_matches_reference_ordering():
    query = numpy.array([1.0, 0.0, 1.0], dtype=numpy.float32)
    row_ids = numpy.array([10, 20, 30, 40], dtype=numpy.int64)
    vectors = numpy.array(
        [
            [1.0, 0.0, 1.0],
            [0.0, 1.0, 0.0],
            [1.0, 0.0, 0.0],
            [0.5, 0.0, 0.5],
        ],
        dtype=numpy.float32,
    )

    expected_ids, expected_scores = _reference_exact_search(query, row_ids, vectors, 3)
    found_ids, found_scores = vector_search.exact_search_cosine(query, row_ids, vectors, 3)

    assert found_ids == expected_ids
    assert found_scores == pytest.approx(expected_scores, rel=1e-6, abs=1e-6)


def test_exact_search_cosine_zero_vector_rows_sort_last():
    query = numpy.array([1.0, 2.0], dtype=numpy.float32)
    row_ids = numpy.array([1, 2, 3], dtype=numpy.int64)
    vectors = numpy.array(
        [
            [0.0, 0.0],
            [1.0, 2.0],
            [2.0, 4.0],
        ],
        dtype=numpy.float32,
    )

    found_ids, found_scores = vector_search.exact_search_cosine(query, row_ids, vectors, 3)

    assert found_ids[:2] == [2, 3]
    assert found_scores[0] == pytest.approx(1.0)
    assert found_scores[1] == pytest.approx(1.0)
    assert found_ids[2] == 1
    assert found_scores[2] == float("-inf")


def test_exact_search_cosine_rejects_dimension_mismatch():
    query = numpy.array([1.0, 2.0], dtype=numpy.float32)
    row_ids = numpy.array([1], dtype=numpy.int64)
    vectors = numpy.array([[1.0, 2.0, 3.0]], dtype=numpy.float32)

    with pytest.raises(ValueError, match="dimension does not match"):
        vector_search.exact_search_cosine(query, row_ids, vectors, 1)


def test_exact_search_cosine_rejects_zero_norm_query():
    query = numpy.array([0.0, 0.0], dtype=numpy.float32)
    row_ids = numpy.array([1], dtype=numpy.int64)
    vectors = numpy.array([[1.0, 2.0]], dtype=numpy.float32)

    with pytest.raises(ValueError, match="norm must be non-zero"):
        vector_search.exact_search_cosine(query, row_ids, vectors, 1)


def test_score_cosine_matches_row_aligned_reference_scores():
    query = numpy.array([1.0, 0.0, 1.0], dtype=numpy.float32)
    vectors = numpy.array(
        [
            [1.0, 0.0, 1.0],
            [0.0, 1.0, 0.0],
            [1.0, 0.0, 0.0],
            [0.0, 0.0, 0.0],
        ],
        dtype=numpy.float32,
    )

    scores = vector_search.score_cosine(query, vectors)

    assert scores == pytest.approx([1.0, 0.0, 0.70710677, float("-inf")], rel=1e-6, abs=1e-6)


def test_score_cosine_rejects_dimension_mismatch():
    query = numpy.array([1.0, 2.0], dtype=numpy.float32)
    vectors = numpy.array([[1.0, 2.0, 3.0]], dtype=numpy.float32)

    with pytest.raises(ValueError, match="dimension does not match"):
        vector_search.score_cosine(query, vectors)
