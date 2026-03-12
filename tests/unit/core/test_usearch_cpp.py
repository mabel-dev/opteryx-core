import numpy
import pytest


vector_search = pytest.importorskip("opteryx.nanobind.vector_search")
usearch_native = pytest.importorskip("opteryx.nanobind.usearch_native")


def test_usearch_exact_matches_exact_baseline():
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
    query = numpy.array([1.0, 0.0, 1.0], dtype=numpy.float32)

    expected_ids, expected_scores = vector_search.exact_search_cosine(query, row_ids, vectors, 3)
    expected_distances = [1.0 - score for score in expected_scores]

    index = usearch_native.UsearchIndex(dimensions=3, capacity=4, metric="cos")
    index.add_batch(row_ids, vectors)
    found_ids, found_scores = index.search(query, 3, exact=True)

    assert found_ids == expected_ids
    assert found_scores == pytest.approx(expected_distances, rel=1e-5, abs=1e-6)


def test_usearch_ann_finds_expected_top_match():
    row_ids = numpy.array([10, 20, 30, 40, 50], dtype=numpy.int64)
    vectors = numpy.array(
        [
            [1.0, 0.0, 0.0],
            [0.95, 0.05, 0.0],
            [0.0, 1.0, 0.0],
            [0.8, 0.2, 0.0],
            [0.0, 0.0, 1.0],
        ],
        dtype=numpy.float32,
    )
    query = numpy.array([1.0, 0.0, 0.0], dtype=numpy.float32)

    index = usearch_native.UsearchIndex(dimensions=3, capacity=8, metric="cos")
    index.add_batch(row_ids, vectors)
    found_ids, found_scores = index.search(query, 3)

    assert index.size() == 5
    assert found_ids[0] == 10
    assert found_scores[0] == pytest.approx(0.0, abs=1e-7)


def test_usearch_rejects_dimension_mismatch_on_add_batch():
    row_ids = numpy.array([1, 2], dtype=numpy.int64)
    vectors = numpy.array([[1.0, 2.0], [3.0, 4.0]], dtype=numpy.float32)

    index = usearch_native.UsearchIndex(dimensions=3, capacity=2, metric="cos")
    with pytest.raises(ValueError, match="dimension does not match"):
        index.add_batch(row_ids, vectors)


def test_usearch_rejects_bad_metric_name():
    with pytest.raises(ValueError, match="unsupported metric"):
        usearch_native.UsearchIndex(dimensions=3, metric="pearson-ish")
