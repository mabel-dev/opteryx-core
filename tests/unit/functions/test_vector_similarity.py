import os
import shutil
import sys
import types
import uuid
from pathlib import Path
from unittest import mock

import numpy
import pyarrow
import pyarrow.parquet
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from draken.vectors.string_vector import StringVector

import opteryx
from draken import Morsel
from opteryx.connectors import DiskConnector
from opteryx.vectors.embeddings import (
    create_hybrid_embedding_provider,
    create_static_embedding_provider,
    embed_text_matrix,
    embed_text_values,
)
from opteryx.exceptions import FunctionExecutionError, UnsupportedSyntaxError
from opteryx.expression.functions.implementations.text import match_against
from opteryx.expression.functions.implementations.utility import cosine_distance, cosine_similarity
from opteryx.operators.heap_sort_node import HeapSortNode


def _make_vector_parquet_dataset() -> str:
    dataset_name = f"_vector_sql_{uuid.uuid4().hex[:8]}"
    dataset_dir = os.path.join("testdata", dataset_name)
    os.makedirs(dataset_dir, exist_ok=True)
    pyarrow.parquet.write_table(
        pyarrow.table(
            {
                "label": pyarrow.array(["match", "diagonal", "orthogonal", "excluded"]),
                "embedding": pyarrow.array(
                    [[1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, -1.0]],
                    type=pyarrow.list_(pyarrow.float64()),
                ),
            }
        ),
        os.path.join(dataset_dir, "part-0000.parquet"),
    )
    return dataset_name


def _drop_vector_parquet_dataset(dataset_name: str) -> None:
    shutil.rmtree(os.path.join("testdata", dataset_name), ignore_errors=True)


def _default_minilm_available() -> bool:
    model_dir = Path("third_party/models/all-MiniLM-L6-v2")
    if not (model_dir / "model.onnx").exists() or not (model_dir / "vocab.txt").exists():
        return False
    try:
        from opteryx.compiled.nanobind import minilm_native  # noqa: F401
    except ImportError:
        return False
    return True


def test_cosine_similarity_numeric_literal_query_scores_each_row():
    rows = numpy.array(
        [
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
            [1.0, 1.0, 0.0],
            [0.0, 0.0, 0.0],
        ],
        dtype=object,
    )
    query = numpy.array([[1.0, 0.0, 0.0]], dtype=object)

    scores = cosine_similarity(rows, query)

    assert scores == pytest.approx([1.0, 0.0, 0.70710677, 0.0], rel=1e-6, abs=1e-6)


def test_cosine_similarity_numeric_pairwise_scores():
    left = numpy.array([[1.0, 0.0], [1.0, 1.0], [0.0, 0.0]], dtype=object)
    right = numpy.array([[1.0, 0.0], [0.0, 1.0], [1.0, 0.0]], dtype=object)

    scores = cosine_similarity(left, right)

    assert scores == pytest.approx([1.0, 0.70710677, 0.0], rel=1e-6, abs=1e-6)


def test_cosine_similarity_numeric_pairwise_ignores_invalid_rows():
    left = numpy.array([[1.0, 0.0], None, [1.0, 1.0, 1.0], [0.0, 1.0]], dtype=object)
    right = numpy.array([[1.0, 0.0], [1.0, 0.0], [1.0, 1.0], None], dtype=object)

    scores = cosine_similarity(left, right)

    assert scores == pytest.approx([1.0, 0.0, 0.0, 0.0], rel=1e-6, abs=1e-6)


def test_cosine_similarity_text_uses_embedding_provider():
    class FakeEmbeddingProvider:
        def embed_texts(self, texts: list[str]):
            vectors = {
                "planet mars": [1.0, 0.0],
                "mars is rocky": [0.9, 0.1],
                "venus is hot": [0.0, 1.0],
            }
            return [vectors[text] for text in texts]

    opteryx.register_embedding_provider(FakeEmbeddingProvider())
    try:
        rows = numpy.array(["mars is rocky", "venus is hot", "   ", None], dtype=object)
        query = numpy.array(["planet mars"], dtype=object)

        scores = cosine_similarity(rows, query)

        assert scores[0] == pytest.approx(0.9938837, rel=1e-6, abs=1e-6)
        assert scores[1] == pytest.approx(0.0, rel=1e-6, abs=1e-6)
        assert scores[2:] == pytest.approx([0.0, 0.0], rel=1e-6, abs=1e-6)
    finally:
        opteryx.clear_embedding_provider()


def test_cosine_similarity_text_prefers_provider_scorer():
    calls = {"score": 0, "batch": 0}

    class FakeEmbeddingProvider:
        prefer_score_string_vector = True

        def score_string_vector(self, query_text: str, values):
            calls["score"] += 1
            assert query_text == "planet mars"
            assert isinstance(values, StringVector)
            return [0, 1], [0.9, 0.1]

        def embed_texts(self, texts: list[str]):
            calls["batch"] += 1
            raise AssertionError("provider scorer should be preferred")

    opteryx.register_embedding_provider(FakeEmbeddingProvider())
    try:
        rows = pyarrow.array(["mars is rocky", "venus is hot", None], type=pyarrow.binary())
        query = numpy.array(["planet mars"], dtype=object)

        scores = cosine_similarity(rows, query)

        assert scores == pytest.approx([0.9, 0.1, 0.0], rel=1e-6, abs=1e-6)
        assert calls["score"] == 1
        assert calls["batch"] == 0
    finally:
        opteryx.clear_embedding_provider()


def test_static_embedding_provider_is_deterministic_and_normalized():
    provider = create_static_embedding_provider(dimensions=64)

    first = provider.embed_text("Fish and chips for dinner")
    second = provider.embed_text("Fish and chips for dinner")
    third = provider.embed_text("Launch vehicle telemetry")

    assert first.dtype == numpy.float32
    assert first.shape == (64,)
    assert numpy.allclose(first, second)
    assert numpy.linalg.norm(first) == pytest.approx(1.0, rel=1e-5, abs=1e-5)
    assert numpy.dot(first, second) > numpy.dot(first, third)


def test_use_static_embedding_provider_supports_text_cosine_similarity():
    opteryx.use_static_embedding_provider(dimensions=64)
    try:
        rows = numpy.array(
            ["fish and chips for dinner", "launch telemetry", "fish supper tonight"],
            dtype=object,
        )
        query = numpy.array(["I would like fish and chips for dinner"], dtype=object)

        scores = cosine_similarity(rows, query)

        assert scores[0] > scores[1]
        assert scores[0] > 0.0
        assert scores[2] > scores[1]
    finally:
        opteryx.clear_embedding_provider()


def test_hybrid_provider_shortlists_lexical_matches_before_rerank():
    if not _default_minilm_available():
        pytest.skip("native MiniLM provider is not available in this environment")

    provider = create_hybrid_embedding_provider(static_dimensions=64, rerank_k=2)
    seen = {"texts": None}

    class FakeReranker:
        def embed_text(self, text: str):
            raise AssertionError("single-text rerank path should not be used")

        def embed_texts(self, texts: list[str]):
            seen["texts"] = texts
            vectors = []
            for text in texts:
                normalized = text.lower()
                if normalized == "fish and chips for dinner":
                    vectors.append([1.0, 0.0])
                elif "fish" in normalized or "chips" in normalized or "dinner" in normalized:
                    vectors.append([0.9, 0.1])
                else:
                    vectors.append([0.0, 1.0])
            return vectors

    provider._reranker = FakeReranker()
    rows = StringVector.from_arrow(
        pyarrow.array(
            [
                "launch telemetry update",
                "fish and chips by the harbor",
                "dinner reservation and fish special",
                "orbital mechanics lecture",
            ],
            type=pyarrow.string(),
        )
    )

    positions, scores = provider.score_string_vector("fish and chips for dinner", rows)

    assert seen["texts"] is not None
    assert seen["texts"][0] == "fish and chips for dinner"
    assert "fish and chips by the harbor" in seen["texts"][1:]
    assert "dinner reservation and fish special" in seen["texts"][1:]
    score_map = dict(zip(positions.tolist(), scores.tolist(), strict=True))
    assert score_map[1] > score_map[0]
    assert score_map[2] > score_map[3]


def test_cosine_distance_returns_one_minus_similarity():
    rows = numpy.array([[1.0, 0.0], [0.0, 1.0]], dtype=object)
    query = numpy.array([[1.0, 0.0]], dtype=object)

    distances = cosine_distance(rows, query)

    assert distances == pytest.approx([0.0, 1.0], rel=1e-6, abs=1e-6)


def test_match_against_accepts_scalar_literal_in_draken_style_call_shape():
    class FakeEmbeddingProvider:
        def embed_texts(self, texts: list[str]):
            assert texts == [
                "cape canaveral florida",
                "LC-18A, Cape Canaveral AFS, Florida, USA",
                "Site 1/5, Baikonur Cosmodrome, Kazakhstan",
            ]
            return [
                [1.0, 0.0],
                [0.95, 0.05],
                [0.0, 1.0],
            ]

    opteryx.register_embedding_provider(FakeEmbeddingProvider())
    try:
        rows = numpy.array(
            [
                "LC-18A, Cape Canaveral AFS, Florida, USA",
                "Site 1/5, Baikonur Cosmodrome, Kazakhstan",
            ],
            dtype=object,
        )

        matches = match_against(rows, "cape canaveral florida")
        matches = matches.to_pylist() if hasattr(matches, "to_pylist") else matches

        assert matches == [True, False]
    finally:
        opteryx.clear_embedding_provider()


def test_match_against_string_vector_uses_batched_embeddings():
    calls = {"score": 0, "batch": 0}

    class FakeEmbeddingProvider:
        def score_string_vector(self, query_text: str, values):
            calls["score"] += 1
            raise AssertionError("string-vector scorer should not bypass embedding cache")

        def embed_texts(self, texts: list[str]):
            calls["batch"] += 1
            assert texts == ["planet mars", "mars is rocky", "venus is hot"]
            vectors = {
                "planet mars": [1.0, 0.0],
                "mars is rocky": [0.95, 0.05],
                "venus is hot": [0.0, 1.0],
            }
            return [vectors[text] for text in texts]

    opteryx.register_embedding_provider(FakeEmbeddingProvider())
    try:
        rows = StringVector.from_arrow(
            pyarrow.array(["mars is rocky", None, "venus is hot", "   "], type=pyarrow.string())
        )

        matches = match_against(rows, "planet mars")
        matches = matches.to_pylist() if hasattr(matches, "to_pylist") else matches

        assert matches == [True, False, False, False]
        assert calls["score"] == 0
        assert calls["batch"] == 1
    finally:
        opteryx.clear_embedding_provider()


def test_match_against_scores_dictionary_values_once():
    calls = {"score": 0, "batch": 0}

    class FakeEmbeddingProvider:
        def score_string_vector(self, query_text: str, values):
            calls["score"] += 1
            raise AssertionError("dictionary fast path should not use the uncached scorer")

        def embed_texts(self, texts: list[str]):
            calls["batch"] += 1
            assert texts == ["planet mars", "mars is rocky", "venus is hot"]
            vectors = {
                "planet mars": [1.0, 0.0],
                "mars is rocky": [0.95, 0.05],
                "venus is hot": [0.0, 1.0],
            }
            return [vectors[text] for text in texts]

    opteryx.register_embedding_provider(FakeEmbeddingProvider())
    try:
        rows = pyarrow.array(
            ["mars is rocky", "venus is hot", "mars is rocky", None],
            type=pyarrow.dictionary(pyarrow.int32(), pyarrow.string()),
        )
        vector = Morsel.from_arrow(pyarrow.table({"v": rows})).column(b"v")

        matches = match_against(vector, "planet mars")
        matches = matches.to_pylist() if hasattr(matches, "to_pylist") else matches

        assert matches == [True, False, True, False]
        assert calls["score"] == 0
        assert calls["batch"] == 1
    finally:
        opteryx.clear_embedding_provider()


def test_match_against_is_not_user_callable_sql():
    session = opteryx.session()
    try:
        with pytest.raises(UnsupportedSyntaxError, match="MATCH_AGAINST"):
            session.execute("SELECT MATCH_AGAINST('cape canaveral', 'cape canaveral')")
        with pytest.raises(UnsupportedSyntaxError, match="_MATCH_AGAINST"):
            session.execute("SELECT _MATCH_AGAINST('cape canaveral', 'cape canaveral')")
    finally:
        session.close()


def test_cosine_similarity_via_sql_with_literal_vectors():
    session = opteryx.session()
    try:
        session.execute("SELECT COSINE_SIMILARITY((1.0, 0.0), (1.0, 0.0)) AS score")
        rows = session.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == pytest.approx(1.0, rel=1e-6, abs=1e-6)
    finally:
        session.close()


def test_cosine_distance_via_sql_with_literal_vectors():
    session = opteryx.session()
    try:
        session.execute("SELECT COSINE_DISTANCE((1.0, 0.0), (0.0, 1.0)) AS distance")
        rows = session.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == pytest.approx(1.0, rel=1e-6, abs=1e-6)
    finally:
        session.close()


def test_embed_via_sql_uses_registered_provider():
    class FakeEmbeddingProvider:
        def embed_text(self, text: str):
            assert text == "cape canaveral"
            return [0.25, 0.5, 0.75]

    opteryx.register_embedding_provider(FakeEmbeddingProvider())
    session = opteryx.session()
    try:
        session.execute("SELECT EMBED('cape canaveral') AS embedding")
        rows = session.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == pytest.approx([0.25, 0.5, 0.75], rel=1e-6, abs=1e-6)
    finally:
        session.close()
        opteryx.clear_embedding_provider()


def test_embed_uses_default_minilm_provider():
    if not _default_minilm_available():
        pytest.skip("native MiniLM provider is not available in this environment")

    opteryx.clear_embedding_provider()
    session = opteryx.session()
    try:
        session.execute("SELECT EMBED('cape canaveral') AS embedding")
        rows = session.fetchall()
        assert len(rows) == 1
        embedding = rows[0][0]
        assert isinstance(embedding, list)
        assert len(embedding) == 384
        assert all(numpy.isfinite(embedding))
    finally:
        session.close()


def test_embed_invalid_provider_raises_function_execution_error():
    class BrokenProvider:
        def embed_text(self, text: str):
            return "not a vector"

    opteryx.register_embedding_provider(BrokenProvider())
    session = opteryx.session()
    try:
        with pytest.raises(FunctionExecutionError, match="embedding_provider"):
            session.execute("SELECT EMBED('cape canaveral') AS embedding")
    finally:
        session.close()
        opteryx.clear_embedding_provider()


def test_embed_text_values_deduplicates_duplicate_texts_within_a_batch():
    calls = {"single": 0, "batch": 0}

    class FakeEmbeddingProvider:
        def embed_texts(self, texts: list[str]):
            calls["batch"] += 1
            assert texts == ["mars", "venus"]
            return [[1.0, 0.0], [0.0, 1.0]]

        def embed_text(self, text: str):
            calls["single"] += 1
            raise AssertionError("batch path should be used")

    opteryx.register_embedding_provider(FakeEmbeddingProvider())
    try:
        embedded = embed_text_values(["mars", "mars", "venus", "mars"])
        assert embedded == [[1.0, 0.0], [1.0, 0.0], [0.0, 1.0], [1.0, 0.0]]
        assert calls["batch"] == 1
        assert calls["single"] == 0
    finally:
        opteryx.clear_embedding_provider()


def test_embed_text_values_reuses_cached_results_across_calls():
    calls = {"single": 0}

    class FakeEmbeddingProvider:
        def embed_text(self, text: str):
            calls["single"] += 1
            return [float(len(text)), 1.0]

    opteryx.register_embedding_provider(FakeEmbeddingProvider())
    try:
        first = embed_text_values(["mars", "venus"])
        second = embed_text_values(["venus", "mars", "mars"])

        assert first == [[4.0, 1.0], [5.0, 1.0]]
        assert second == [[5.0, 1.0], [4.0, 1.0], [4.0, 1.0]]
        assert calls["single"] == 2
    finally:
        opteryx.clear_embedding_provider()


def test_embed_text_matrix_reuses_cached_results_and_returns_float32():
    calls = {"single": 0}

    class FakeEmbeddingProvider:
        def embed_text(self, text: str):
            calls["single"] += 1
            return [float(len(text)), 1.0, 2.0]

    opteryx.register_embedding_provider(FakeEmbeddingProvider())
    try:
        first = embed_text_matrix(["mars", "venus"])
        second = embed_text_matrix(["venus", "mars", "mars"])

        assert first.dtype == numpy.float32
        assert second.dtype == numpy.float32
        assert first == pytest.approx(
            numpy.asarray([[4.0, 1.0, 2.0], [5.0, 1.0, 2.0]], dtype=numpy.float32),
            rel=1e-6,
            abs=1e-6,
        )
        assert second == pytest.approx(
            numpy.asarray(
                [[5.0, 1.0, 2.0], [4.0, 1.0, 2.0], [4.0, 1.0, 2.0]],
                dtype=numpy.float32,
            ),
            rel=1e-6,
            abs=1e-6,
        )
        assert calls["single"] == 2
    finally:
        opteryx.clear_embedding_provider()


def test_mission_rag_search_sql_returns_expected_location():
    session = opteryx.session()
    try:
        session.execute(
            """
            SELECT
                Mission,
                Location,
                COSINE_SIMILARITY(Location, 'cape canaveral florida') AS score
            FROM testdata.missions
            WHERE MATCH(Location) AGAINST('cape canaveral florida')
            ORDER BY score DESC
            LIMIT 10
            """
        )
        rows = session.fetchall()

        assert len(rows) >= 1

        normalized_rows = []
        for mission, location, score in rows:
            if isinstance(location, bytes):
                location = location.decode("utf8", errors="ignore")
            normalized_rows.append((mission, location, score))

        assert any(
            mission is not None
            and "cape canaveral" in location.lower()
            and "florida" in location.lower()
            and score > 0.5
            for mission, location, score in normalized_rows
        )
    finally:
        session.close()


def test_match_against_via_sql_uses_semantic_embedding_threshold():
    class FakeEmbeddingProvider:
        def embed_texts(self, texts: list[str]):
            vectors = {
                "planet mars": [1.0, 0.0],
                "mars is the red planet": [0.9, 0.1],
                "venus has a thick atmosphere": [0.0, 1.0],
                "jupiter is the largest planet": [0.3, 0.7],
            }
            return [vectors[text] for text in texts]

    opteryx.register_embedding_provider(FakeEmbeddingProvider())
    session = opteryx.session()
    try:
        result = session.execute_to_arrow(
            """
            SELECT label
            FROM (
                VALUES
                    ('mars', 'mars is the red planet'),
                    ('venus', 'venus has a thick atmosphere'),
                    ('jupiter', 'jupiter is the largest planet')
            ) AS docs(label, text)
            WHERE MATCH(text) AGAINST('planet mars')
            ORDER BY label
            """
        )
        labels = [
            value.decode("utf8") if isinstance(value, bytes) else value
            for value in result["label"].to_pylist()
        ]
        assert labels == ["mars"]
    finally:
        session.close()
        opteryx.clear_embedding_provider()


def test_vector_order_by_limit_can_route_through_usearch_via_sql():
    import opteryx.compiled.nanobind as nanobind_pkg

    calls = {"created": 0, "add_batch": 0, "search": 0}

    class FakeIndex:
        def __init__(
            self, dimensions, capacity=0, metric="cos", expansion_add=0, expansion_search=0
        ):
            calls["created"] += 1
            assert dimensions == 2
            assert metric == "cos"

        def add_batch(self, row_ids, vectors):
            calls["add_batch"] += 1
            assert row_ids.tolist() == [0, 1, 2]
            assert vectors.shape == (3, 2)

        def search(self, query_vector, k, exact=False):
            calls["search"] += 1
            assert query_vector.tolist() == pytest.approx([1.0, 0.0], abs=1e-6)
            assert k == 2
            assert exact is False
            return [0, 1], [0.0, 0.29289323]

    with (
        mock.patch.object(HeapSortNode, "_USEARCH_ENABLED", True),
        mock.patch.object(HeapSortNode, "_USEARCH_MIN_ROWS", 1),
        mock.patch.object(
            nanobind_pkg,
            "usearch_native",
            types.SimpleNamespace(UsearchIndex=FakeIndex),
            create=True,
        ),
    ):
        session = opteryx.session()
        try:
            result = session.execute_to_arrow(
                """
                SELECT label
                FROM (
                    VALUES
                        ('match', (1.0, 0.0)),
                        ('diagonal', (1.0, 1.0)),
                        ('orthogonal', (0.0, 1.0))
                ) AS vectors(label, embedding)
                ORDER BY COSINE_DISTANCE(embedding, (1.0, 0.0))
                LIMIT 2
                """
            )

            labels = [
                value.decode("utf8") if isinstance(value, bytes) else value
                for value in result["label"].to_pylist()
            ]
            assert labels == ["match", "diagonal"]
            assert calls["created"] >= 1
            assert calls["add_batch"] >= 1
            assert calls["search"] >= 1

            operations = session.telemetry.get("operations", {})
            usearch_hits = sum(
                op.get("feature_vector_topk_usearch", 0) for op in operations.values()
            )
            usearch_rows = sum(
                op.get("vector_topk_usearch_rows_indexed", 0) for op in operations.values()
            )
            assert usearch_hits >= 1
            assert usearch_rows >= 3
        finally:
            session.close()


def test_vector_order_by_limit_can_use_embed_query_vector():
    import opteryx.compiled.nanobind as nanobind_pkg

    calls = {"created": 0, "add_batch": 0, "search": 0}

    class FakeEmbeddingProvider:
        def embed_text(self, text: str):
            assert text == "match"
            return [1.0, 0.0]

    class FakeIndex:
        def __init__(
            self, dimensions, capacity=0, metric="cos", expansion_add=0, expansion_search=0
        ):
            calls["created"] += 1
            assert dimensions == 2
            assert metric == "cos"

        def add_batch(self, row_ids, vectors):
            calls["add_batch"] += 1
            assert row_ids.tolist() == [0, 1, 2]
            assert vectors.shape == (3, 2)

        def search(self, query_vector, k, exact=False):
            calls["search"] += 1
            assert query_vector.tolist() == pytest.approx([1.0, 0.0], abs=1e-6)
            assert k == 2
            assert exact is False
            return [0, 1], [0.0, 0.29289323]

    opteryx.register_embedding_provider(FakeEmbeddingProvider())
    try:
        with (
            mock.patch.object(HeapSortNode, "_USEARCH_ENABLED", True),
            mock.patch.object(HeapSortNode, "_USEARCH_MIN_ROWS", 1),
            mock.patch.object(
                nanobind_pkg,
                "usearch_native",
                types.SimpleNamespace(UsearchIndex=FakeIndex),
                create=True,
            ),
        ):
            session = opteryx.session()
            try:
                result = session.execute_to_arrow(
                    """
                    SELECT label
                    FROM (
                        VALUES
                            ('match', (1.0, 0.0)),
                            ('diagonal', (1.0, 1.0)),
                            ('orthogonal', (0.0, 1.0))
                    ) AS vectors(label, embedding)
                    ORDER BY COSINE_DISTANCE(embedding, EMBED('match'))
                    LIMIT 2
                    """
                )

                labels = [
                    value.decode("utf8") if isinstance(value, bytes) else value
                    for value in result["label"].to_pylist()
                ]
                assert labels == ["match", "diagonal"]
                assert calls["created"] >= 1
                assert calls["add_batch"] >= 1
                assert calls["search"] >= 1
            finally:
                session.close()
    finally:
        opteryx.clear_embedding_provider()


def test_vector_order_by_limit_only_indexes_filtered_candidates():
    import opteryx.compiled.nanobind as nanobind_pkg

    calls = {"created": 0, "add_batch": 0, "search": 0}
    batches = []

    class FakeIndex:
        def __init__(
            self, dimensions, capacity=0, metric="cos", expansion_add=0, expansion_search=0
        ):
            calls["created"] += 1
            assert dimensions == 2
            assert metric == "cos"

        def add_batch(self, row_ids, vectors):
            calls["add_batch"] += 1
            batches.append((row_ids.tolist(), vectors.tolist()))
            assert row_ids.tolist() == list(range(vectors.shape[0]))

        def search(self, query_vector, k, exact=False):
            calls["search"] += 1
            assert query_vector.tolist() == pytest.approx([1.0, 0.0], abs=1e-6)
            assert k == 2
            assert exact is False
            return [0, 1], [0.0, 0.29289323]

    with (
        mock.patch.object(HeapSortNode, "_USEARCH_ENABLED", True),
        mock.patch.object(HeapSortNode, "_USEARCH_MIN_ROWS", 1),
        mock.patch.object(
            nanobind_pkg,
            "usearch_native",
            types.SimpleNamespace(UsearchIndex=FakeIndex),
            create=True,
        ),
    ):
        session = opteryx.session()
        try:
            result = session.execute_to_arrow(
                """
                SELECT label
                FROM (
                    VALUES
                        ('match', (1.0, 0.0)),
                        ('diagonal', (1.0, 1.0)),
                        ('orthogonal', (0.0, 1.0)),
                        ('excluded', (0.0, -1.0))
                ) AS vectors(label, embedding)
                WHERE label != 'excluded'
                ORDER BY COSINE_DISTANCE(embedding, (1.0, 0.0))
                LIMIT 2
                """
            )

            labels = [
                value.decode("utf8") if isinstance(value, bytes) else value
                for value in result["label"].to_pylist()
            ]
            assert labels == ["match", "diagonal"]
            assert calls["created"] >= 1
            assert calls["add_batch"] >= 1
            assert calls["search"] >= 1
            assert any(len(vectors) == 3 for _, vectors in batches)
            assert all([0.0, -1.0] not in vectors for _, vectors in batches)

            operations = session.telemetry.get("operations", {})
            candidate_rows = sum(
                op.get("vector_topk_candidate_rows", 0) for op in operations.values()
            )
            usearch_rows = sum(
                op.get("vector_topk_usearch_rows_indexed", 0) for op in operations.values()
            )
            assert candidate_rows >= 3
            assert usearch_rows >= 3
        finally:
            session.close()


def test_vector_order_by_limit_can_scan_parquet_backed_vector_column():
    import opteryx.compiled.nanobind as nanobind_pkg

    calls = {"created": 0, "search": 0}
    dataset_name = _make_vector_parquet_dataset()

    class FakeIndex:
        def __init__(
            self, dimensions, capacity=0, metric="cos", expansion_add=0, expansion_search=0
        ):
            calls["created"] += 1
            assert dimensions == 2
            assert metric == "cos"
            assert capacity >= 3

        def add_batch(self, row_ids, vectors):
            assert vectors.shape[1] == 2
            assert all(vector != [0.0, -1.0] for vector in vectors.tolist())

        def search(self, query_vector, k, exact=False):
            calls["search"] += 1
            assert query_vector.tolist() == pytest.approx([1.0, 0.0], abs=1e-6)
            assert k == 2
            assert exact is False
            return [0, 1], [0.0, 0.29289323]

    opteryx.register_workspace("testdata", DiskConnector)

    with (
        mock.patch.object(HeapSortNode, "_USEARCH_ENABLED", True),
        mock.patch.object(HeapSortNode, "_USEARCH_MIN_ROWS", 1),
        mock.patch.object(
            nanobind_pkg,
            "usearch_native",
            types.SimpleNamespace(UsearchIndex=FakeIndex),
            create=True,
        ),
    ):
        session = opteryx.session()
        try:
            result = session.execute_to_arrow(
                f"""
                SELECT label
                FROM testdata.{dataset_name}
                WHERE label != 'excluded'
                ORDER BY COSINE_DISTANCE(CAST(embedding AS VECTOR), (1.0, 0.0))
                LIMIT 2
                """
            )

            labels = [
                value.decode("utf8") if isinstance(value, bytes) else value
                for value in result["label"].to_pylist()
            ]
            assert labels == ["match", "diagonal"]
            assert calls["created"] >= 1
            assert calls["search"] >= 1

            operations = session.telemetry.get("operations", {})
            usearch_hits = sum(
                op.get("feature_vector_topk_usearch", 0) for op in operations.values()
            )
            candidate_rows = sum(
                op.get("vector_topk_candidate_rows", 0) for op in operations.values()
            )
            assert usearch_hits >= 1
            assert candidate_rows >= 3
        finally:
            session.close()
            _drop_vector_parquet_dataset(dataset_name)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
