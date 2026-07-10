import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx  # noqa: E402


def _materialize(query: str):
    session = opteryx.session()
    try:
        for _ in session.execute_to_morsels(query):
            pass
        return session
    except Exception:
        session.close()
        raise


def test_vector_topk_query_renders_vector_heap_sort():
    cursor = _materialize(
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
    try:
        assert cursor.telemetry.get("optimization_fuse_operators_vector_heap_sort") is not None
    finally:
        cursor.close()


def test_non_nearest_neighbor_vector_sort_does_not_render_vector_heap_sort():
    cursor = _materialize(
        """
        SELECT label
        FROM (
            VALUES
                ('match', (1.0, 0.0)),
                ('diagonal', (1.0, 1.0)),
                ('orthogonal', (0.0, 1.0))
        ) AS vectors(label, embedding)
        ORDER BY COSINE_DISTANCE(embedding, (1.0, 0.0)) DESC
        LIMIT 2
        """
    )
    try:
        assert cursor.telemetry.get("optimization_fuse_operators_vector_heap_sort") is None
    finally:
        cursor.close()


def test_string_array_sort_does_not_render_vector_heap_sort():
    cursor = _materialize(
        """
        SELECT label
        FROM (
            VALUES
                ('match', ('cape', 'canaveral')),
                ('diagonal', ('baikonur', 'kazakhstan')),
                ('orthogonal', ('vandenberg', 'california'))
        ) AS vectors(label, embedding)
        ORDER BY COSINE_DISTANCE(embedding, ('cape', 'canaveral'))
        LIMIT 2
        """
    )
    try:
        assert cursor.telemetry.get("optimization_fuse_operators_vector_heap_sort") is None
    finally:
        cursor.close()


def test_embed_literal_vector_sort_renders_vector_heap_sort():
    class FakeEmbeddingProvider:
        def embed_text(self, text: str):
            assert text == "match"
            return [1.0, 0.0]

    opteryx.register_embedding_provider(FakeEmbeddingProvider())
    try:
        cursor = _materialize(
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
        try:
            assert cursor.telemetry.get("optimization_fuse_operators_vector_heap_sort") is not None
        finally:
            cursor.close()
    finally:
        opteryx.clear_embedding_provider()
