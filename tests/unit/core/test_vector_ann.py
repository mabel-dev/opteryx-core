"""draken/ops/ann — fp16 cosine HNSW over a VECTOR_FP16 column (vector index, Stage B).

What each test protects:
  * the graph's distances are the SQL kernel's COSINE_DISTANCE values (no re-rank needed);
  * graph search agrees with the exact scan at high recall on clustered data;
  * rows outside the searchable domain (null, deleted, zero-magnitude, non-finite) are never
    returned by either path;
  * the admitted mask filters DURING traversal — every hit is admitted, and k hits are still
    returned when k admitted rows exist;
  * a graph is never used against the wrong data: checksum, row count, dimension, binding.
"""

import os
import random
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

from draken import draken_native
from opteryx.compiled.nanobind import vectors as V

DIM = 32
N = 3000
CLUSTERS = 24


def _bitmap(rows, n):
    out = bytearray((n + 7) // 8)
    for r in rows:
        out[r >> 3] |= 1 << (r & 7)
    return bytes(out)


def _dataset(seed=7):
    rng = random.Random(seed)
    centres = [[rng.gauss(0, 1) for _ in range(DIM)] for _ in range(CLUSTERS)]
    rows = []
    for _ in range(N):
        c = centres[rng.randrange(CLUSTERS)]
        rows.append([x + 0.5 * rng.gauss(0, 1) for x in c])
    rows[5] = [0.0] * DIM                      # zero magnitude: undefined cosine
    rows[6] = [float("nan")] + [1.0] * (DIM - 1)  # non-finite
    rows[7] = None                              # null
    vec = draken_native.vector_fp16_from_sequence(rows, DIM)
    queries = []
    for _ in range(40):
        c = centres[rng.randrange(CLUSTERS)]
        queries.append([x + 0.5 * rng.gauss(0, 1) for x in c])
    return vec, queries


UNSEARCHABLE = {5, 6, 7}
DELETED = {9, 11}


@pytest.fixture(scope="module")
def built():
    vec, queries = _dataset()
    excluded = _bitmap(DELETED, N)
    graph = V.ann_hnsw_build(vec, excluded, 16, 128, 2, b"bind:test")
    return vec, queries, excluded, graph


def test_recall_against_exact(built):
    vec, queries, excluded, graph = built
    total = 0.0
    for q in queries:
        ann_rows, _ = V.ann_hnsw_search(graph, vec, q, 10, 64, None)
        exact_rows, _ = V.ann_exact_topk(vec, q, 10, excluded, None)
        total += len(set(ann_rows) & set(exact_rows)) / 10.0
    assert total / len(queries) >= 0.95, total / len(queries)


def test_distances_are_the_sql_kernel_values(built):
    vec, queries, excluded, graph = built
    for q in queries[:5]:
        ann_rows, ann_d = V.ann_hnsw_search(graph, vec, q, 10, 64, None)
        exact_rows, exact_d = V.ann_exact_topk(vec, q, 10, excluded, None)
        exact = dict(zip(exact_rows, exact_d))
        for row, d in zip(ann_rows, ann_d):
            if row in exact:
                assert d == exact[row], (row, d, exact[row])   # bit-identical, same function
        assert ann_d == sorted(ann_d)


def test_unsearchable_and_deleted_rows_never_returned(built):
    vec, queries, excluded, graph = built
    banned = UNSEARCHABLE | DELETED
    for q in queries:
        ann_rows, _ = V.ann_hnsw_search(graph, vec, q, 50, 128, None)
        exact_rows, _ = V.ann_exact_topk(vec, q, 50, excluded, None)
        assert not banned & set(ann_rows)
        assert not banned & set(exact_rows)


def test_admitted_mask_filters_during_traversal(built):
    vec, queries, _, graph = built
    admitted_rows = set(range(0, N, 7))           # ~14% admitted
    admitted = _bitmap(admitted_rows, N)
    for q in queries[:10]:
        rows, _ = V.ann_hnsw_search(graph, vec, q, 10, 64, admitted)
        assert len(rows) == 10                     # filtered in the walk, not after it
        assert set(rows) <= admitted_rows


def test_zero_query_returns_nothing(built):
    vec, _, excluded, graph = built
    assert V.ann_hnsw_search(graph, vec, [0.0] * DIM, 10, 64, None) == ([], [])
    assert V.ann_exact_topk(vec, [0.0] * DIM, 10, excluded, None) == ([], [])


def test_graph_refuses_the_wrong_data(built):
    vec, queries, _, graph = built
    tampered = bytearray(graph)
    tampered[-1] ^= 1
    with pytest.raises(RuntimeError, match="checksum"):
        V.ann_hnsw_search(bytes(tampered), vec, queries[0], 10, 64, None)

    shorter = draken_native.vector_fp16_from_sequence([[1.0] * DIM] * (N - 1), DIM)
    with pytest.raises(RuntimeError, match="row count"):
        V.ann_hnsw_search(graph, shorter, queries[0], 10, 64, None)

    narrower = draken_native.vector_fp16_from_sequence([[1.0] * (DIM - 1)] * N, DIM - 1)
    with pytest.raises(ValueError, match="dimension"):
        V.ann_hnsw_search(graph, narrower, queries[0], 10, 64, None)

    with pytest.raises(RuntimeError, match="bad magic"):
        V.ann_hnsw_search(b"x" * len(graph), vec, queries[0], 10, 64, None)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
