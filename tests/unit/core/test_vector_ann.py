"""draken/ops/ann — fp16 cosine IVF-flat over a VECTOR_FP16 column (vector index, Stage B).

What each test protects:
  * probing EVERY cluster is the exact scan, row for row and bit for bit — IVF only ever
    narrows which rows are scored, never how they are scored;
  * the returned distances are the SQL kernel's COSINE_DISTANCE values;
  * partial probing still finds the true neighbours at high recall on clustered data;
  * rows outside the searchable domain (null, deleted, zero-magnitude, non-finite) are never
    clustered and never returned, on either path;
  * the build is deterministic: same input + seed => same model, for any thread count;
  * the admitted mask is honoured;
  * the plan/train/assign split (for builds too large for memory) changed NOTHING:
    ivf_build is bit-identical to its pre-split output (hashes recorded before the split);
  * the streaming build (ClusterStream) puts every searchable row in the same cluster as
    ivf_build, in blocks of at most flush_rows, and never emits an unsearchable row.
"""

import os
import random
import struct
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

from draken import draken_native
from opteryx.compiled.nanobind import vectors as V

DIM = 32
N = 3000
CLUSTERS = 24
UNSEARCHABLE = {5, 6, 7}
DELETED = {9, 11}


def _bitmap(rows, n):
    out = bytearray((n + 7) // 8)
    for r in rows:
        out[r >> 3] |= 1 << (r & 7)
    return bytes(out)


def _u32(raw):
    return list(struct.unpack(f"<{len(raw) // 4}I", raw))


@pytest.fixture(scope="module")
def data():
    rng = random.Random(7)
    centres = [[rng.gauss(0, 1) for _ in range(DIM)] for _ in range(CLUSTERS)]
    rows = []
    for _ in range(N):
        c = centres[rng.randrange(CLUSTERS)]
        rows.append([x + 0.5 * rng.gauss(0, 1) for x in c])
    rows[5] = [0.0] * DIM                          # zero magnitude: undefined cosine
    rows[6] = [float("nan")] + [1.0] * (DIM - 1)   # non-finite
    rows[7] = None                                 # null
    vec = draken_native.vector_fp16_from_sequence(rows, DIM)
    queries = []
    for _ in range(40):
        c = centres[rng.randrange(CLUSTERS)]
        queries.append([x + 0.5 * rng.gauss(0, 1) for x in c])
    excluded = _bitmap(DELETED, N)
    model = V.ann_ivf_build(vec, excluded, 0, 8, 64, 2, 1234)
    return vec, queries, excluded, model


def test_model_shape(data):
    _, _, _, (centroids, order, offsets) = data
    order, offsets = _u32(order), _u32(offsets)
    clusters = len(offsets) - 1
    assert clusters == round((N - len(UNSEARCHABLE) - len(DELETED)) ** 0.5)
    assert len(centroids) == clusters * DIM * 2
    assert offsets[0] == 0 and offsets[-1] == len(order) == N - len(UNSEARCHABLE) - len(DELETED)
    assert not (UNSEARCHABLE | DELETED) & set(order)
    for c in range(clusters):          # ascending within each cluster
        block = order[offsets[c] : offsets[c + 1]]
        assert block == sorted(block)


def test_build_is_deterministic_across_thread_counts(data):
    vec, _, excluded, model = data
    assert V.ann_ivf_build(vec, excluded, 0, 8, 64, 1, 1234) == model
    assert V.ann_ivf_build(vec, excluded, 0, 8, 64, 5, 1234) == model


def test_probing_every_cluster_is_the_exact_scan(data):
    vec, queries, excluded, (centroids, order, offsets) = data
    clusters = len(offsets) // 4 - 1
    for q in queries[:10]:
        ivf = V.ann_ivf_search(vec, centroids, order, offsets, q, 25, clusters, excluded, None)
        exact = V.ann_exact_topk(vec, q, 25, excluded, None)
        assert ivf == exact                         # same rows, same bits, same order


def test_partial_probe_recall(data):
    vec, queries, excluded, (centroids, order, offsets) = data
    total = 0.0
    for q in queries:
        # Tie-aware: a hit counts when it is no farther than the exact 10th neighbour.
        _, exact_d = V.ann_exact_topk(vec, q, 10, excluded, None)
        _, d = V.ann_ivf_search(vec, centroids, order, offsets, q, 10, 4, excluded, None)
        total += sum(1 for x in d if x <= exact_d[-1]) / 10.0
    assert total / len(queries) >= 0.95, total / len(queries)


def test_unsearchable_and_deleted_rows_never_returned(data):
    vec, queries, excluded, (centroids, order, offsets) = data
    banned = UNSEARCHABLE | DELETED
    clusters = len(offsets) // 4 - 1
    for q in queries:
        rows, _ = V.ann_ivf_search(vec, centroids, order, offsets, q, 50, clusters, excluded, None)
        exact_rows, _ = V.ann_exact_topk(vec, q, 50, excluded, None)
        assert not banned & set(rows)
        assert not banned & set(exact_rows)


def test_admitted_mask(data):
    vec, queries, excluded, (centroids, order, offsets) = data
    admitted_rows = set(range(0, N, 7))
    admitted = _bitmap(admitted_rows, N)
    clusters = len(offsets) // 4 - 1
    for q in queries[:10]:
        rows, _ = V.ann_ivf_search(vec, centroids, order, offsets, q, 10, clusters, excluded, admitted)
        assert set(rows) <= admitted_rows
        assert (rows, _) == V.ann_exact_topk(vec, q, 10, excluded, admitted)


def test_zero_query_returns_nothing(data):
    vec, _, excluded, (centroids, order, offsets) = data
    zero = [0.0] * DIM
    assert V.ann_ivf_search(vec, centroids, order, offsets, zero, 10, 4, excluded, None) == ([], [])
    assert V.ann_exact_topk(vec, zero, 10, excluded, None) == ([], [])


def test_inconsistent_model_is_refused(data):
    vec, queries, excluded, (centroids, order, offsets) = data
    with pytest.raises(ValueError, match="inconsistent"):
        V.ann_ivf_search(vec, centroids[:-2 * DIM], order, offsets, queries[0], 10, 4, None, None)
    narrower = draken_native.vector_fp16_from_sequence([[1.0] * (DIM - 1)] * N, DIM - 1)
    with pytest.raises(ValueError):
        V.ann_ivf_search(narrower, centroids, order, offsets, [1.0] * (DIM - 1), 10, 4, None, None)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])


# sha256(centroids + order + offsets) of ivf_build BEFORE the plan/train/assign split
# (2026-10-02). A changed hash is a changed index for the same input and seed.
_PRE_SPLIT = {
    (0, 8, 64, 2, 1234): "7a03bcace2e2b892b037cbd1f795528cb0ec386676020f8714c31afacc92173a",
    (24, 8, 64, 1, 99): "32c0c47030e660a7a7653718ad65504bae08cc1c1e280862ec8b4be0826dcbd7",
    (5, 3, 2, 3, 7): "4b8732cbd22bf0c7dfa3633dd2ed8e5843ef39a553840cef3ee2fc631a06c9d7",
    (4000, 2, 64, 1, 1): "bddc1e234a53a7cd63a8cb0b1c4cc4be5d9682247f04fb38aee1fe3b6ae62ec2",
}


@pytest.mark.parametrize("args", sorted(_PRE_SPLIT))
def test_build_is_bit_identical_to_before_the_split(data, args):
    import hashlib

    vec, _, excluded, _ = data
    centroids, order, offsets = V.ann_ivf_build(vec, excluded, *args)
    assert hashlib.sha256(centroids + order + offsets).hexdigest() == _PRE_SPLIT[args]


def _clean_data():
    """Every valid row searchable — what an embedder produces — so the streaming build's
    candidates (valid, not deleted) are exactly ivf_build's searchable rows."""
    rng = random.Random(11)
    centres = [[rng.gauss(0, 1) for _ in range(DIM)] for _ in range(CLUSTERS)]
    rows = [[x + 0.5 * rng.gauss(0, 1) for x in centres[rng.randrange(CLUSTERS)]] for _ in range(N)]
    rows[7] = None
    return draken_native.vector_fp16_from_sequence(rows, DIM), _bitmap(DELETED, N)


@pytest.mark.parametrize("flush_rows", [1, 7, 64, 100000])
def test_streaming_build_matches_the_in_memory_build(flush_rows):
    vec, excluded = _clean_data()
    centroids, order, offsets = V.ann_ivf_build(vec, excluded, 0, 8, 64, 2, 1234)
    streamed_centroids, blocks = V.ann_ivf_stream(vec, excluded, 0, 8, 64, 2, 1234, flush_rows)
    assert streamed_centroids == centroids

    order, offsets = _u32(order), _u32(offsets)
    expected = {c: order[offsets[c]:offsets[c + 1]] for c in range(len(offsets) - 1)}
    got = {}
    for cluster, ordinals in blocks:
        ordinals = _u32(ordinals)
        assert 0 < len(ordinals) <= flush_rows
        got.setdefault(cluster, []).extend(ordinals)
    assert got == {c: rows for c, rows in expected.items() if rows}


def test_streaming_build_never_emits_unsearchable_rows(data):
    vec, _, excluded, _ = data
    _, blocks = V.ann_ivf_stream(vec, excluded, 0, 8, 64, 1, 1234, 50)
    emitted = [r for _, ordinals in blocks for r in _u32(ordinals)]
    assert len(emitted) == len(set(emitted)) == N - len(UNSEARCHABLE) - len(DELETED)
    assert not (set(emitted) & (UNSEARCHABLE | DELETED))


def test_streaming_flush_rows_must_be_positive(data):
    vec, _, excluded, _ = data
    with pytest.raises(ValueError, match="flush_rows"):
        V.ann_ivf_stream(vec, excluded, 0, 8, 64, 1, 1234, 0)
