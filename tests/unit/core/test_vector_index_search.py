"""Searching one data file's vector index (src/cpp/engine/vector_index_search.hpp, §8 D2).

Index files are built on the core static-hash embedder (native, deterministic), and the
query embeds through the same kernel. What each test protects:
  * probing every cluster IS exact search: the result equals the exact top-k over every
    stored vector (cosine, ties by ordinal), distances included;
  * probing fewer clusters reads only their row groups of the vectors file, and every hit
    it returns carries its exact distance;
  * deleted ordinals are never returned;
  * a remote index (signed URL, range GETs only) answers exactly as the local one;
  * an index file that disagrees with its data file fails loud.
"""

import math
import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(1, os.path.dirname(__file__))

from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo.parquet import write_parquet

from opteryx.operators._operators import build_vector_index_local
from opteryx.operators._operators import search_vector_index_file

from test_vector_index_build import _embed
from test_vector_index_build import _read
from test_vector_index_build import _serve_ranges

WORDS = ["red", "planet", "gas", "giant", "ice", "moon", "ring", "storm", "dust", "orbit", "comet", "sun"]
ROWS = 1500
QUERY = "storm moon ring"


def _texts():
    return [None if i % 53 == 9 else " ".join(WORDS[(i * k + i // 7) % 12] for k in (1, 5, 7)) + f" {i % 11}"
            for i in range(ROWS)]


@pytest.fixture(scope="module")
def index(tmp_path_factory):
    base = tmp_path_factory.mktemp("search")
    fn, dims = _embed()
    morsel = Morsel()
    morsel.append_vector("body", vector_from_sequence(_texts(), dtype="VARCHAR"))
    data = base / "d.parquet"
    data.write_bytes(write_parquet(morsel, max_rows_per_row_group=256))
    vectors, centroids = str(base / "v.skene"), str(base / "c.skene")
    info = build_vector_index_local(str(data), "body", [], fn, dims, vectors, centroids, flush_rows=32)
    # The query's own vector, through the same kernel: index a one-row file holding it.
    q = Morsel()
    q.append_vector("body", vector_from_sequence([QUERY], dtype="VARCHAR"))
    qdata = base / "q.parquet"
    qdata.write_bytes(write_parquet(q))
    build_vector_index_local(str(qdata), "body", [], fn, dims, str(base / "qv"), str(base / "qc"))
    (query_vector,) = _stored(str(base / "qv")).values()
    return {
        "vectors": vectors, "centroids": centroids, "info": info, "query": query_vector,
        "stored": _stored(vectors),
    }


def _stored(path):
    _, groups = _read(path)
    out = {}
    for g in groups:
        out.update(zip(g.column("ordinal").to_pylist(), g.column("embedding").to_pylist()))
    return out


def _cosine_distance(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    return 1.0 - dot / (math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))


def _search(index, k, nprobe, deleted=None, vectors=None, centroids=None):
    fn, dims = _embed()
    return search_vector_index_file(
        vectors or index["vectors"], os.path.getsize(index["vectors"]),
        centroids or index["centroids"], os.path.getsize(index["centroids"]),
        QUERY, fn, dims, k, nprobe, ROWS, deleted,
    )


def _exact(index, k, deleted=()):
    scored = sorted(
        (_cosine_distance(index["query"], v), o) for o, v in index["stored"].items() if o not in deleted
    )
    return scored[:k]


def test_probing_every_cluster_is_exact_search(index):
    clusters = index["info"]["clusters"]
    hits, stats = _search(index, 25, clusters)
    assert stats["probed"] == clusters
    expected = _exact(index, 25)
    assert [o for o, _ in hits] == [o for _, o in expected]
    for (_, got), (want, _) in zip(hits, expected):
        assert got == pytest.approx(want, abs=2e-3)       # fp16 storage


def test_a_narrow_probe_reads_only_its_clusters(index):
    hits, stats = _search(index, 10, 2)
    assert stats["probed"] == 2
    assert stats["row_groups_read"] < index["info"]["vectors_row_groups"]
    assert stats["rows_scored"] < len(index["stored"])
    distances = [d for _, d in hits]
    assert distances == sorted(distances)
    for ordinal, distance in hits:
        assert distance == pytest.approx(_cosine_distance(index["query"], index["stored"][ordinal]), abs=2e-3)


def test_deleted_rows_are_never_returned(index):
    clusters = index["info"]["clusters"]
    best = [o for _, o in _exact(index, 5)]
    hits, _ = _search(index, 25, clusters, deleted=sorted(best))
    assert not set(best) & {o for o, _ in hits}
    assert [o for o, _ in hits] == [o for _, o in _exact(index, 25, deleted=set(best))]


def test_a_remote_index_answers_as_the_local_one(index):
    requests = []
    servers = [_serve_ranges(open(p, "rb").read(), requests) for p in (index["vectors"], index["centroids"])]
    try:
        urls = [f"http://127.0.0.1:{s.server_address[1]}/f?X-Goog-Signature=x" for s in servers]
        remote = _search(index, 10, 3, vectors=urls[0], centroids=urls[1])
    finally:
        for s in servers:
            s.shutdown()
    assert remote == _search(index, 10, 3)
    assert requests and all(method == "GET" for method, _ in requests)


def test_an_index_that_overruns_its_data_file_fails_loud(index):
    fn, dims = _embed()
    with pytest.raises(RuntimeError, match="beyond its data file"):
        search_vector_index_file(
            index["vectors"], os.path.getsize(index["vectors"]), index["centroids"],
            os.path.getsize(index["centroids"]), QUERY, fn, dims, 10, index["info"]["clusters"], 100,
        )
