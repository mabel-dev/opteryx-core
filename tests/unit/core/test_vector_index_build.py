"""Building one data file's vector index (src/cpp/engine/vector_index_build.hpp).

Runs on the core embedding capability (the static hash: native, deterministic, no model),
through the same `draken_embed` kernel a model-backed capability replaces.

What each test protects:
  * every row that is not null and not deleted is indexed exactly once, under its PHYSICAL
    ordinal (numbered before deletes), and nothing else is;
  * every vectors-file row group holds ONE cluster's rows, at most flush_rows of them, the
    centroids file lists each row group under exactly one cluster, and each row's cluster
    is its nearest centroid (cosine, ties to the lowest id);
  * the stored vector is the row's embedding (the kernel's own output for that text);
  * the build is deterministic whatever the embed threads, decode workers and batching
    window: byte-identical files;
  * the reported sizes are the files' sizes, and the logical bytes follow §5.5;
  * a file with nothing to index writes nothing; misuse fails loud and leaves no file;
  * a remote data file (an https URL that carries its own credential, as a signed URL does)
    builds the same files as the local one, through range GETs only - a signed GET URL
    cannot answer a HEAD, so its size must be given, and is refused without it.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import math

import pytest

import skene
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from draken.ops.kernels._kernel_registry import lookup_kernel
from rugo.parquet import write_parquet

from opteryx.operators._operators import build_vector_index_local
from opteryx.types.vectors.embedding_capability import active_embedding_capability

ROWS = 1000
ROW_GROUP = 128          # 8 row groups, the last partial
NULLS = {3, 200, 777}
DELETED = [0, 5, 129, 130, 640, 999]


def _texts():
    words = ["red", "planet", "gas", "giant", "ice", "moon", "ring", "storm", "dust", "orbit"]
    out = []
    for i in range(ROWS):
        out.append(None if i in NULLS else " ".join(words[(i * k) % 10] for k in (1, 3, 7)) + f" {i % 37}")
    return out


@pytest.fixture(scope="module")
def data_file(tmp_path_factory):
    morsel = Morsel()
    morsel.append_vector("id", vector_from_sequence(list(range(ROWS)), dtype="INTEGER"))
    morsel.append_vector("body", vector_from_sequence(_texts(), dtype="VARCHAR"))
    path = tmp_path_factory.mktemp("vidx") / "data.parquet"
    path.write_bytes(write_parquet(morsel, compression="zstd", max_rows_per_row_group=ROW_GROUP))
    return str(path)


def _embed():
    fn, _ = lookup_kernel("draken_embed")
    return fn, active_embedding_capability().dimensions


def _build(data_file, out_dir, **kwargs):
    fn, dims = _embed()
    vectors, centroids = str(out_dir / "v.skene"), str(out_dir / "c.skene")
    args = dict(clusters=0, flush_rows=16, embed_threads=1, decode_workers=1)
    args.update(kwargs)
    info = build_vector_index_local(data_file, "body", list(DELETED), fn, dims, vectors, centroids, **args)
    return info, vectors, centroids


def _read(path):
    data = open(path, "rb").read()
    meta = skene.read_metadata(data)
    morsels = [skene.read_morsel(data, g) for g in range(len(meta["row_groups"]))]
    for m in morsels:
        m.materialize()
    return data, morsels


def _cosine(a, b):
    return sum(x * y for x, y in zip(a, b)) / (math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))


def test_every_indexable_row_once_in_its_nearest_cluster(data_file, tmp_path):
    info, vectors_path, centroids_path = _build(data_file, tmp_path)
    dims = _embed()[1]
    _, groups = _read(vectors_path)
    _, (centroid_morsel,) = _read(centroids_path)

    centroids = centroid_morsel.column("centroid").to_pylist()
    assert all(len(c) == dims for c in centroids)
    lists = centroid_morsel.column("row_groups").to_pylist()
    counts = centroid_morsel.column("rows").to_pylist()
    owner = {g: c for c, gs in enumerate(lists) for g in gs}
    assert sorted(owner) == list(range(len(groups)))            # each row group, one cluster

    ordinals = []
    for g, morsel in enumerate(groups):
        rows = morsel.column("ordinal").to_pylist()
        assert 0 < len(rows) <= 16
        for vec in morsel.column("embedding").to_pylist():
            sims = [_cosine(vec, c) for c in centroids]
            # Its cluster is a nearest centroid (within float noise: the engine sums in a
            # fixed fp64 lane order, Python in its own).
            assert sims[owner[g]] >= max(sims) - 1e-9, f"row group {g}"
        ordinals.extend(rows)

    expected = sorted(set(range(ROWS)) - NULLS - set(DELETED))
    assert sorted(ordinals) == expected and len(ordinals) == len(set(ordinals))
    assert sum(counts) == len(expected) == info["rows_indexed"]
    assert [sum(len(groups[g].column("ordinal").to_pylist()) for g in gs) for gs in lists] == counts
    assert info["clusters"] == len(lists) == round(len(expected) ** 0.5)
    assert info["vectors_row_groups"] == len(groups)


def test_stored_vector_is_the_rows_embedding(data_file, tmp_path):
    """SQL COSINE_SIMILARITY(text, text) embeds both texts through the same kernel, so the
    cosine of two STORED vectors must be the similarity SQL computes for their texts."""
    import opteryx

    _, vectors_path, _ = _build(data_file, tmp_path)
    _, groups = _read(vectors_path)
    texts = _texts()
    stored = {}
    for morsel in groups:
        for o, v in zip(morsel.column("ordinal").to_pylist(), morsel.column("embedding").to_pylist()):
            stored[o] = v
    pairs = [(1, 2), (10, 500), (64, 65), (998, 131), (250, 251)]
    for a, b in pairs:
        ours = _cosine(stored[a], stored[b])
        result = list(
            opteryx.session().execute_to_morsels(
                f"SELECT COSINE_SIMILARITY('{texts[a]}', '{texts[b]}') AS s"
            )
        )
        (theirs,) = [x for m in result for x in m.column("s").to_pylist()]
        assert abs(ours - theirs) < 1e-6, (a, b, ours, theirs)


@pytest.mark.parametrize("threads, workers", [(4, 1), (1, 3), (3, 4)])
def test_build_is_deterministic(data_file, tmp_path, threads, workers):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    _, a_vec, a_cen = _build(data_file, tmp_path / "a")
    _, b_vec, b_cen = _build(data_file, tmp_path / "b", embed_threads=threads, decode_workers=workers)
    assert open(a_vec, "rb").read() == open(b_vec, "rb").read()
    assert open(a_cen, "rb").read() == open(b_cen, "rb").read()


def test_sizes_and_logical_bytes(data_file, tmp_path):
    info, vectors_path, centroids_path = _build(data_file, tmp_path)
    dims = _embed()[1]
    assert info["vectors_bytes"] == os.path.getsize(vectors_path)
    assert info["centroids_bytes"] == os.path.getsize(centroids_path)
    k, rows, groups = info["clusters"], info["rows_indexed"], info["vectors_row_groups"]
    assert info["logical_bytes"] == rows * (2 * dims + 4) + k * (2 * dims + 4) + 4 * groups + 4 * (k + 1)
    assert not [p for p in os.listdir(tmp_path) if p.endswith("-partial")]


def test_nothing_to_index_writes_nothing(tmp_path):
    morsel = Morsel()
    morsel.append_vector("body", vector_from_sequence([None, None, "kept but deleted"], dtype="VARCHAR"))
    path = tmp_path / "empty.parquet"
    path.write_bytes(write_parquet(morsel))
    fn, dims = _embed()
    vectors, centroids = str(tmp_path / "v.skene"), str(tmp_path / "c.skene")
    assert build_vector_index_local(str(path), "body", [2], fn, dims, vectors, centroids) is None
    assert sorted(os.listdir(tmp_path)) == ["empty.parquet"]


@pytest.mark.parametrize(
    "column, deleted, message",
    [
        ("nope", [], "has no column 'nope'"),
        ("id", [], "not text"),
        ("body", [5, 5], "ascending, unique"),
        ("body", [ROWS], "inside the file"),
    ],
)
def test_misuse_fails_and_leaves_no_file(data_file, tmp_path, column, deleted, message):
    fn, dims = _embed()
    vectors, centroids = str(tmp_path / "v.skene"), str(tmp_path / "c.skene")
    with pytest.raises(RuntimeError, match=message):
        build_vector_index_local(data_file, column, deleted, fn, dims, vectors, centroids)
    assert os.listdir(tmp_path) == []


def test_no_embedding_kernel_is_refused(data_file, tmp_path):
    with pytest.raises(ValueError, match="no embedding kernel"):
        build_vector_index_local(data_file, "body", [], 0, 256, str(tmp_path / "v"), str(tmp_path / "c"))


# --- a remote data file: read through range GETs (a signed URL in production) -------------


def _serve_ranges(payload, requests):
    from http.server import BaseHTTPRequestHandler
    from http.server import ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_HEAD(self):                   # a signed GET URL refuses every other method
            requests.append(("HEAD", None))
            self.send_response(403)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self):
            first, last = self.headers["Range"].removeprefix("bytes=").split("-")
            first, last = int(first), min(int(last), len(payload) - 1)
            requests.append(("GET", (first, last)))
            body = payload[first : last + 1]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {first}-{last}/{len(payload)}")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def test_a_remote_file_builds_the_same_files_through_range_gets(data_file, tmp_path):
    payload = open(data_file, "rb").read()
    requests = []
    server = _serve_ranges(payload, requests)
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}/data.parquet?X-Goog-Signature=x"
        (tmp_path / "local").mkdir()
        (tmp_path / "remote").mkdir()
        _, l_vec, l_cen = _build(data_file, tmp_path / "local")
        _, r_vec, r_cen = _build(url, tmp_path / "remote", data_bytes=len(payload))
    finally:
        server.shutdown()
    assert open(r_vec, "rb").read() == open(l_vec, "rb").read()
    assert open(r_cen, "rb").read() == open(l_cen, "rb").read()
    assert requests and all(method == "GET" for method, _ in requests)


def test_a_remote_file_needs_its_size(tmp_path):
    fn, dims = _embed()
    with pytest.raises(RuntimeError, match="needs its size"):
        build_vector_index_local(
            "https://storage.example/data.parquet", "body", [], fn, dims,
            str(tmp_path / "v"), str(tmp_path / "c"),
        )
    assert os.listdir(tmp_path) == []


# --- the GCS path: the body streamed into a resumable upload session -----------------------
#
# A local stand-in for GCS's resumable protocol, strict where GCS is strict: non-final chunks
# must be a multiple of 256 KiB and must start exactly where the session's held bytes end; the
# final chunk names the total. Faults are injected per request to prove the client resumes
# from what the SESSION says it holds.

import re
import threading
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer

_QUANTUM = 256 * 1024


class _Session:
    def __init__(self, faults=()):
        self.held = bytearray()
        self.finished = False
        self.faults = list(faults)      # per data PUT: None | ("status", code) | ("partial",)
        self.puts = 0
        self.lock = threading.Lock()


def _serve(session):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _reply(self, status, held=None):
            self.send_response(status)
            if held:
                self.send_header("Range", f"bytes=0-{held - 1}")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_PUT(self):
            body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
            content_range = self.headers["Content-Range"]
            with session.lock:
                if content_range == "bytes */*":                      # status query
                    return self._reply(200 if session.finished else 308, len(session.held))
                m = re.fullmatch(r"bytes (\*|(\d+)-(\d+))/(\*|\d+)", content_range)
                total = None if m.group(4) == "*" else int(m.group(4))
                if m.group(1) == "*":                                  # nothing left: finalise
                    if total == len(session.held):
                        session.finished = True
                        return self._reply(200)
                    return self._reply(400)
                start, last = int(m.group(2)), int(m.group(3))
                if start != len(session.held) or last - start + 1 != len(body):
                    return self._reply(400)
                if total is None and len(body) % _QUANTUM:
                    return self._reply(400)
                session.puts += 1
                fault = session.faults.pop(0) if session.faults else None
                if fault and fault[0] == "status":
                    return self._reply(fault[1])
                if fault and fault[0] == "partial":
                    # GCS persists a chunk in whole 256 KiB units: keep the first half.
                    session.held += body[: (len(body) // 2) // _QUANTUM * _QUANTUM]
                    return self._reply(308, len(session.held))
                session.held += body
                if total is not None and total == len(session.held):
                    session.finished = True
                    return self._reply(200)
                return self._reply(308, len(session.held))

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}/upload?upload_id=test"


@pytest.fixture(scope="module")
def big_file(tmp_path_factory):
    """Large enough (~2.6 MB of vectors) for several 512 KiB chunks."""
    words = ["red", "planet", "gas", "giant", "ice", "moon", "ring", "storm", "dust", "orbit"]
    texts = [" ".join(words[(i * k) % 10] for k in (1, 3, 7, 9)) + f" {i}" for i in range(5000)]
    morsel = Morsel()
    morsel.append_vector("body", vector_from_sequence(texts, dtype="VARCHAR"))
    path = tmp_path_factory.mktemp("vidx_big") / "big.parquet"
    path.write_bytes(write_parquet(morsel, compression="zstd", max_rows_per_row_group=1024))
    return str(path)


def _to_session(data_file, session, **kwargs):
    from opteryx.operators._operators import build_vector_index_to_session

    fn, dims = _embed()
    server, uri = _serve(session)
    try:
        args = dict(clusters=0, flush_rows=64, embed_threads=4, decode_workers=1, chunk_bytes=2 * _QUANTUM)
        args.update(kwargs)
        return build_vector_index_to_session(data_file, "body", [], fn, dims, uri, **args)
    finally:
        server.shutdown()


@pytest.mark.parametrize(
    "faults",
    [
        (),
        (("status", 503),),                       # a retryable failure: resend after asking
        (None, ("partial",)),                     # the session kept half a chunk: resend the rest
        (("partial",), ("partial",), None, ("status", 503)),
        (("status", 429), ("partial",), ("status", 500)),
    ],
)
def test_session_body_plus_prefix_is_the_local_file(big_file, tmp_path, faults):
    fn, dims = _embed()
    vectors_path, centroids_path = str(tmp_path / "v.skene"), str(tmp_path / "c.skene")
    build_vector_index_local(big_file, "body", [], fn, dims, vectors_path, centroids_path,
                             flush_rows=64, embed_threads=1, decode_workers=1)
    session = _Session(faults)
    info = _to_session(big_file, session)

    assert session.finished
    assert info["body_bytes"] == len(session.held)
    assert info["prefix"] + bytes(session.held) == open(vectors_path, "rb").read()
    assert info["centroids"] == open(centroids_path, "rb").read()
    assert info["vectors_bytes"] == os.path.getsize(vectors_path)
    assert session.puts >= 5                      # several chunks, not one shot


def test_session_refusal_fails_and_never_finishes(big_file):
    session = _Session([("status", 403)])
    with pytest.raises(RuntimeError, match="403"):
        _to_session(big_file, session)
    assert not session.finished


def test_session_with_nothing_to_index_is_never_finished(tmp_path):
    from opteryx.operators._operators import build_vector_index_to_session

    morsel = Morsel()
    morsel.append_vector("body", vector_from_sequence([None, None], dtype="VARCHAR"))
    path = tmp_path / "empty.parquet"
    path.write_bytes(write_parquet(morsel))
    fn, dims = _embed()
    session = _Session()
    server, uri = _serve(session)
    try:
        assert build_vector_index_to_session(str(path), "body", [], fn, dims, uri) is None
    finally:
        server.shutdown()
    assert not session.finished and session.puts == 0


def test_session_chunk_must_be_a_multiple_of_256_kib(big_file):
    with pytest.raises(RuntimeError, match="256 KiB"):
        _to_session(big_file, _Session(), chunk_bytes=_QUANTUM + 1)


# --- REFRESH INDEX's GCS orchestration (opteryx_connector._build_index_on_gcs) -------------
#
# Control plane around the native build: read the data file through a signed URL, stream the
# body into a session, upload the prefix and centroids, compose prefix + body into the
# vectors file and delete the two parts. A failed or empty build cancels its session.


class _FakeGcsIO:
    """The catalog GcsFileIO surface REFRESH INDEX uses, over objects in a dict; the
    resumable session is the strict stand-in above."""

    def __init__(self, faults=()):
        self.objects = {}
        self.session = _Session(faults)
        self.server, self.uri = _serve(self.session)
        self.body_path = None
        self.cancelled = []

    def open_upload_session(self, location):
        assert self.body_path is None
        self.body_path = location
        return self.uri

    def cancel_upload_session(self, uri):
        self.cancelled.append(uri)

    def new_output(self, path):
        io = self

        class _Out:
            def create(self):
                self.chunks = []
                return self

            def write(self, data):
                self.chunks.append(bytes(data))

            def close(self):
                io.objects[path] = b"".join(self.chunks)

        return _Out()

    def compose(self, sources, destination):
        if self.session.finished:
            self.objects[self.body_path] = bytes(self.session.held)
        self.objects[destination] = b"".join(self.objects[s] for s in sources)

    def delete(self, path):
        del self.objects[path]


def _gcs_build(monkeypatch, data_file, io):
    from types import SimpleNamespace

    import opteryx.connectors.io_systems.gcs_filesystem as gcs_filesystem
    from opteryx.connectors.opteryx_connector import _build_index_on_gcs
    from opteryx.operators._operators import build_vector_index_to_session

    payload = open(data_file, "rb").read()
    reads = []
    reader = _serve_ranges(payload, reads)
    signed = []

    class _Signer:
        def rewrite_to_signed_url(self, path, expiry_seconds):
            signed.append((path, expiry_seconds))
            return f"http://127.0.0.1:{reader.server_address[1]}/signed"

    monkeypatch.setattr(gcs_filesystem, "OpteryxGcsFileSystem", _Signer)
    task = SimpleNamespace(
        data_file="gs://bucket/t/data/f.parquet", data_bytes=len(payload), deleted=(),
        vectors="gs://bucket/t/index/i/f-1.vectors.skene", centroids="gs://bucket/t/index/i/f-1.centroids.skene",
    )
    fn, dims = _embed()
    options = dict(clusters=0, flush_rows=64, embed_threads=4, decode_workers=1, chunk_bytes=2 * _QUANTUM)
    try:
        return task, signed, _build_index_on_gcs(io, task, "body", fn, dims, options, build_vector_index_to_session)
    finally:
        reader.shutdown()
        io.server.shutdown()


def test_gcs_build_composes_the_vectors_file_and_cleans_up(monkeypatch, big_file, tmp_path):
    fn, dims = _embed()
    vectors_path, centroids_path = str(tmp_path / "v.skene"), str(tmp_path / "c.skene")
    build_vector_index_local(big_file, "body", [], fn, dims, vectors_path, centroids_path,
                             flush_rows=64, embed_threads=1, decode_workers=1)
    io = _FakeGcsIO()
    task, signed, built = _gcs_build(monkeypatch, big_file, io)

    assert signed == [(task.data_file, 7 * 24 * 3600)]           # one URL, the longest life
    assert io.body_path == f"{task.vectors}.body"
    assert sorted(io.objects) == sorted([task.vectors, task.centroids])   # parts deleted
    assert io.objects[task.vectors] == open(vectors_path, "rb").read()
    assert io.objects[task.centroids] == open(centroids_path, "rb").read()
    assert built["vectors_bytes"] == len(io.objects[task.vectors])
    assert io.cancelled == []


def test_gcs_build_failure_cancels_the_session_and_writes_nothing(monkeypatch, big_file):
    io = _FakeGcsIO([("status", 403)])
    with pytest.raises(RuntimeError, match="403"):
        _gcs_build(monkeypatch, big_file, io)
    assert io.cancelled == [io.uri] and io.objects == {}


def test_gcs_build_with_nothing_to_index_cancels_and_writes_nothing(monkeypatch, tmp_path):
    morsel = Morsel()
    morsel.append_vector("body", vector_from_sequence([None, None], dtype="VARCHAR"))
    path = tmp_path / "empty.parquet"
    path.write_bytes(write_parquet(morsel))
    io = _FakeGcsIO()
    _, _, built = _gcs_build(monkeypatch, str(path), io)
    assert built is None
    assert io.cancelled == [io.uri] and io.objects == {} and io.session.puts == 0
