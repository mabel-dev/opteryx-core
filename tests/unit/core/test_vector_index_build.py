"""Building one data file's vector index (src/cpp/engine/vector_index_build.hpp, design §5.2).

Runs on the core embedding capability (the deterministic static-hash embedder the engine
ships with), so the build is exercised natively end to end: parquet decode, embedding,
k-means, cluster assignment, the index file. What each test protects:
  * every indexable row (not null, not deleted) is written exactly once, under its PHYSICAL
    ordinal, into a block of its nearest centroid's cluster;
  * the stored vector IS the row's embedding (the cosine of two stored vectors equals the
    similarity SQL computes for their texts);
  * the build is deterministic across thread counts;
  * the sizes the commit records are the file's; a file with nothing to index is not written;
  * misuse fails and leaves no file; a remote data file is read through range GETs;
  * the GCS path streams the whole file into ONE resumable session, finished natively.

`read_vidx` is the format's reference reader (vector_index_file.hpp), in plain Python, for
every test here and in the search, carry and integration suites.
"""

import os
import struct
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import math

import pytest

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

VIDX_MAGIC = 0x58444956
VIDX_TAIL = 24


def read_vidx(path):
    """The index file, decoded: dims, the centroids, and its blocks as
    (cluster, [ordinals], [vectors]) in file order. Checks the layout the way the native
    reader does (tail, footer length, body length) so a malformed file fails here too."""
    data = open(path, "rb").read()
    footer_bytes, _checksum, version, magic = struct.unpack_from("<QQII", data, len(data) - VIDX_TAIL)
    assert magic == VIDX_MAGIC and version == 1, path
    footer = data[len(data) - VIDX_TAIL - footer_bytes : len(data) - VIDX_TAIL]
    dims, clusters, blocks, _reserved, rows = struct.unpack_from("<IIIIQ", footer, 0)
    at = 24
    centroids = [list(struct.unpack_from(f"<{dims}e", footer, at + c * dims * 2)) for c in range(clusters)]
    at += clusters * dims * 2
    table = [struct.unpack_from("<II", footer, at + b * 8) for b in range(blocks)]
    assert at + blocks * 8 == footer_bytes
    out, pos, total = [], 0, 0
    for cluster, n in table:
        ordinals = list(struct.unpack_from(f"<{n}I", data, pos))
        pos += 4 * n
        vectors = [list(struct.unpack_from(f"<{dims}e", data, pos + i * dims * 2)) for i in range(n)]
        pos += 2 * n * dims
        out.append((cluster, ordinals, vectors))
        total += n
    assert total == rows and pos == len(data) - VIDX_TAIL - footer_bytes, path
    return {"dims": dims, "centroids": centroids, "blocks": out, "footer_bytes": footer_bytes, "rows": rows}


def stored_vectors(path):
    """{ordinal: vector} of an index file."""
    out = {}
    for _, ordinals, vectors in read_vidx(path)["blocks"]:
        out.update(zip(ordinals, vectors))
    return out


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
    path = str(out_dir / "i.vidx")
    args = dict(clusters=0, flush_rows=16, embed_threads=1, decode_workers=1)
    args.update(kwargs)
    info = build_vector_index_local(data_file, "body", list(DELETED), fn, dims, path, **args)
    return info, path


def _cosine(a, b):
    return sum(x * y for x, y in zip(a, b)) / (math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))


def test_every_indexable_row_once_in_its_nearest_cluster(data_file, tmp_path):
    info, path = _build(data_file, tmp_path)
    dims = _embed()[1]
    index = read_vidx(path)
    assert index["dims"] == dims
    centroids = index["centroids"]

    ordinals = []
    for cluster, block_ordinals, vectors in index["blocks"]:
        assert 0 < len(block_ordinals) <= 16
        for vec in vectors:
            sims = [_cosine(vec, c) for c in centroids]
            # Its cluster is a nearest centroid (within float noise: the engine sums in a
            # fixed fp64 lane order, Python in its own).
            assert sims[cluster] >= max(sims) - 1e-9, cluster
        ordinals.extend(block_ordinals)

    expected = sorted(set(range(ROWS)) - NULLS - set(DELETED))
    assert sorted(ordinals) == expected and len(ordinals) == len(set(ordinals))
    assert index["rows"] == len(expected) == info["rows_indexed"]
    assert info["clusters"] == len(centroids) == round(len(expected) ** 0.5)
    assert info["blocks"] == len(index["blocks"])


def test_stored_vector_is_the_rows_embedding(data_file, tmp_path):
    """SQL COSINE_SIMILARITY(text, text) embeds both texts through the same kernel, so the
    cosine of two STORED vectors must be the similarity SQL computes for their texts."""
    import opteryx

    _, path = _build(data_file, tmp_path)
    stored = stored_vectors(path)
    texts = _texts()
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
    _, a = _build(data_file, tmp_path / "a")
    _, b = _build(data_file, tmp_path / "b", embed_threads=threads, decode_workers=workers)
    assert open(a, "rb").read() == open(b, "rb").read()


def test_sizes_and_logical_bytes(data_file, tmp_path):
    info, path = _build(data_file, tmp_path)
    dims = _embed()[1]
    index = read_vidx(path)
    assert info["file_bytes"] == os.path.getsize(path)
    assert info["footer_bytes"] == index["footer_bytes"] == 24 + 2 * dims * info["clusters"] + 8 * info["blocks"]
    # The format is its own decoded form: the billed size is the file.
    assert info["logical_bytes"] == info["file_bytes"]
    assert info["file_bytes"] == info["rows_indexed"] * (2 * dims + 4) + info["footer_bytes"] + VIDX_TAIL
    assert os.listdir(tmp_path) == ["i.vidx"]                       # no partial left behind


def test_nothing_to_index_writes_nothing(tmp_path):
    morsel = Morsel()
    morsel.append_vector("body", vector_from_sequence([None, None, "kept but deleted"], dtype="VARCHAR"))
    path = tmp_path / "empty.parquet"
    path.write_bytes(write_parquet(morsel))
    fn, dims = _embed()
    assert build_vector_index_local(str(path), "body", [2], fn, dims, str(tmp_path / "i.vidx")) is None
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
    with pytest.raises(RuntimeError, match=message):
        build_vector_index_local(data_file, column, deleted, fn, dims, str(tmp_path / "i.vidx"))
    assert os.listdir(tmp_path) == []


def test_no_embedding_kernel_is_refused(data_file, tmp_path):
    with pytest.raises(ValueError, match="no embedding kernel"):
        build_vector_index_local(data_file, "body", [], 0, 256, str(tmp_path / "i.vidx"))


# --- a remote data file: read through range GETs (gs:// + bearer header in production) ----

import threading
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer


def _serve_ranges(payload, requests):
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
            if first == "":                  # a suffix range: the last `last` bytes
                first, last = max(0, len(payload) - int(last)), len(payload) - 1
            else:
                first, last = int(first), min(int(last), len(payload) - 1)
            requests.append(("GET", (first, last), self.headers.get("Authorization")))
            body = payload[first : last + 1]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {first}-{last}/{len(payload)}")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def test_a_remote_file_builds_the_same_file_through_range_gets(data_file, tmp_path):
    payload = open(data_file, "rb").read()
    requests = []
    server = _serve_ranges(payload, requests)
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}/data.parquet?X-Goog-Signature=x"
        (tmp_path / "local").mkdir()
        (tmp_path / "remote").mkdir()
        _, local = _build(data_file, tmp_path / "local")
        _, remote = _build(url, tmp_path / "remote", data_bytes=len(payload))
    finally:
        server.shutdown()
    assert open(remote, "rb").read() == open(local, "rb").read()
    assert requests and all(method == "GET" for method, *_ in requests)


def test_a_remote_file_is_read_with_the_given_authorization_header(data_file, tmp_path):
    payload = open(data_file, "rb").read()
    requests = []
    server = _serve_ranges(payload, requests)
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}/data.parquet"
        (tmp_path / "local").mkdir()
        (tmp_path / "remote").mkdir()
        _, local = _build(data_file, tmp_path / "local")
        _, remote = _build(url, tmp_path / "remote", data_bytes=len(payload), auth_header="Bearer t0k3n")
    finally:
        server.shutdown()
    assert open(remote, "rb").read() == open(local, "rb").read()
    # The footer and every row group alike.
    assert requests and all(auth == "Bearer t0k3n" for _, _, auth in requests)


def test_a_gcs_data_file_without_an_authorization_header_is_refused(tmp_path):
    fn, dims = _embed()
    with pytest.raises(RuntimeError, match="no Authorization header"):
        build_vector_index_local("gs://bucket/data.parquet", "body", [], fn, dims, str(tmp_path / "i.vidx"),
                                 data_bytes=1024)
    assert os.listdir(tmp_path) == []


def test_a_remote_file_needs_its_size(tmp_path):
    fn, dims = _embed()
    with pytest.raises(RuntimeError, match="needs its size"):
        build_vector_index_local("https://storage.example/data.parquet", "body", [], fn, dims,
                                 str(tmp_path / "i.vidx"))
    assert os.listdir(tmp_path) == []


# --- the GCS path: the file streamed into a resumable upload session -----------------------
#
# A local stand-in for GCS's resumable protocol, strict where GCS is strict: non-final chunks
# must be a multiple of 256 KiB and must start exactly where the session's held bytes end; the
# final chunk names the total. Faults are injected per request to prove the client resumes
# from what the SESSION says it holds.

import re

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
def test_the_session_holds_the_local_file(big_file, tmp_path, faults):
    fn, dims = _embed()
    path = str(tmp_path / "i.vidx")
    build_vector_index_local(big_file, "body", [], fn, dims, path, flush_rows=64, embed_threads=1, decode_workers=1)
    session = _Session(faults)
    info = _to_session(big_file, session)

    assert session.finished
    assert bytes(session.held) == open(path, "rb").read()
    assert info["file_bytes"] == len(session.held) == os.path.getsize(path)
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
# Control plane around the native build: read the data file with a bearer header, open ONE
# resumable session for the index file, let the build stream and finish it. A failed or
# empty build cancels its session. Nothing is composed and nothing else is written.


class _FakeGcsIO:
    """The catalog GcsFileIO surface REFRESH INDEX uses; the resumable session is the strict
    stand-in above. The object exists exactly when the session is finished."""

    def __init__(self, faults=()):
        self.session = _Session(faults)
        self.server, self.uri = _serve(self.session)
        self.opened = []
        self.cancelled = []

    def open_upload_session(self, location):
        self.opened.append(location)
        return self.uri

    def cancel_upload_session(self, uri):
        self.cancelled.append(uri)

    def new_output(self, path):                   # never called: nothing but the session is written
        raise AssertionError(f"the build wrote {path} outside its session")

    @property
    def objects(self):
        return {self.opened[0]: bytes(self.session.held)} if self.session.finished else {}


def _gcs_build(monkeypatch, data_file, io):
    from types import SimpleNamespace

    import opteryx.connectors.opteryx_connector as opteryx_connector
    from opteryx.connectors.opteryx_connector import _build_index_on_gcs
    from opteryx.operators._operators import build_vector_index_to_session

    payload = open(data_file, "rb").read()
    reads = []
    reader = _serve_ranges(payload, reads)
    mapped = []

    def _index_reads():
        def readable(path):
            mapped.append(path)
            return f"http://127.0.0.1:{reader.server_address[1]}/f.parquet", "Bearer t0k3n"
        return readable

    monkeypatch.setattr(opteryx_connector, "_index_reads", _index_reads)
    task = SimpleNamespace(
        data_file="gs://bucket/t/data/f.parquet", data_bytes=len(payload), deleted=(),
        path="gs://bucket/t/index/i/f-1.vidx",
    )
    fn, dims = _embed()
    options = dict(clusters=0, flush_rows=64, embed_threads=4, decode_workers=1, chunk_bytes=2 * _QUANTUM)
    try:
        return task, mapped, reads, _build_index_on_gcs(io, task, "body", fn, dims, options, build_vector_index_to_session)
    finally:
        reader.shutdown()
        io.server.shutdown()


def test_gcs_build_streams_one_object_and_finishes_it(monkeypatch, big_file, tmp_path):
    fn, dims = _embed()
    path = str(tmp_path / "i.vidx")
    build_vector_index_local(big_file, "body", [], fn, dims, path, flush_rows=64, embed_threads=1, decode_workers=1)
    io = _FakeGcsIO()
    task, mapped, reads, built = _gcs_build(monkeypatch, big_file, io)

    assert mapped == [task.data_file]                             # never signed: a bearer header
    assert reads and all(auth == "Bearer t0k3n" for _, _, auth in reads)
    assert io.opened == [task.path]                               # the index file itself, no parts
    assert io.objects == {task.path: open(path, "rb").read()}
    assert built["file_bytes"] == len(io.objects[task.path])
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
    _, _, _, built = _gcs_build(monkeypatch, str(path), io)
    assert built is None
    assert io.cancelled == [io.uri] and io.objects == {} and io.session.puts == 0
