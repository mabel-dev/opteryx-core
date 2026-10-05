"""Compaction's vector carry (src/cpp/engine/vector_index_carry.hpp, design §5.6).

Compaction never embeds: an output's index is re-clustered from the vectors its inputs'
index files already hold, routed by the writer's row-origin map. Runs on the core
static-hash embedder to build the INPUT indexes; the carry itself uses no model.

What each test protects:
  * every carried row lands under its OUTPUT ordinal holding exactly the vector its input
    held for it; rows the inputs did not index stay unindexed; outputs are independent;
  * the invariant: an indexed input row that is live but was not written fails the carry
    (it would be a row the compaction lost); a DELETED one is let go;
  * a row written twice is refused;
  * the RowOriginRecorder takes the origin columns out of the row group natively;
  * remote inputs (bearer header, range GETs) carry to the same file as local ones.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(1, os.path.dirname(__file__))

from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo.parquet import write_parquet

from opteryx.operators._operators import RowOriginRecorder
from opteryx.operators._operators import build_vector_index_local
from opteryx.operators._operators import carry_vector_index_local

from test_vector_index_build import _embed
from test_vector_index_build import _serve_ranges
from test_vector_index_build import stored_vectors

WORDS = ["red", "planet", "gas", "giant", "ice", "moon", "ring", "storm", "dust", "orbit"]


def _texts(n, salt):
    return [None if i % 41 == 7 else " ".join(WORDS[(i * k + salt) % 10] for k in (1, 3, 7)) + f" {salt}-{i % 29}"
            for i in range(n)]


@pytest.fixture(scope="module")
def inputs(tmp_path_factory):
    """Two indexed data files: (index path, (file bytes, footer bytes), {ordinal: vector}, rows)."""
    base = tmp_path_factory.mktemp("carry")
    fn, dims = _embed()
    out = []
    for k, rows in enumerate((700, 450)):
        morsel = Morsel()
        morsel.append_vector("body", vector_from_sequence(_texts(rows, k), dtype="VARCHAR"))
        data = base / f"d{k}.parquet"
        data.write_bytes(write_parquet(morsel, max_rows_per_row_group=128))
        path = str(base / f"i{k}.vidx")
        info = build_vector_index_local(str(data), "body", [], fn, dims, path, flush_rows=32)
        out.append((path, (info["file_bytes"], info["footer_bytes"]), stored_vectors(path), rows))
    return out


def _recorder(origins, name="x"):
    """A recorder fed the way the compaction writer feeds it: row groups carrying the
    origin columns, which it records and strips."""
    recorder = RowOriginRecorder("$carry_file", "$carry_ordinal")
    for start in range(0, len(origins), 100):
        chunk = origins[start:start + 100]
        morsel = Morsel()
        morsel.append_vector("payload", vector_from_sequence([f"{name}{i}" for i in range(len(chunk))], dtype="VARCHAR"))
        morsel.append_vector("$carry_file", vector_from_sequence([f for f, _ in chunk], dtype="INT64"))
        morsel.append_vector("$carry_ordinal", vector_from_sequence([o for _, o in chunk], dtype="INT64"))
        stripped = recorder.take(morsel)
        assert stripped.column_names == [b"payload"]
    assert recorder.rows == len(origins)
    return recorder


def _carry(tmp_path, inputs, outputs, deleted=((), ()), locations=None, auth_header="", **kwargs):
    _, dims = _embed()
    specs = [
        (locations[k] if locations else path, size, footer, list(deleted[k]), auth_header)
        for k, (path, (size, footer), _, _) in enumerate(inputs)
    ]
    paths = [str(tmp_path / f"out{j}.vidx") for j in range(len(outputs))]
    results = carry_vector_index_local(
        specs, [_recorder(o, f"o{j}") for j, o in enumerate(outputs)], dims, paths, flush_rows=16, **kwargs,
    )
    return results, paths


def test_every_carried_row_keeps_its_vector_under_its_output_ordinal(tmp_path, inputs):
    # Two outputs, interleaving both inputs (a sort-aware compaction's shape).
    origins = [(0, o) for o in range(inputs[0][3])] + [(1, o) for o in range(inputs[1][3])]
    origins.sort(key=lambda fo: (fo[1] * 7 + fo[0] * 3) % 1000)
    outputs = [origins[: len(origins) // 2], origins[len(origins) // 2:]]
    results, paths = _carry(tmp_path, inputs, outputs)

    for j, origin in enumerate(outputs):
        carried = stored_vectors(paths[j])
        expected = {
            out_ordinal: inputs[f][2][o]
            for out_ordinal, (f, o) in enumerate(origin)
            if o in inputs[f][2]
        }
        assert carried == expected                         # unindexed (null) rows stay unindexed
        assert results[j]["rows_indexed"] == len(expected)
        assert results[j]["file_bytes"] == os.path.getsize(paths[j])


def test_a_live_indexed_row_left_behind_fails_the_carry(tmp_path, inputs):
    indexed = sorted(inputs[0][2])
    lost = indexed[10]
    origins = [(0, o) for o in range(inputs[0][3]) if o != lost] + [(1, o) for o in range(inputs[1][3])]
    with pytest.raises(RuntimeError, match=f"indexed row {lost} of .* is live but was not written"):
        _carry(tmp_path, inputs, [origins])
    assert not [p for p in os.listdir(tmp_path) if "vidx" in p]


def test_a_deleted_row_is_let_go(tmp_path, inputs):
    indexed = sorted(inputs[0][2])
    gone = indexed[10]
    origins = [(0, o) for o in range(inputs[0][3]) if o != gone] + [(1, o) for o in range(inputs[1][3])]
    results, _ = _carry(tmp_path, inputs, [origins], deleted=([gone], []))
    assert results[0]["rows_indexed"] == len(inputs[0][2]) + len(inputs[1][2]) - 1


def test_a_row_written_twice_is_refused(tmp_path, inputs):
    origins = [(0, o) for o in range(inputs[0][3])] + [(1, o) for o in range(inputs[1][3])] + [(0, 3)]
    with pytest.raises(RuntimeError, match="written twice"):
        _carry(tmp_path, inputs, [origins])


def test_remote_inputs_carry_to_the_same_files(tmp_path, inputs):
    origins = [(1, o) for o in range(inputs[1][3])] + [(0, o) for o in range(inputs[0][3])]
    (tmp_path / "local").mkdir()
    (tmp_path / "remote").mkdir()
    _, local_paths = _carry(tmp_path / "local", inputs, [origins])
    servers, locations, requests = [], [], []
    for path, _, _, _ in inputs:
        server = _serve_ranges(open(path, "rb").read(), requests)
        servers.append(server)
        locations.append(f"http://127.0.0.1:{server.server_address[1]}/i.vidx")
    try:
        _, remote_paths = _carry(tmp_path / "remote", inputs, [origins], locations=locations,
                                 auth_header="Bearer t0k3n")
    finally:
        for server in servers:
            server.shutdown()
    assert open(remote_paths[0], "rb").read() == open(local_paths[0], "rb").read()
    assert requests and all(m == "GET" and auth == "Bearer t0k3n" for m, _, auth in requests)
    # Each pass reads an input's body whole, in large requests: never one per block.
    assert len(requests) <= 3 * 2 * len(inputs)


def test_an_output_that_carries_nothing_gets_no_files(tmp_path, inputs):
    nulls = [o for o in range(inputs[0][3]) if o not in inputs[0][2]]
    rest = [(0, o) for o in range(inputs[0][3]) if o in inputs[0][2]] + [(1, o) for o in range(inputs[1][3])]
    results, paths = _carry(tmp_path, inputs, [[(0, o) for o in nulls], rest])
    assert results[0] is None
    assert not os.path.exists(paths[0])
    assert results[1]["rows_indexed"] == len(inputs[0][2]) + len(inputs[1][2])
