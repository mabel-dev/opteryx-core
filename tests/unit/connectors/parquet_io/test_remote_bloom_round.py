"""Remote bloom membership: the native scan reads a REMOTE file's bloom filters in its
first fetch round and skips the row groups they prove empty
(docs/PARQUET_WRITE_FOR_READ_SPEED.md §3.3b, src/cpp/engine/parquet_bloom_probe.hpp).

Planning attaches the bloom tests to the remote work items min/max kept; the scan
issues every file's bloom bytes asynchronously at make_global, so their round trip
runs alongside the first fetch blocks instead of ahead of them, and once they land
every unit not yet submitted whose bloom proves the literal absent is walked past.
Pinned here over HTTP (dev/throttle_server.py, with a round-trip delay so the first
wave and the bloom round genuinely overlap), on the production native path:

  * the answer is the local read's answer, whatever the bloom excludes;
  * a scattered point lookup reads far fewer row groups and range GETs than the
    same file without blooms, and reports the units it skipped;
  * a literal present in every row group excludes nothing;
  * a file whose bloom bytes exceed the cost cap plans no probe at all, and a
    local scan never plans one (its blooms are read at plan time, as before).
"""

import os
import random
import subprocess
import sys
import tempfile

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from opteryx import config
from opteryx.compiled.platform import cgroup_memory_limit_bytes, physical_memory_total_bytes
from opteryx.compiled.structures.memory_pool import configure_chunk_cache
from rugo import parquet as rp

from tests.unit.connectors.parquet_io.test_fetch_blocks import _register_http_workspace

SERVER = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../dev/throttle_server.py")
)
R = 512          # rows per row group
N_RG = 400       # many more row groups than any in-flight window
N = R * N_RG

rng = random.Random(21)
KEYS = rng.sample(range(1, 1 << 62), N)      # distinct, scattered: min/max keeps every group
TARGET_ROW = 300 * R + 17                     # one row, in row group 300
TARGET = KEYS[TARGET_ROW]
ABSENT = [k for k in rng.sample(range(1, 1 << 62), 50) if k not in set(KEYS)][:2]


def _morsel():
    return Morsel.from_vectors(
        [b"k", b"c", b"p", b"s"],
        [vector_from_sequence(KEYS, DrakenType.INT64),
         vector_from_sequence([i % 3 for i in range(N)], DrakenType.INT64),
         vector_from_sequence([i * 7 for i in range(N)], DrakenType.INT64),
         vector_from_sequence([f"key-{i:09d}" for i in range(N)], DrakenType.VARCHAR)],
    )


def _write(root, name, bloom_filters):
    d = os.path.join(root, name)
    os.makedirs(d)
    data = rp.write_parquet(_morsel(), max_rows_per_row_group=R, bloom_filters=bloom_filters)
    with open(os.path.join(d, "part-0.parquet"), "wb") as f:
        f.write(data)
    return d


def _server(root):
    proc = subprocess.Popen(
        [sys.executable, SERVER, "--root", root, "--port", "0", "--rtt-ms", "40",
         "--error-rate", "0", "--seed", "1"],
        stdout=subprocess.PIPE, text=True,
    )
    ready = proc.stdout.readline()
    if not ready.startswith("READY"):
        proc.kill()
        raise RuntimeError(f"throttle server failed to start: {ready!r}")
    return proc, int(ready.strip().split("port=")[1])


def _run(sql):
    import opteryx

    session = opteryx.session()
    rows = []
    for m in session.execute_to_morsels(sql):
        rows.extend(zip(*[m.column(n).to_pylist() for n in m.column_names]))
    tel = session.telemetry
    diags = tel.get("io_scan_diagnostics") or []
    gets = sum(int(d.get("http_request_count", 0)) for d in diags)
    readings = [r for r in tel.get("operations", {}).values() if "row_groups_read" in r]
    assert len(readings) == 1, readings
    sources = set((tel.get("scan_sources") or {}).values())
    session.close()
    return sorted(rows), gets, readings[0], sources


@pytest.fixture(scope="module", autouse=True)
def chunk_cache_off():
    """Counts are on the cache-MISS path (see test_fetch_blocks.chunk_cache_off)."""
    container = cgroup_memory_limit_bytes() or physical_memory_total_bytes()
    configure_chunk_cache(container, 0, 0)
    yield
    configure_chunk_cache(
        container, config.CHUNK_CACHE_MEMORY_PERCENT, config.CHUNK_CACHE_RESERVE_BYTES
    )


@pytest.fixture(scope="module")
def served():
    with tempfile.TemporaryDirectory() as tmp:
        bloomed = _write(tmp, "bloomed", True)
        bare = _write(tmp, "bare", False)
        proc, port = _server(tmp)
        try:
            _register_http_workspace("rb_bloomed", tmp, port, "bloomed")
            _register_http_workspace("rb_bare", tmp, port, "bare")
            yield {"bloomed": bloomed, "bare": bare}
        finally:
            proc.kill()
            proc.wait()


def test_point_lookup_skips_row_groups_the_bloom_proves_empty(served):
    sql = "SELECT k, p FROM {rel} WHERE k = " + str(TARGET)
    expected, _, _, _ = _run(sql.format(rel=f"'{served['bloomed']}'"))
    assert expected == [(TARGET, TARGET_ROW * 7)]

    rows, gets, reading, sources = _run(sql.format(rel="rb_bloomed.bloomed"))
    assert sources == {"NativeParquetScanSource"}, sources
    assert rows == expected
    skipped = reading.get("row_groups_pruned_bloom")
    assert skipped is not None, reading
    # Everything after the first wave is walked past once the round lands; the
    # row group holding the target is never skipped (its bloom holds it).
    assert skipped > N_RG // 2, reading
    assert reading["row_groups_read"] == N_RG - skipped, reading

    bare_rows, bare_gets, bare_reading, _ = _run(sql.format(rel="rb_bare.bare"))
    assert bare_rows == expected
    assert "row_groups_pruned_bloom" not in bare_reading     # no bloom, no probe
    assert bare_reading["row_groups_read"] == N_RG
    assert gets < bare_gets, (gets, bare_gets)


def test_absent_in_list_returns_nothing_and_skips(served):
    # k and p projected: k's bloom (1,040 B per row group) is 0.21 of the 4.9 KB
    # of chunks it guards. Projecting k alone would be 0.252 — over the cap (see
    # test_bloom_bytes_over_the_cap_plan_no_probe).
    sql = "SELECT k, p FROM {rel} WHERE k IN (" + ", ".join(map(str, ABSENT)) + ")"
    rows, _, reading, _ = _run(sql.format(rel="rb_bloomed.bloomed"))
    assert rows == []
    assert reading.get("row_groups_pruned_bloom", 0) > N_RG // 2, reading


def test_literal_in_every_row_group_excludes_nothing(served):
    # c's bloom is tiny next to k and p, so a probe is planned — and every row
    # group holds c = 1, so it excludes nothing.
    sql = "SELECT k, p FROM {rel} WHERE c = 1"
    expected, _, _, _ = _run(sql.format(rel=f"'{served['bloomed']}'"))
    rows, _, reading, _ = _run(sql.format(rel="rb_bloomed.bloomed"))
    assert len(expected) == sum(1 for i in range(N) if i % 3 == 1)
    assert rows == expected
    assert reading.get("row_groups_pruned_bloom") == 0, reading
    assert reading["row_groups_read"] == N_RG, reading


def test_bloom_bytes_over_the_cap_plan_no_probe(served):
    # `s` is distinct per row but compresses to a few bytes a value, so its bloom
    # (~10 bits per distinct value, 1,040 B) is 3.3x the 315 B chunk it guards:
    # no probe is planned and nothing is reported.
    sql = "SELECT s FROM {rel} WHERE s = 'key-000150017'"
    rows, _, reading, _ = _run(sql.format(rel="rb_bloomed.bloomed"))
    assert rows == [("key-000150017",)]
    assert "row_groups_pruned_bloom" not in reading, reading


def test_local_scan_plans_no_remote_probe(served):
    sql = "SELECT k FROM '" + served["bloomed"] + "' WHERE k = " + str(TARGET)
    rows, gets, reading, _ = _run(sql)
    assert rows == [(TARGET,)]
    assert gets == 0
    assert "row_groups_pruned_bloom" not in reading
    # The local path still prunes with its plan-time bloom read: the target's
    # row group plus the 1% filters' false positives.
    assert 1 <= reading["row_groups_read"] <= 12, reading


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
