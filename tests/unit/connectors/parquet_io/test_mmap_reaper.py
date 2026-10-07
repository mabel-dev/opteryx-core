"""
Whole-file mmap teardown runs off the query path (rugo io_pipeline.hpp, MappingReaper).

With the whole-file local mmap cache on (x86 default; forced here with RUGO_LOCAL_MMAP_CACHE=1 so the Mac
exercises it too), every scanned file is mapped once per pipeline. The pipeline destructor used to `munmap` each
file inline, ~0.65 ms per file on the query's critical path; it now hands them to a native reaper thread.

What is checked, in a fresh process per case (RUGO_LOCAL_MMAP_CACHE is read once per process):

  * the answers are unchanged;
  * cache on: every mapped file is unmapped by the reaper — `mm_reap_files` reaches the file count and
    `mm_reap_ns` accrues (the wall-clock benefit is measured by A/B, not asserted here: timing asserts are flaky);
  * a cache-off process maps nothing per pipeline, so the reaper never runs;
  * back-to-back queries stay correct while the reaper is still unmapping the previous query's files.
"""

import os
import subprocess
import sys
import tempfile
import textwrap

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))
N_FILES = 6
ROWS = 20_000

_SCRIPT = textwrap.dedent(
    """
    import json, os, sys, time
    sys.path.insert(0, {repo!r})
    import opteryx, rugo.rugo_native as rp
    from opteryx.connectors import DiskConnector
    os.chdir({tmp!r})
    opteryx.register_workspace("ws_reap", DiskConnector)
    rp.reset_cpp_telemetry()
    sql = "SELECT COUNT(*), SUM(k), SUM(LENGTH(s)) FROM ws_reap.t"
    answers = []
    teardown_ms = []
    for _ in range(3):  # back-to-back: the reaper may still be unmapping the previous query's files
        s = opteryx.session()
        rows = []
        for m in s.execute_to_morsels(sql):
            rows.extend(zip(*[m.column(c).to_pylist() for c in m.column_names]))
        answers.append(rows)
        tel = s.telemetry if isinstance(s.telemetry, dict) else s.telemetry.as_dict()
        teardown_ms.append(tel["time_engine_teardown_close_scans"] * 1000)
    deadline = time.time() + 10
    while time.time() < deadline and rp.get_cpp_telemetry()["mm_reap_files"] < {expect}:
        time.sleep(0.02)
    t = rp.get_cpp_telemetry()
    print(json.dumps(dict(answers=answers, reap_files=t["mm_reap_files"], reap_ns=t["mm_reap_ns"], teardown_ms=teardown_ms)))
    """
)


@pytest.fixture(scope="module")
def data_dir():
    tmp = tempfile.mkdtemp()
    d = os.path.join(tmp, "ws_reap", "t")
    os.makedirs(d)
    for i in range(N_FILES):
        tbl = pa.table(
            {
                "k": pa.array(range(i * ROWS, (i + 1) * ROWS), type=pa.int64()),
                "s": pa.array([("x" * (j % 40)) for j in range(ROWS)], type=pa.string()),
            }
        )
        pq.write_table(tbl, os.path.join(d, f"f{i}.parquet"), row_group_size=ROWS // 2, compression="snappy")
    return tmp


def _expected():
    total_rows = N_FILES * ROWS
    sum_k = sum(range(total_rows))
    sum_len = N_FILES * sum(j % 40 for j in range(ROWS))
    return [[total_rows, sum_k, sum_len]]


def _run(data_dir, env, expect):
    import json

    e = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", **env)
    out = subprocess.run(
        [sys.executable, "-c", _SCRIPT.format(repo=REPO, tmp=data_dir, expect=expect)],
        capture_output=True, text=True, env=e, timeout=300,
    )
    assert out.returncode == 0, out.stderr[-2000:]
    return json.loads(out.stdout.strip().splitlines()[-1])


def _assert_answers(res):
    want = [tuple(_expected()[0])]
    for a in res["answers"]:
        assert [tuple(r) for r in a] == want, res["answers"]


def test_reaper_unmaps_every_file_off_the_query_path(data_dir):
    res = _run(data_dir, {"RUGO_LOCAL_MMAP_CACHE": "1"}, expect=N_FILES * 3)
    _assert_answers(res)
    # 3 queries x N_FILES files, each mapped once per pipeline -> each unmapped once by the reaper.
    assert res["reap_files"] == N_FILES * 3, res
    assert res["reap_ns"] > 0, res


def test_cache_off_maps_nothing_per_pipeline(data_dir):
    res = _run(data_dir, {"RUGO_LOCAL_MMAP_CACHE": "0"}, expect=0)
    _assert_answers(res)
    assert res["reap_files"] == 0, res
