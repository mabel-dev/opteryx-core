"""
The process-lifetime local file table (rugo/src/parquet/io_pipeline.hpp, LocalFileTable).

Every local parquet path resolves to ONE descriptor (and, on first need, one whole-file
mapping) shared by every scan in the process, instead of each query opening and mapping
the files again. The table is bounded at RLIMIT_NOFILE/16 of the soft limit; over the
cap, files no scan holds are dropped least-recently-used first and reopened on next use.

Each case runs in a subprocess: the cap is read from the soft limit when the table is
first built, and the descriptors it holds live as long as the process.
"""

import json
import os
import subprocess
import sys
import tempfile

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pyarrow as pa
import pyarrow.parquet as pq

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../.."))
N_FILES = 12
ROWS_PER_GROUP = 40_000
GROUPS = 3

# Runs in the subprocess, cwd = the temp dir holding the `ds_files` dataset.
# argv: repo, soft fd limit (0 = leave as is).
_SCRIPT = r"""
import json, os, subprocess, sys, resource
repo, soft = sys.argv[1], int(sys.argv[2])
if soft:
    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
sys.path.insert(0, repo)
import opteryx

tmp = os.getcwd()
# One spelling per file: the table is keyed by the path as the scan names it.
files = sorted(os.path.join("ds_files", f) for f in os.listdir("ds_files"))

def held_descriptors():
    # Open DESCRIPTORS (not mappings) this process holds on the test's files.
    real_tmp = os.path.realpath(tmp)
    if sys.platform == "darwin":
        out = subprocess.run(["lsof", "-n", "-P", "-p", str(os.getpid()), "-F", "fn"],
                             capture_output=True, text=True, check=True).stdout.splitlines()
        names, desc = [], ""
        for line in out:
            if line.startswith("f"):
                desc = line[1:]
            elif line.startswith("n") and desc.isdigit():
                names.append(line[1:])
    else:
        names = [os.path.realpath(f"/proc/self/fd/{fd}") for fd in os.listdir("/proc/self/fd")]
    # lsof names pipes and sockets "->0x..." — only absolute names are files.
    return sum(1 for n in names if n.startswith("/") and os.path.realpath(n).startswith(real_tmp))

def rows(sql):
    s = opteryx.session()
    out = []
    for m in s.execute_to_morsels(sql):
        for i in range(m.num_rows):
            out.append([m.column(c).to_pylist()[i] for c in m.column_names])
    return out

per_file, whole, held = [], [], []
for _ in range(3):
    per_file.append([rows(f"SELECT COUNT(*), SUM(a), MAX(s) FROM READ_PARQUET('{f}') WHERE a % 7 <> 3")
                     for f in files])
    # After one-file-at-a-time scans: each new file was a table miss, so the table
    # is back under its cap here.
    held.append(held_descriptors())
    whole.append(rows("SELECT COUNT(*), SUM(a), MAX(s) FROM ds_files WHERE a % 7 <> 3"))
print("RESULT" + json.dumps({"per_file": per_file, "whole": whole, "held": held}))
"""


def _write_files(tmp):
    os.mkdir(os.path.join(tmp, "ds_files"))
    expected = []
    for k in range(N_FILES):
        n = ROWS_PER_GROUP * GROUPS
        a = [k * n + i for i in range(n)]
        # ~0.5 MB per string chunk uncompressed: large chunks read through the mapping,
        # the int chunks below the small-chunk threshold read with pread.
        s = [f"f{k:02d}-{i:010d}" for i in range(n)]
        pq.write_table(pa.table({"a": a, "s": s}), os.path.join(tmp, "ds_files", f"part-{k:02d}.parquet"),
                       row_group_size=ROWS_PER_GROUP, compression="none", use_dictionary=False)
        kept = [(x, y) for x, y in zip(a, s) if x % 7 != 3]
        expected.append([len(kept), sum(x for x, _ in kept), max(y for _, y in kept)])
    return expected


def _run(tmp, soft):
    result = subprocess.run([sys.executable, "-c", _SCRIPT, REPO, str(soft)], cwd=tmp,
                            capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stderr[-3000:]
    return json.loads(result.stdout.split("RESULT", 1)[1])


def _check_answers(got, expected):
    total = [sum(e[0] for e in expected), sum(e[1] for e in expected), max(e[2] for e in expected)]
    for round_rows in got["per_file"]:
        assert [r[0] for r in round_rows] == expected
    for whole_rows in got["whole"]:
        assert whole_rows == [total]


def test_descriptors_outlive_the_query():
    """Every file scanned stays open after its queries end: the next query of the
    process reads through the same descriptor instead of opening the file again."""
    with tempfile.TemporaryDirectory() as tmp:
        expected = _write_files(tmp)
        got = _run(tmp, 0)
        _check_answers(got, expected)
        assert got["held"] == [N_FILES] * 3, got["held"]


def test_cap_evicts_least_recently_used_files_and_answers_stay_right():
    """A 128 soft limit caps the table at 8 files. Twelve files scanned one query at a
    time, and all twelve as one dataset in one query (more held at once than the cap
    allows), must give the same answers. After a pass of one-file queries (each new
    file a miss that evicts the least recently used), exactly 8 descriptors remain."""
    with tempfile.TemporaryDirectory() as tmp:
        expected = _write_files(tmp)
        got = _run(tmp, 128)
        _check_answers(got, expected)
        assert got["held"] == [128 // 16] * 3, got["held"]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
