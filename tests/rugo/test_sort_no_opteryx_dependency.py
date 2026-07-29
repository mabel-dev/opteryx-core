# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Proof: draken.morsels.sort (the shared vergesort + fast-sort core) is usable
with NO opteryx import anywhere in the process — the actual reason for moving
the sort out of the opteryx-only src/cpp/engine/native_sort.hpp and into
draken. rugo has no query engine to route sorting through; this is what a
standalone rugo user calls directly.

Must run in a subprocess: this repo's own conftest/other tests may already
have imported opteryx by the time this test runs, which would make an
in-process `'opteryx' not in sys.modules` check meaningless.
"""

import os
import subprocess
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

_WORKLOAD = r"""
import sys

# rugo + draken only -- no opteryx import anywhere in this workload.
from rugo.parquet import read_parquet
from draken.morsels.sort import sort_morsels
import draken.draken_native as dn
from draken.morsels.morsel import Morsel

# 1. rugo reads real morsels from a file; draken sorts them. No opteryx planner,
#    no Sink/Source, no Engine anywhere in this path -- just the two draken/rugo
#    wheels a standalone rugo install actually ships.
with read_parquet("testdata/planets/planets.parquet") as reader:
    morsels = list(reader)

out = sort_morsels(morsels, [b"mean_temperature"], [True])
result = []
for m in out:
    m.materialize()
    result.extend(m.column(b"mean_temperature").to_pylist())
assert result == sorted(result), f"not sorted: {result}"
assert result[0] < 0 < result[-1], f"expected negative-to-positive spread: {result}"

# 2. The exact bug this session found and fixed (Vector.compress()'s masked
#    sign bit) -- proven fixed here too, with no opteryx in the process at all,
#    not just in the (opteryx-adjacent) tests/compiled/ suite.
values = [3.14, 1.0, -2.5, 0.0, 100.0]
v = dn.vector_float64_from_sequence(values)
m = Morsel.from_vectors([b"x"], [v])
out = sort_morsels([m], [b"x"], [True])
result = []
for om in out:
    om.materialize()
    result.extend(om.column(b"x").to_pylist())
assert result == sorted(values), f"float sign-order bug regressed: {result}"

# 3. The single-morsel permutation entrypoint too.
from array import array as pyarray
morsel_sort = __import__("draken.morsels.sort", fromlist=["morsel_sort"]).morsel_sort
perm = morsel_sort(m, [b"x"], [True])
assert isinstance(perm, pyarray)
assert [values[i] for i in perm] == sorted(values)

leaked = "opteryx" in sys.modules
if leaked:
    sys.stderr.write("LEAKED: opteryx was imported by a rugo/draken-only workload\n")
    sys.exit(1)
sys.exit(0)
"""


def test_sort_morsels_works_with_no_opteryx_import():
    result = subprocess.run(
        [sys.executable, "-c", _WORKLOAD],
        cwd=REPO_ROOT,
        env=os.environ,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "standalone rugo/draken sort workload failed:\n"
        + result.stdout
        + result.stderr
    )


if __name__ == "__main__":
    test_sort_morsels_works_with_no_opteryx_import()
    print("draken.morsels.sort works with zero opteryx dependency")
