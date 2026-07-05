# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Guard: the engine must not import pyarrow or numpy at runtime.

pyarrow/numpy are allowed in tests and dev scripts, but NOT inside the engine
(opteryx/rugo/draken). This runs a representative read + execute + native write
workload in a fresh subprocess and asserts neither library got imported. It
must be a subprocess because the test process itself uses pyarrow as an oracle.
"""

import os
import subprocess
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

_WORKLOAD = r"""
import sys
import opteryx
from rugo.parquet import write_parquet

session = opteryx.session()

# read + execute: scan, filter, sort, group-by, LIKE
for sql in (
    "SELECT name, gravity FROM $planets WHERE id > 3 ORDER BY gravity",
    "SELECT COUNT(*), MAX(gravity) FROM $planets GROUP BY orbitalInclination > 0",
    "SELECT name FROM $planets WHERE name LIKE 'M%'",
):
    for _ in session.execute_to_morsels(sql):
        pass

# native write path (no pyarrow)
morsel = next(iter(session.execute_to_morsels("SELECT id, name, gravity FROM $planets")))
write_parquet(morsel)

leaked = sorted(m for m in ("pyarrow", "numpy") if m in sys.modules)
if leaked:
    sys.stderr.write("ENGINE IMPORTED: " + ", ".join(leaked) + "\n")
    sys.exit(1)
sys.exit(0)
"""


def test_engine_does_not_import_pyarrow_or_numpy():
    result = subprocess.run(
        [sys.executable, "-c", _WORKLOAD],
        cwd=REPO_ROOT,
        env=os.environ,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "engine imported a banned library:\n"
        + result.stdout
        + result.stderr
    )


if __name__ == "__main__":
    test_engine_does_not_import_pyarrow_or_numpy()
    print("✅ engine is arrow-free")
