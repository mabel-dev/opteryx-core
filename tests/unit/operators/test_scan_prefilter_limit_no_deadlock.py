"""
A LIMIT that closes a scan while the in-worker prefilter is still running must
not deadlock.

The nogil expression VM calls kernels through function-pointer typedefs in
opteryx/expression/evaluator/evaluation.pyx. Declared without `noexcept`, Cython
followed every call with __Pyx_ErrOccurredWithGIL(), so a decode worker running
the pushed LIKE prefilter blocked on the GIL while the consumer thread - holding
the GIL - waited in NativeScanPlan.close -> ParquetIOPipeline::wait_shutdown for
that worker's task to finish. Every thread then slept forever.

Run in a subprocess: a regression is a hang, and a hang must fail the test, not
the suite.
"""

import os
import subprocess
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))

_SCRIPT = """
import sys
sys.path.insert(0, {root!r})
import opteryx
session = opteryx.session()
for _ in range(3):
    rows = sum(
        m.num_rows
        for m in session.execute_to_morsels(
            "SELECT URL FROM testdata.clickbench_tiny WHERE URL LIKE '%a%' LIMIT 3"
        )
    )
    assert rows == 3, rows
print("done")
"""


def test_limit_over_prefiltered_scan_does_not_deadlock():
    result = subprocess.run(
        [sys.executable, "-c", _SCRIPT.format(root=ROOT)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().endswith("done")


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
