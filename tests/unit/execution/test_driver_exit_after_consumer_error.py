"""A consumer that dies mid-stream must not hang the process at exit.

The generator held in a variable survives an uncaught error in the traceback until
interpreter finalization; only then does its teardown wait for the native driver's
finish(). A driver that re-attached to the interpreter to signal it was parked forever
by CPython (PyThread_hang_thread) - the ClickBench c6a.metal load sat out a 10 h
timeout (2026-11-22). The driver is GIL-free now (_engine_plan_task); this runs the
3-line failure in a fresh interpreter and requires a prompt, honest exit.
"""

import os
import subprocess
import sys

REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

# testdata.clickbench_tiny yields many morsels, so the engine is still producing
# (backpressured on the output queue) when the consumer raises on the first one.
SCRIPT = f"""
import sys
sys.path.insert(1, {REPO_ROOT!r})
import opteryx
morsels = opteryx.session().execute_to_morsels("SELECT * FROM testdata.clickbench_tiny")
for morsel in morsels:
    raise RuntimeError("consumer fails on the first morsel")
"""


def test_uncaught_consumer_error_exits_promptly():
    try:
        done = subprocess.run(
            [sys.executable, "-c", SCRIPT], cwd=REPO_ROOT, capture_output=True, text=True, timeout=60
        )
    except subprocess.TimeoutExpired as hung:
        raise AssertionError("process hung at exit after an uncaught consumer error") from hung
    assert done.returncode == 1, (done.returncode, done.stderr[-2000:])
    assert "RuntimeError: consumer fails on the first morsel" in done.stderr, done.stderr[-2000:]


if __name__ == "__main__":  # pragma: no cover
    test_uncaught_consumer_error_exits_promptly()
    print("okay")
