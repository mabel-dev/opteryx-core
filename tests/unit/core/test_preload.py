"""
opteryx.preload() imports the planner and execution modules that `import opteryx` leaves
to the first query. Run in a fresh interpreter: this test process has already imported them.
"""

import os
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))

PROBE = """
import sys
sys.path.insert(0, sys.argv[1])
import opteryx
assert "opteryx.query_session" not in sys.modules, "import opteryx is no longer lazy"
before = len(sys.modules)
opteryx.preload()
assert "opteryx.query_session" in sys.modules
assert "opteryx.planner.binder" in sys.modules
print(len(sys.modules) - before)
"""


def test_preload_imports_the_lazy_modules():
    result = subprocess.run(
        [sys.executable, "-c", PROBE, ROOT], capture_output=True, text=True, cwd=ROOT, check=False
    )
    assert result.returncode == 0, result.stderr
    assert int(result.stdout.strip()) > 200, result.stdout


if __name__ == "__main__":  # pragma: no cover
    test_preload_imports_the_lazy_modules()
    print("okay")
