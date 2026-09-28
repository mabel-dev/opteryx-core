"""Every compiled planner module must import first, in a fresh interpreter.

Guards the column_type -> opteryx.types -> logical_type -> column_type cycle: it only
showed when a planner module was the first thing imported, because anything that
had already loaded opteryx.types masked it. Each module is imported in its own
subprocess so no earlier import in this test process can mask it again.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
PLANNER_DIR = REPO_ROOT / "opteryx" / "compiled" / "planner"

# Discovered from the source tree so a new planner module is covered without an edit.
PLANNER_MODULES = sorted(
    f"opteryx.compiled.planner.{path.stem}"
    for path in [*PLANNER_DIR.glob("*.pyx"), *PLANNER_DIR.glob("*.py")]
    if path.stem != "__init__"
)


def test_planner_modules_discovered():
    assert "opteryx.compiled.planner.column_type" in PLANNER_MODULES
    assert "opteryx.compiled.planner.logical_category" in PLANNER_MODULES


@pytest.mark.parametrize("module", PLANNER_MODULES)
def test_planner_module_imports_first_in_fresh_interpreter(module):
    result = subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        cwd=REPO_ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"fresh `import {module}` failed:\n{result.stderr}"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
