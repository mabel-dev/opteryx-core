"""Run the PageIndex parse + page-predicate tests under pytest.

The assertions live in `rugo/src/parquet/page_index_test.cpp`: the parse of the
two Thrift structs and the per-page bounds test have no Python entry point (the
pipeline consumes them inside a worker), and the bytes under test are produced
by the writer's own Compact Protocol encoder so the encoding is real. This
wrapper exists so the driver runs in the suite instead of only on demand.

Compile flags are NOT duplicated here — `make page-index-test` owns them.
"""

import os
import shutil
import subprocess

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


@pytest.mark.skipif(shutil.which("make") is None, reason="make not available")
@pytest.mark.skipif(shutil.which("clang++") is None, reason="clang++ not available")
def test_page_index_parse_and_predicates():
    result = subprocess.run(
        ["make", "page-index-test"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, (
        f"page-index-test failed (exit {result.returncode})\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
    assert "ALL PASS" in result.stdout, f"driver did not report ALL PASS:\n{result.stdout}"


if __name__ == "__main__":  # pragma: no cover
    test_page_index_parse_and_predicates()
    print("✅ page index parse + predicates")
