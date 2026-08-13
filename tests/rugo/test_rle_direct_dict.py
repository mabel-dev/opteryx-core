"""Run the RLE skip-dense -> Dict direct-builder tests under pytest.

The assertions live in `rugo/src/parquet/rle_direct_dict_test.cpp` because the
builders are `static inline` in io_pipeline.hpp with no Python entry point, and
the path they serve cannot be reached from any rugo-written parquet file (rugo
writes REP_OPTIONAL, the RLE outputs need max_definition_level == 0). This wrapper
exists so the driver actually runs in the suite instead of only on demand.

Compile flags are NOT duplicated here — `make rle-dict-test` owns them.
"""

import os
import shutil
import subprocess

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


@pytest.mark.skipif(shutil.which("make") is None, reason="make not available")
@pytest.mark.skipif(shutil.which("clang++") is None, reason="clang++ not available")
def test_rle_direct_dict_builders():
    result = subprocess.run(
        ["make", "rle-dict-test"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, (
        f"rle-dict-test failed (exit {result.returncode})\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
    assert "ALL PASS" in result.stdout, f"driver did not report ALL PASS:\n{result.stdout}"


if __name__ == "__main__":  # pragma: no cover
    test_rle_direct_dict_builders()
    print("✅ rle direct dict builders")
