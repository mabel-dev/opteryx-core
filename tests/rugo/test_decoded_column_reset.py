"""Run the DecodedColumn::reset() completeness test under pytest.

The assertions live in `rugo/src/parquet/decoded_column_reset_test.cpp` because
reset() is a reuse behaviour of a plain C++ struct with no Python entry point:
the Cython binding only ever sees an already-decoded column and cannot observe
whether the buffers it reads were cleared between decodes. Nothing at this layer
can see the bug the test is for.

That bug: DecodedColumn is reused across column decodes so the vectors keep
their capacity. A member added to the struct but not cleared in reset() carries
the previous column's data into the next one — a silent wrong answer, not a
crash.

Compile flags are NOT duplicated here — `make decoded-column-reset-test` owns
them. The driver needs only -I rugo/src/parquet (decode.hpp reaches nothing but
metadata.hpp and the standard library), so it does not depend on `make compile`.
"""

import os
import shutil
import subprocess

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


@pytest.mark.skipif(shutil.which("make") is None, reason="make not available")
@pytest.mark.skipif(shutil.which("clang++") is None, reason="clang++ not available")
def test_decoded_column_reset_is_complete():
    result = subprocess.run(
        ["make", "decoded-column-reset-test"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, (
        f"decoded-column-reset-test failed (exit {result.returncode})\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
    assert "ALL PASS" in result.stdout, f"driver did not report ALL PASS:\n{result.stdout}"


if __name__ == "__main__":  # pragma: no cover
    test_decoded_column_reset_is_complete()
    print("✅ decoded column reset is complete")
