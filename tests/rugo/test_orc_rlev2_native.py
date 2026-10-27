"""Exercise the native ORC RLEv2 decoder in the rugo test suite."""

import os
import subprocess


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def test_orc_rlev2_native():
    result = subprocess.run(
        ["make", "orc-rle-test"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"ORC RLEv2 native test failed (exit {result.returncode})\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
    assert "ORC RLEv2 native correctness tests passed" in result.stdout
