"""Safe comparison runner that handles segfaults in subprocesses."""

import subprocess
import sys
import pickle
import tempfile
import os


def run_comparison_safe(left_type, left_encoding, right_type, right_encoding, operation_name):
    """Run a comparison in a subprocess to isolate segfaults.

    Returns:
        (success: bool, result: Any, error_msg: str)
        - success=True, result=value: Comparison succeeded
        - success=False, result=None, error_msg: Comparison failed (NotImplementedError, TypeError, segfault, etc.)
    """

    # Create a temporary file to hold the result
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        temp_script = f.name
        f.write(f"""
import sys
import os
import pickle

# Add both the root and vectors test directory to path
sys.path.insert(0, '/Users/justin/Nextcloud/opteryx-core')
sys.path.insert(0, '/Users/justin/Nextcloud/opteryx-core/draken/tests/vectors')
os.chdir('/Users/justin/Nextcloud/opteryx-core/draken/tests/vectors')

from _vector_helpers import create_vector_with_encoding, apply_comparison

try:
    left_vec = create_vector_with_encoding(
        {left_type!r}, {left_encoding}, size=100, nullable=False
    )
    right_vec = create_vector_with_encoding(
        {right_type!r}, {right_encoding}, size=100, nullable=False
    )
    result = apply_comparison(left_vec, right_vec, {operation_name!r})
    print("SUCCESS")
    sys.exit(0)
except NotImplementedError as e:
    print(f"NOT_IMPLEMENTED:{{str(e)}}")
    sys.exit(1)
except TypeError as e:
    print(f"TYPE_ERROR:{{str(e)}}")
    sys.exit(2)
except Exception as e:
    print(f"ERROR:{{type(e).__name__}}:{{str(e)}}")
    sys.exit(3)
""")

    try:
        # Run the comparison in a subprocess with timeout
        result = subprocess.run(
            [sys.executable, temp_script],
            capture_output=True,
            text=True,
            timeout=5,
            cwd="/Users/justin/Nextcloud/opteryx-core"
        )

        output = result.stdout.strip()
        stderr = result.stderr.strip()

        # Get only the last line of output (skip module load messages)
        last_line = output.split('\n')[-1] if output else ""

        # Parse result from last line
        if last_line == "SUCCESS":
            return (True, "OK", None)
        elif last_line.startswith("NOT_IMPLEMENTED:"):
            msg = last_line[len("NOT_IMPLEMENTED:"):]
            return (False, None, f"NOT_IMPLEMENTED: {msg}")
        elif last_line.startswith("TYPE_ERROR:"):
            msg = last_line[len("TYPE_ERROR:"):]
            return (False, None, f"TYPE_ERROR: {msg}")
        elif last_line.startswith("ERROR:"):
            msg = last_line[len("ERROR:"):]
            return (False, None, f"ERROR: {msg}")
        else:
            # If subprocess exited with an error, it might be a segfault or import error
            if result.returncode != 0:
                return (False, None, f"Process exited with code {result.returncode}. Output: {output}")
            return (False, None, f"Subprocess error: {last_line}")

    except subprocess.TimeoutExpired:
        return (False, None, "TIMEOUT: Comparison took too long (>5s)")
    except Exception as e:
        return (False, None, f"Subprocess error: {type(e).__name__}: {str(e)}")
    finally:
        # Clean up temp file
        try:
            os.unlink(temp_script)
        except:
            pass
