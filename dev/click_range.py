import os
import sys
import traceback

sys.path.insert(0, os.getcwd())
import opteryx
from tests.performance.clickbench.opteryx import runner

if len(sys.argv) != 3:
    print("Usage: click_range.py START END (inclusive, 0-based indices)")
    sys.exit(2)

start = int(sys.argv[1])
end = int(sys.argv[2])
if start < 0 or end >= len(runner.STATEMENTS) or start > end:
    print("Invalid range")
    sys.exit(2)

for idx in range(start, end + 1):
    stmt, exc = runner.STATEMENTS[idx]
    s = stmt.replace("{DATASET}", runner.DATASET.value)
    print(f"RUNNING {idx:02d}: {s[:120]!r}", flush=True)
    session = None
    try:
        session = opteryx.session()
        for _ in session.execute_to_morsels(s):
            pass
        print(f"OK {idx:02d}", flush=True)
    except Exception as e:
        print(f"EXCEPTION at {idx:02d}:", type(e), e, flush=True)
        traceback.print_exc()
        if session is not None:
            try:
                session.close()
            except Exception:
                pass
        # Distinguish Parquet pipeline decode errors (allow bisect to skip ranges
        # that fail to decode under ASAN) from other failures. Return code 2 for
        # Parquet pipeline decode so caller can treat it as "inconclusive".
        msg = str(e)
        if isinstance(e, RuntimeError) and msg.startswith("Parquet pipeline error"):
            sys.exit(2)
        # Otherwise, report a real failure in this range
        sys.exit(1)
    finally:
        if session is not None:
            session.close()

print("ALL OK")
sys.exit(0)
