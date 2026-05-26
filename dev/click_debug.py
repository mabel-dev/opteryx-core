import os
import sys
import traceback

sys.path.insert(0, os.getcwd())
import opteryx
from tests.performance.clickbench.opteryx import runner

print("running individual statements debug")
for idx, (stmt, exc) in enumerate(runner.STATEMENTS):
    s = stmt.replace("{DATASET}", runner.DATASET.value)
    print(f"RUNNING {idx:02d}: {s[:120]!r}")
    try:
        session = opteryx.session()
        for _ in session.execute_to_morsels(s):
            pass
        session.close()
        print(f"OK {idx:02d}")
    except Exception:
        print(f"EXCEPTION at {idx:02d}")
        traceback.print_exc()
        try:
            session.close()
        except Exception:
            pass
    sys.stdout.flush()
