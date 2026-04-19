import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()

print("SELECT COUNT(*) FROM scratch.hits GROUP BY URL (NO LIMIT)...")
try:
    morsels = session.execute_to_morsels("SELECT COUNT(*) FROM scratch.hits GROUP BY URL;")
    m = next(morsels)
    print(f"✓ Works: {m.num_rows} rows, {m.num_columns} columns")
except Exception as e:
    print(f"✗ Crashes: {type(e).__name__}")

print("\nSELECT COUNT(*) FROM scratch.hits GROUP BY URL LIMIT 5...")
try:
    morsels = session.execute_to_morsels("SELECT COUNT(*) FROM scratch.hits GROUP BY URL LIMIT 5;")
    m = next(morsels)
    print(f"✓ Works: {m.num_rows} rows, {m.num_columns} columns")
except Exception as e:
    print(f"✗ Crashes: {type(e).__name__}")
