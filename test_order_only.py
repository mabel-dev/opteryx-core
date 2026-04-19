import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()

print("GROUP BY URL + ORDER BY (no LIMIT)...")
try:
    morsels = session.execute_to_morsels("SELECT URL, COUNT(*) as c FROM scratch.hits GROUP BY URL ORDER BY c DESC;")
    print("Got iterator, getting first morsel...")
    first = next(morsels)
    print(f"✓ Got first morsel: {first.num_rows} rows")
except Exception as e:
    print(f"✗ CRASH: {type(e).__name__}")
