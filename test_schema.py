import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()

# Test GROUP BY on string, no LIMIT (works)
print("GROUP BY URL (no LIMIT) - checking schema...")
try:
    morsels = session.execute_to_morsels("SELECT COUNT(*) FROM scratch.hits GROUP BY URL;")
    m = next(morsels)
    print(f"Morsel columns: {m.columns}")
    print(f"Number of columns: {m.num_columns}")
except Exception as e:
    print(f"Error: {e}")
