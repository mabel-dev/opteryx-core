import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()

tests = [
    ("Just COUNT (aggregate only)", "SELECT COUNT(*) FROM scratch.hits GROUP BY URL LIMIT 5;"),
    ("Just URL (group key)", "SELECT URL FROM scratch.hits GROUP BY URL LIMIT 5;"),
    ("Both URL and COUNT", "SELECT URL, COUNT(*) FROM scratch.hits GROUP BY URL LIMIT 5;"),
]

for name, sql in tests:
    print(f"{name}...", end=" ", flush=True)
    try:
        morsels = session.execute_to_morsels(sql)
        first = next(morsels)
        print(f"✓ ({first.num_rows} rows)")
    except Exception as e:
        print(f"✗ CRASH")
