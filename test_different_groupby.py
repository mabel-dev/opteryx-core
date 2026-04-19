import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()

tests = [
    ("GROUP BY URL (string), SELECT COUNT", "SELECT COUNT(*) FROM scratch.hits GROUP BY URL LIMIT 5;"),
    ("GROUP BY CounterID (int), SELECT COUNT", "SELECT COUNT(*) FROM scratch.hits GROUP BY CounterID LIMIT 5;"),
    ("GROUP BY URL, SELECT URL (string in output)", "SELECT URL FROM scratch.hits GROUP BY URL LIMIT 5;"),
    ("GROUP BY CounterID, SELECT URL (string in output)", "SELECT URL FROM scratch.hits GROUP BY CounterID LIMIT 5;"),
]

for name, sql in tests:
    print(f"{name}...", end=" ", flush=True)
    try:
        morsels = session.execute_to_morsels(sql)
        first = next(morsels)
        print(f"✓")
    except Exception as e:
        print(f"✗ CRASH")
