import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()

for limit in [1, 2, 5, 10, 100]:
    print(f"GROUP BY URL LIMIT {limit}...", end=" ", flush=True)
    try:
        morsels = session.execute_to_morsels(f"SELECT URL FROM scratch.hits GROUP BY URL LIMIT {limit};")
        first = next(morsels)
        print(f"✓ ({first.num_rows} rows)")
    except Exception as e:
        print(f"✗ CRASH")
        break
