import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()

# Try progressively more complex queries
queries = [
    "SELECT COUNT(*) FROM scratch.hits;",  # No GROUP BY
    "SELECT URL FROM scratch.hits LIMIT 10;",  # No aggregation, just selection
    "SELECT URL, COUNT(*) AS c FROM scratch.hits GROUP BY URL;",  # No ORDER BY
    "SELECT URL, COUNT(*) AS c FROM scratch.hits GROUP BY URL ORDER BY c DESC;",  # With ORDER BY
    "SELECT URL, COUNT(*) AS c FROM scratch.hits GROUP BY URL ORDER BY c DESC LIMIT 10;",  # With LIMIT
]

for i, sql in enumerate(queries):
    print(f"Query {i}: {sql[:60]}")
    try:
        morsels = session.execute_to_morsels(sql)
        count = 0
        for m in morsels:
            count += 1
        print(f"  ✓ Success ({count} morsels)")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
