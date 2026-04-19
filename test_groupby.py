import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()
print("Session created")

# Test GROUP BY (no ORDER/LIMIT)
sql = "SELECT URL, COUNT(*) AS c FROM scratch.hits GROUP BY URL;"
print(f"Executing: {sql}")
morsels = session.execute_to_morsels(sql)
print("Got morsels")
for m in morsels:
    print(f"Morsel: {m.num_rows} rows")
print("Done")
