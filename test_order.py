import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()
print("Session created")

# Test GROUP BY with ORDER BY
sql = "SELECT URL, COUNT(*) AS c FROM scratch.hits GROUP BY URL ORDER BY c DESC;"
print(f"Executing: {sql}")
morsels = session.execute_to_morsels(sql)
print("Got morsels, iterating...")
count = 0
for m in morsels:
    count += 1
    if count <= 3:
        print(f"Morsel {count}: {m.num_rows} rows")
print(f"Processed {count} morsels, Done")
