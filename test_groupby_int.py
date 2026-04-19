import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()
print("Session created")

# Group by an integer column
sql = "SELECT Year, COUNT(*) FROM scratch.hits GROUP BY Year;"
print(f"Executing: {sql}")
sys.stdout.flush()

morsels = session.execute_to_morsels(sql)
print("Got morsels, iterating...")
sys.stdout.flush()

count = 0
for m in morsels:
    count += 1
    if count <= 3:
        print(f"Morsel {count}: {m.num_rows} rows")
        sys.stdout.flush()
print(f"Processed {count} morsels, Done")
sys.stdout.flush()
