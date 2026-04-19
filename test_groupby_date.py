import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()
print("Session created")

# Just test COUNT without GROUP BY first
sql = "SELECT COUNT(*) FROM scratch.hits;"
print(f"Executing: {sql}")
sys.stdout.flush()

morsels = session.execute_to_morsels(sql)
print("Got morsels, iterating...")
sys.stdout.flush()

for m in morsels:
    print(f"Morsel: {m.num_rows} rows")
    sys.stdout.flush()
print("Done with COUNT test")
sys.stdout.flush()
