import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()
print("Session created")

# Small GROUP BY that should fit in one morsel
sql = "SELECT CounterID, COUNT(*) as c FROM scratch.hits WHERE CounterID < 5 GROUP BY CounterID;"
print(f"Executing: {sql}")
sys.stdout.flush()

morsels = session.execute_to_morsels(sql)
print("Got morsels, iterating...")
sys.stdout.flush()

count = 0
for m in morsels:
    count += 1
    print(f"Morsel {count}: {m.num_rows} rows")
    sys.stdout.flush()
    
print(f"Done - processed {count} morsels")
sys.stdout.flush()
