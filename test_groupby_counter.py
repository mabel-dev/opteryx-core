import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()
print("Session created")

sql = "SELECT CounterID, COUNT(*) as c FROM scratch.hits GROUP BY CounterID LIMIT 5;"
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
    if count >= 5:
        break
    
print(f"Done - processed {count} morsels")
sys.stdout.flush()
