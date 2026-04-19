import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()
sql = "SELECT URL, COUNT(*) AS c FROM scratch.hits GROUP BY URL LIMIT 100;"
print(f"Executing: {sql}")
morsels = session.execute_to_morsels(sql)
print("Got morsels iterator")
count = 0
try:
    for m in morsels:
        count += 1
        if count <= 3:
            print(f"Morsel {count}: {m.num_rows} rows")
except Exception as e:
    print(f"Error after {count} morsels: {e}")
    import traceback
    traceback.print_exc()
print(f"Successfully processed {count} morsels")
print("About to exit")
