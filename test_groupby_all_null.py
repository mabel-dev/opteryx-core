import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()

# Try GROUP BY on a column that has many NULL values
sql = "SELECT CAST(NULL AS VARCHAR) AS col, COUNT(*) FROM scratch.hits LIMIT 5 GROUP BY col;"
print(f"Executing: {sql}")
sys.stdout.flush()

try:
    morsels = session.execute_to_morsels(sql)
    print("Got morsels iterator")
    sys.stdout.flush()
    
    for m in morsels:
        print(f"Morsel: {m.num_rows} rows")
        sys.stdout.flush()
    
    print("Done")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
