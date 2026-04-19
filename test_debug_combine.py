import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()
sql = "SELECT URL, COUNT(*) as c FROM scratch.hits GROUP BY URL LIMIT 10;"
print(f"Executing: {sql}")
sys.stdout.flush()

try:
    morsels = session.execute_to_morsels(sql)
    print("Got morsels iterator")
    sys.stdout.flush()
    
    count = 0
    for m in morsels:
        count += 1
        print(f"Morsel {count}: {m.num_rows} rows")
        sys.stdout.flush()
        if count > 10:
            break
    
    print(f"Successfully iterated {count} morsels")
    sys.stdout.flush()
except Exception as e:
    print(f"Exception caught: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
    sys.stdout.flush()

print("Exiting...")
sys.stdout.flush()
