import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()
sql = "SELECT URL FROM scratch.hits GROUP BY URL LIMIT 10;"
print(f"1. Starting execution of: {sql}")
sys.stdout.flush()

try:
    morsels = session.execute_to_morsels(sql)
    print("2. Got morsels iterator")
    sys.stdout.flush()
    
    count = 0
    for m in morsels:
        count += 1
        print(f"3.{count}. Morsel {count}: {m.num_rows} rows")
        sys.stdout.flush()
    
    print(f"4. Successfully processed {count} morsels")
    sys.stdout.flush()
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    sys.stdout.flush()

print("5. About to exit")
sys.stdout.flush()
