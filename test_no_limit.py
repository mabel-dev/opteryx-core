import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()

print("GROUP BY string column (URL) - NO LIMIT...")
sys.stdout.flush()

try:
    morsels = session.execute_to_morsels("SELECT URL FROM scratch.hits GROUP BY URL;")
    print("Got iterator, getting first morsel...")
    sys.stdout.flush()
    
    first = next(morsels)
    print(f"Got first morsel: {first.num_rows} rows")
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
