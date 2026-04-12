import sys
import os
sys.path.insert(0, os.getcwd())

import opteryx

session = opteryx.session()
try:
    morsels = session.execute_to_morsels("SELECT * FROM testdata.satellites LIMIT 5")
    for m in morsels:
        print(f"Got morsel with {m.num_rows} rows")
except Exception as e:
    import traceback
    traceback.print_exc()
