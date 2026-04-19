import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()

print("GROUP BY URL (no LIMIT) - checking structure...")
try:
    morsels = session.execute_to_morsels("SELECT COUNT(*) FROM scratch.hits GROUP BY URL;")
    m = next(morsels)
    print(f"num_columns: {m.num_columns}")
    print(f"num_rows: {m.num_rows}")
    print(f"column names: {m.column_names}")
    for i, name in enumerate(m.column_names):
        col = m.column(name)
        print(f"  Column {i}: {name} - {type(col).__name__}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
