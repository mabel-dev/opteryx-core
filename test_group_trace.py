import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()

print("GROUP BY string column (URL)...")
sys.stdout.flush()

morsels = session.execute_to_morsels("SELECT URL FROM scratch.hits GROUP BY URL LIMIT 5;")
print("Got morsels iterator")
sys.stdout.flush()

print("Getting first morsel...")
sys.stdout.flush()

try:
    first = next(morsels)
    print(f"Got first morsel: {first.num_rows} rows")
except StopIteration:
    print("No morsels")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

print("Done")
