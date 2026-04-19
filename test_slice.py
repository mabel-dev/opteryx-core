import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()

print("GROUP BY URL, then manually check if we can slice the result...")
try:
    morsels = session.execute_to_morsels("SELECT URL FROM scratch.hits GROUP BY URL;")
    morsel = next(morsels)
    print(f"✓ Got morsel: {morsel.num_rows} rows")
    
    print("Now trying to slice it...")
    sliced = morsel.slice(0, 5)
    print(f"✓ Sliced to: {sliced.num_rows} rows")
except Exception as e:
    print(f"✗ Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
