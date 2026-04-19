import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()

print("GROUP BY URL, then manually accessing columns...")
try:
    morsels = session.execute_to_morsels("SELECT URL FROM scratch.hits GROUP BY URL;")
    morsel = next(morsels)
    print(f"✓ Got morsel: {morsel.num_rows} rows")
    
    print("Accessing URL column...")
    url_col = morsel.column('URL')
    print(f"✓ Got URL column: {type(url_col).__name__}")
    
except Exception as e:
    print(f"✗ Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
