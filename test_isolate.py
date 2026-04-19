import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()

# Test 1: GROUP BY integer column
print("TEST 1: GROUP BY integer column (CounterID)")
try:
    morsels = session.execute_to_morsels("SELECT CounterID, COUNT(*) FROM scratch.hits GROUP BY CounterID LIMIT 5;")
    for m in morsels:
        print(f"  Morsel: {m.num_rows} rows")
    print("  ✓ PASS")
except Exception as e:
    print(f"  ✗ FAIL: {e}")
sys.stdout.flush()

# Test 2: GROUP BY string column, just access columns
print("\nTEST 2: GROUP BY string column (URL), just iterate")
try:
    morsels = session.execute_to_morsels("SELECT URL, COUNT(*) FROM scratch.hits GROUP BY URL LIMIT 5;")
    for m in morsels:
        print(f"  Morsel: {m.num_rows} rows")
    print("  ✓ PASS")
except Exception as e:
    print(f"  ✗ FAIL: {e}")
sys.stdout.flush()

print("\nDone")
