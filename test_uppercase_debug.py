#!/usr/bin/env python3
import sys
sys.path.insert(0, '/Users/justin/Nextcloud/opteryx-core')

from opteryx.compiled.draken.vectors.string_vector import StringVector

# Test 1: Create a constant-encoded StringVector directly
print("Test 1: Direct constant StringVector creation...")
try:
    const_vec = StringVector.from_constant('test', 5)
    print(f"Created constant vector: {const_vec}, encoding={const_vec.encoding}")

    from opteryx.compiled.vector_ops.function_definitions import vector_uppercase
    result = vector_uppercase(const_vec)
    print(f"Result: {result}")
    print("SUCCESS - Direct constant encoding works")
except Exception as e:
    print(f"FAILED: {e}")
    import traceback
    traceback.print_exc()

# Test 2: Try calling via SQL
print("\nTest 2: SQL constant folding...")
try:
    import opteryx
    from opteryx.connectors import DiskConnector
    import sys
    import io

    # Capture stderr to see any messages before segfault
    old_stderr = sys.stderr
    sys.stderr = io.StringIO()

    opteryx.register_workspace("testdata", DiskConnector)
    session = opteryx.session(memberships=["Apollo 11", "opteryx"])

    print("About to execute query...", flush=True)
    sys.stdout.flush()

    # This is where it probably segfaults
    morsels = list(session.execute_to_morsels("SELECT UPPER('test')"))

    print(f"Morsels: {morsels}")
    for morsel in morsels:
        for col in morsel.column_names:
            print(f"  {col}: {morsel[col]}")
    print("SUCCESS - SQL works")
except SystemExit:
    raise
except KeyboardInterrupt:
    raise
except Exception as e:
    sys.stderr = old_stderr
    print(f"FAILED: {e}")
    import traceback
    traceback.print_exc()
