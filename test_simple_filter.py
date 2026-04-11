import sys
sys.path.insert(0, '.')

import opteryx
from opteryx.connectors import DiskConnector

opteryx.register_workspace("testdata", DiskConnector)

session = opteryx.session()

# Test a simple filter
print("Testing: SELECT id FROM $planets")
result1 = session.execute_to_arrow("SELECT id FROM $planets")
print(f"All rows: {result1.to_pylist()}")
print(f"Shape: {result1.shape}")

print("\nTesting: SELECT * FROM $planets WHERE id = 1")
result2 = session.execute_to_arrow("SELECT * FROM $planets WHERE id = 1")
print(f"Filtered result: {result2.to_pylist()}")
print(f"Shape: {result2.shape}")

