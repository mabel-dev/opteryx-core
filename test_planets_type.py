import sys
sys.path.insert(0, '.')

import opteryx
from opteryx.connectors import DiskConnector

opteryx.register_workspace("testdata", DiskConnector)

session = opteryx.session()

# Get the planets data
result = session.execute_to_arrow("SELECT id FROM $planets LIMIT 2")

print(f"Result type: {type(result)}")
print(f"Result schema: {result.schema}")
print(f"Result data: {result.to_pylist()}")

# Now let's test filtering
result2 = session.execute_to_arrow("SELECT * FROM $planets WHERE id = 1 LIMIT 2")
print(f"\nFiltered result (id = 1): {result2.to_pylist()}")

