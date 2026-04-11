import sys
sys.path.insert(0, '.')

import opteryx
from opteryx.connectors import DiskConnector

opteryx.register_workspace("testdata", DiskConnector)

session = opteryx.session()

# Get the planets data without filtering
result = session.execute_to_arrow("SELECT id FROM $planets")

print(f"Result: {result.to_pylist()}")
print(f"Number of rows: {len(result)}")

