import sys
sys.path.insert(0, '.')

import opteryx
from opteryx.connectors import DiskConnector

opteryx.register_workspace("testdata", DiskConnector)

session = opteryx.session()

# Get the planets data - this time using execute_to_morsels
morsels = session.execute_to_morsels("SELECT id FROM $planets")

print("Morsels obtained")
for morsel_idx, morsel in enumerate(morsels):
    print(f"\nMorsel {morsel_idx}:")
    print(f"Arrays: {len(morsel.arrays)}")
    if morsel.arrays:
        id_column = morsel.arrays[0]
        print(f"ID column type: {type(id_column)}")
        print(f"ID column values: {id_column.to_pylist()}")
        
        # Try to call equals
        try:
            result = id_column.equals(1)
            print(f"Equals result type: {type(result)}")
            print(f"Equals result values: {result.to_pylist()}")
        except Exception as e:
            print(f"Error calling equals: {e}")
            import traceback
            traceback.print_exc()

