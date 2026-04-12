import sys
import os
sys.path.insert(0, os.getcwd())

import pyarrow as pa
from opteryx.compiled.draken.vectors.bool_vector import BoolVector
from opteryx.compiled.draken.morsels.morsel import Morsel

# Create a small morsel
names = ["id"]
ids = pa.array([1, 2, 3, 4, 5, 6, 7, 8, 9])
morsel = Morsel.from_vectors(names, [ids])

print(f"Original morsel rows: {morsel.num_rows}")

# Create a mask where only id=1 is True (index 0)
mask = BoolVector.from_arrow(pa.array([True, False, False, False, False, False, False, False, False]))
print(f"Mask any(): {mask.any()}")

# Apply filter
try:
    filtered = morsel.filter_mask(mask)
    print(f"Filtered morsel rows: {filtered.num_rows}")
except Exception as e:
    print(f"Error filtering: {e}")

