import sys
import os
sys.path.insert(0, os.getcwd())

import opteryx
import pyarrow as pa
from opteryx.compiled.draken.vectors.bool_vector import BoolVector
from opteryx.compiled.draken.morsels.morsel import Morsel

# Create a small morsel
names = ["id", "name"]
ids = pa.array([1, 2, 3, 4, 5, 6, 7, 8, 9])
names_arr = pa.array(["Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune", "Pluto"])
morsel = Morsel.from_vectors(names, [ids, names_arr])

print(f"Original morsel rows: {morsel.num_rows}")

# Create a mask where only id=1 is True (index 0)
mask = BoolVector.from_arrow(pa.array([True, False, False, False, False, False, False, False, False]))
print(f"Mask: {mask.to_arrow().to_pylist()}")
print(f"Mask any(): {mask.any()}")
print(f"Mask sum: {mask.to_arrow().cast(pa.int8()).sum().as_py()}")

# Apply filter
filtered = morsel.filter_mask(mask)
print(f"Filtered morsel rows: {filtered.num_rows}")

# Try another mask (id > 5)
mask2 = BoolVector.from_arrow(pa.array([False, False, False, False, False, True, True, True, True]))
filtered2 = morsel.filter_mask(mask2)
print(f"Filtered2 (id > 5) rows: {filtered2.num_rows}")
