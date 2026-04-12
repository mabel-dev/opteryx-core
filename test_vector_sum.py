import sys
import os
sys.path.insert(0, os.getcwd())

import pyarrow as pa
from opteryx.compiled.draken.vectors.bool_vector import BoolVector

mask = BoolVector.from_arrow(pa.array([True, False, False, False, False, False, False, False, False]))
print(f"Mask: {mask.to_arrow().to_pylist()}")
print(f"Mask any(): {mask.any()}")
