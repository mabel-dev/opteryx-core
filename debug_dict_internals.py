#!/usr/bin/env python3
import pyarrow as pa
from opteryx.draken import Morsel

# Create a dictionary vector
dictionary = pa.array([b"one", None, b"three"], type=pa.binary())
indices = pa.array([0, 1, 2, None, 1, 0], type=pa.int8())
table = pa.table({"k": pa.DictionaryArray.from_arrays(indices, dictionary)})
original = Morsel.from_arrow(table)
vec = original.column(b'k')

print(f"Dictionary Vector class: {type(vec).__name__}")
print(f"Encoding: {vec.encoding}")
print(f"Length: {len(vec)}")
print(f"Data: {vec.to_pylist()}")

# Try to access dict_accessor
#if hasattr(vec, 'dict_accessor'):
#    print(f"\ndict_accessor method exists")
#    try:
#        accessor = vec.dict_accessor()
#        print(f"dict_accessor: {accessor}")
#    except Exception as e:
#        print(f"Error calling dict_accessor: {e}")

# Check encoding method
if hasattr(vec, 'dict_accessor'):
    print(f"\nHas dict_accessor")
#    try:
#        accessor = vec.dict_accessor()
#        print(f"Accessor result: {accessor}")
#    except Exception as e:
#        print(f"Error: {e}")

# Try accessing dictionary data
print(f"\nTrying to access dictionary through to_arrow:")
try:
    arrow = vec.to_arrow()
    print(f"Arrow repr: {arrow}")
    print(f"Dictionary: {arrow.dictionary}")
    print(f"Indices: {arrow.indices}")
except Exception as e:
    print(f"Error: {e}")
