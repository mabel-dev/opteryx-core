#!/usr/bin/env python3
import pyarrow as pa
from pathlib import Path
from opteryx.draken import Morsel
from opteryx.draken.storage import write_morsel, read_morsel

# Test 1: Dictionary WITH nulls (the failing case)
print("=== Test 1: Dictionary with nulls ===")
dictionary1 = pa.array([b"one", None, b"three"], type=pa.binary())
indices1 = pa.array([0, 1, 2, None, 1, 0], type=pa.int8())
table1 = pa.table({"k": pa.DictionaryArray.from_arrays(indices1, dictionary1)})
original1 = Morsel.from_arrow(table1)
print(f"Original data: {original1.column(b'k').to_pylist()}")

path1 = Path("/tmp/test_dict_with_nulls.drkm")
write_morsel(path1, original1, {"codec_default": "none", "checksum_enabled": True})
restored1 = read_morsel(path1, {"checksum_enabled": True})
print(f"Restored data: {restored1.column(b'k').to_pylist()}")

# Test 2: Dictionary WITHOUT nulls
print("\n=== Test 2: Dictionary without nulls ===")
dictionary2 = pa.array([b"one", b"two", b"three"], type=pa.binary())
indices2 = pa.array([0, 1, 2, 1, 1, 0], type=pa.int8())
table2 = pa.table({"k": pa.DictionaryArray.from_arrays(indices2, dictionary2)})
original2 = Morsel.from_arrow(table2)
print(f"Original data: {original2.column(b'k').to_pylist()}")

path2 = Path("/tmp/test_dict_no_nulls.drkm")
write_morsel(path2, original2, {"codec_default": "none", "checksum_enabled": True})
restored2 = read_morsel(path2, {"checksum_enabled": True})
print(f"Restored data: {restored2.column(b'k').to_pylist()}")
