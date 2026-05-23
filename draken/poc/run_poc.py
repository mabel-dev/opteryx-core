"""
Run the Milestone E.0 binding POC.

Build first with:  python setup_poc.py build_ext --inplace
Then run with:     python run_poc.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import binding_poc

TEST_VALUES = [1, 2, 3, 4, 5, 100, -7, 42]

result = binding_poc.run_binding_poc(TEST_VALUES)

print(f"length         : {result['length']}")
print(f"non_null_count : {result['non_null_count']}")
print(f"sum            : {result['sum']}")
print(f"min            : {result['min']}")
print(f"max            : {result['max']}")

expected_sum = sum(TEST_VALUES)
expected_min = min(TEST_VALUES)
expected_max = max(TEST_VALUES)

assert result['sum']            == expected_sum, f"sum mismatch: {result['sum']} != {expected_sum}"
assert result['min']            == expected_min, f"min mismatch: {result['min']} != {expected_min}"
assert result['max']            == expected_max, f"max mismatch: {result['max']} != {expected_max}"
assert result['non_null_count'] == len(TEST_VALUES)

print("\nAll assertions passed — POC proves:")
print("  [+] cimport draken.core.buffers binds DrakenVector struct via buffers.pxd")
print("  [+] cdef extern from ops/int64_reductions.h namespace draken::ops works")
print("  [+] i64_sum / i64_min / i64_max run correctly (nogil, data[selection[i]] pattern)")
print("  [+] Manually-constructed DrakenVector (no mimalloc, no vector_alloc) is valid")
