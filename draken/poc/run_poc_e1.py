"""
run_poc_e1.py — Milestone E.1 end-to-end POC runner.

Proves the architecture: zero object in .pyx, nanobind C++ is the Python edge.

Prerequisites:
  1. make compile          (builds draken_native.so)
  2. From repo root:
       python draken/poc/setup_poc_e1.py build_ext --inplace --build-lib draken/poc

Usage:
    python draken/poc/run_poc_e1.py
"""

import ctypes
import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, REPO_ROOT)
sys.path.insert(0, os.path.dirname(__file__))  # for poc_e1 in draken/poc/

# Load draken_native with RTLD_GLOBAL so draken_vector_unwrap is visible
# to poc_e1.so when it is loaded.
_old_flags = sys.getdlopenflags()
sys.setdlopenflags(ctypes.RTLD_GLOBAL | os.RTLD_NOW)
from draken import draken_native  # noqa: E402
sys.setdlopenflags(_old_flags)

import poc_e1  # noqa: E402  (must come after draken_native in RTLD_GLOBAL)


def _check(label, got, expected):
    if got != expected:
        print(f"  FAIL {label}: got {got!r}, expected {expected!r}")
        return False
    print(f"  PASS {label}")
    return True


def run():
    ok = True

    print("\n--- E.1 POC: sum_kernel (unwrap → cdef kernel → Python int) ---")

    v = draken_native.vector_from_sequence([1, 2, 3, 4, 5])
    ok &= _check("sum [1..5]", poc_e1.sum_kernel(v), 15)

    v_neg = draken_native.vector_from_sequence([-1, -2, -3])
    ok &= _check("sum negatives", poc_e1.sum_kernel(v_neg), -6)

    v_nulls = draken_native.vector_from_sequence([10, None, 30, None, 50])
    ok &= _check("sum with nulls", poc_e1.sum_kernel(v_nulls), 90)

    v_empty = draken_native.vector_from_sequence([])
    ok &= _check("sum empty", poc_e1.sum_kernel(v_empty), 0)

    print("\n--- E.1 POC: min_kernel / max_kernel ---")

    v = draken_native.vector_from_sequence([3, 1, 4, 1, 5, 9, 2, 6])
    ok &= _check("min", poc_e1.min_kernel(v), 1)
    ok &= _check("max", poc_e1.max_kernel(v), 9)

    print("\n--- E.1 POC: non-dense input shapes ---")

    v_const = draken_native.vector_from_constant(7, 4)
    ok &= _check("constant-encoded sum (4×7)", poc_e1.sum_kernel(v_const), 28)

    v_dict = draken_native.vector_from_dict([10, 20], [0, 1, 0, 1])
    ok &= _check("dict-encoded sum", poc_e1.sum_kernel(v_dict), 60)

    print("\n--- E.1 POC: TypeError on non-Vector (draken_vector_unwrap type-check) ---")

    for bad in (42, "string", [1, 2, 3], None, 3.14, {}):
        try:
            poc_e1.sum_kernel(bad)
            print(f"  FAIL TypeError not raised for {bad!r}")
            ok = False
        except TypeError:
            print(f"  PASS TypeError raised for {bad!r}")

    print()
    if ok:
        print("ALL PASS — E.1 POC complete.")
    else:
        print("FAILURES DETECTED — see above.")
        sys.exit(1)


if __name__ == "__main__":
    run()
