"""
run_poc_e7.py — Milestone E.7 end-to-end POC runner.

Proves draken_vector_own_string:
  - to_pylist() round-trip matches expected values.
  - _slot_fields() determinism: matches vector_from_string_sequence on the same input.
  - Construct/destroy stress: no crash → RAII frees all three buffers correctly.

Prerequisites:
  1. make compile          (builds draken_native.so)
  2. From repo root:
       python draken/poc/setup_poc_e7.py build_ext --inplace --build-lib draken/poc

Usage:
    python draken/poc/run_poc_e7.py
"""

import ctypes
import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, REPO_ROOT)
sys.path.insert(0, os.path.dirname(__file__))  # for poc_e7 in draken/poc/

# Load draken_native with RTLD_GLOBAL so draken_vector_own_string is visible.
_old_flags = sys.getdlopenflags()
sys.setdlopenflags(ctypes.RTLD_GLOBAL | os.RTLD_NOW)
from draken import draken_native as dn  # noqa: E402
sys.setdlopenflags(_old_flags)

import poc_e7  # noqa: E402  (must come after draken_native in RTLD_GLOBAL)


def _check(label, got, expected):
    if got != expected:
        print(f"  FAIL {label}: got {got!r}, expected {expected!r}")
        return False
    print(f"  PASS {label}")
    return True


def run():
    ok = True

    # Test values: inline ("hello", "world", ""), long ("café_long_string_xyz"),
    # None (null row), empty string "".
    # "café" is 5 bytes UTF-8; we need a long-form string (>12 bytes).
    test_values = ["hello", "world", "café_over_twelve!", None, ""]

    print("\n--- E.7 POC: to_pylist() round-trip ---")

    v_own = poc_e7.make_string_vec(test_values)
    got = v_own.to_pylist()
    ok &= _check("to_pylist matches", got, test_values)

    print("\n--- E.7 POC: _slot_fields() determinism vs vector_from_string_sequence ---")

    v_ref = dn.vector_from_string_sequence(test_values)
    for i, val in enumerate(test_values):
        if val is None:
            own_fields = v_own._slot_fields(i)
            ref_fields = v_ref._slot_fields(i)
            ok &= _check(f"  slot[{i}] (null) own==None", own_fields, None)
            ok &= _check(f"  slot[{i}] (null) ref==None", ref_fields, None)
        else:
            own_fields = v_own._slot_fields(i)
            ref_fields = v_ref._slot_fields(i)
            ok &= _check(f"  slot[{i}] {val!r} fields match", own_fields, ref_fields)

    print("\n--- E.7 POC: empty vector ---")

    v_empty = poc_e7.make_string_vec([])
    ok &= _check("empty to_pylist", v_empty.to_pylist(), [])
    ok &= _check("empty length", len(v_empty), 0)

    print("\n--- E.7 POC: all-null vector ---")

    v_nulls = poc_e7.make_string_vec([None, None, None])
    ok &= _check("all-null to_pylist", v_nulls.to_pylist(), [None, None, None])

    print("\n--- E.7 POC: all-inline strings (no arena) ---")

    inline_vals = ["a", "bc", "def", None, ""]
    v_inline = poc_e7.make_string_vec(inline_vals)
    ok &= _check("all-inline to_pylist", v_inline.to_pylist(), inline_vals)
    v_inline_ref = dn.vector_from_string_sequence(inline_vals)
    for i, val in enumerate(inline_vals):
        if val is not None:
            ok &= _check(f"  inline slot[{i}] {val!r}",
                         v_inline._slot_fields(i), v_inline_ref._slot_fields(i))

    print("\n--- E.7 POC: long strings only (all arena) ---")

    long_vals = ["hello world!!" , "abcdefghijklmn", None, "over_twelve_bytes"]
    v_long = poc_e7.make_string_vec(long_vals)
    ok &= _check("long-string to_pylist", v_long.to_pylist(), long_vals)
    v_long_ref = dn.vector_from_string_sequence(long_vals)
    for i, val in enumerate(long_vals):
        if val is not None:
            ok &= _check(f"  long slot[{i}] fields match",
                         v_long._slot_fields(i), v_long_ref._slot_fields(i))

    print("\n--- E.7 POC: construct/destroy stress (RAII) ---")

    stress_values = ["hello", "world!", "café_over_twelve!", None, ""]
    iters = poc_e7.stress_construct_destroy(stress_values, 500)
    ok &= _check("stress iterations", iters, 500)

    print()
    if ok:
        print("ALL PASS — E.7 POC complete.")
    else:
        print("FAILURES DETECTED — see above.")
        sys.exit(1)


if __name__ == "__main__":
    run()
