"""
run_poc_e7.py — Milestone E.7 end-to-end POC runner (type-family revision).

Proves draken_vector_own_string with type parameter:
  - VARCHAR: to_pylist() round-trip + _slot_fields() determinism + RAII stress.
  - NVARCHAR: to_pylist() round-trip; same storage; type tag differs.
  - VARBINARY: to_pylist() returns bytes objects; round-trip correct.

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

    print("\n--- E.7 POC: NVARCHAR round-trip ---")

    # Verify NVARCHAR stores str, returns str, and type tag is correct.
    nv_vals = ["café", "日本", None, "hello"]
    v_nv = poc_e7.make_nvarchar_vec(nv_vals)
    ok &= _check("nvarchar to_pylist", v_nv.to_pylist(), nv_vals)

    import draken.draken_native as dn2
    ok &= _check("nvarchar type tag", v_nv.type, dn2.DrakenType.NVARCHAR)

    # _slot_fields determinism: same bytes → same slot regardless of VARCHAR vs NVARCHAR.
    v_nv_ref = dn.vector_from_nvarchar_sequence(nv_vals)
    ok &= _check("nvarchar type via factory", v_nv_ref.type, dn2.DrakenType.NVARCHAR)
    for i, val in enumerate(nv_vals):
        if val is not None:
            ok &= _check(f"  nvarchar slot[{i}] {val!r}",
                         v_nv._slot_fields(i), v_nv_ref._slot_fields(i))

    print("\n--- E.7 POC: VARBINARY round-trip ---")

    # Verify VARBINARY stores bytes, returns bytes, and type tag is correct.
    bv_vals = [b"hello", b"\x00\x01\x02", None, b"caf\xc3\xa9", b""]
    v_bv = poc_e7.make_bytes_vec(bv_vals)
    ok &= _check("varbinary to_pylist", v_bv.to_pylist(), bv_vals)
    ok &= _check("varbinary type tag", v_bv.type, dn2.DrakenType.VARBINARY)

    v_bv_ref = dn.vector_from_bytes_sequence(bv_vals)
    ok &= _check("varbinary type via factory", v_bv_ref.type, dn2.DrakenType.VARBINARY)
    ok &= _check("varbinary factory to_pylist", v_bv_ref.to_pylist(), bv_vals)

    # Null handling.
    v_bv_null = poc_e7.make_bytes_vec([None, None])
    ok &= _check("varbinary all-null", v_bv_null.to_pylist(), [None, None])

    print()
    if ok:
        print("ALL PASS — E.7 POC complete.")
    else:
        print("FAILURES DETECTED — see above.")
        sys.exit(1)


if __name__ == "__main__":
    run()
