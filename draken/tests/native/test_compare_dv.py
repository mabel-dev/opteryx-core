"""Tests for draken/ops/compare_dv.{h,cpp} — arena-backed compare entry
point for the native eval engine (Stage B: INT64 + FLOAT64).

The C API takes raw `DrakenVector*` and `DrakenFrameArena*` pointers, so
Python testing goes through a single nanobind smoke function
(`_compare_dv_smoke_test`) that exercises the dispatch end-to-end in C++
and reports per-step results.

Coverage:
  * INT64 EQ produces correct DRAKEN_BOOL bitmap
  * FLOAT64 LT produces correct DRAKEN_BOOL bitmap
  * BOOL EQ / LT produce correct DRAKEN_BOOL bitmaps (bit-packed operands)
  * DATE32 EQ / TIMESTAMP64 LT produce correct DRAKEN_BOOL bitmaps
  * DECIMAL EQ (int64-backed) produces correct DRAKEN_BOOL bitmap — routed
    through the same i64_compare_vector kernel as INT64/TIMESTAMP64
  * DECIMAL128 EQ (int128-backed) produces correct DRAKEN_BOOL bitmap —
    routed through the i128_compare_vector kernel
  * Unsupported type returns NULL (ARRAY — no compare kernel)
  * Cross-type operands return NULL
  * Length mismatch returns NULL
  * Out-of-range op_code returns NULL
  * NULL inputs return NULL
  * arena destroy after result allocation does not crash
"""

import pytest

from draken.draken_native import _bool_compare_fastpath_fuzz_test, _compare_dv_smoke_test


def test_bool_compare_fastpath_matches_uniform_path():
    """R5 close-out follow-up: bool_compare_vector's dense-identity byte-wise
    fast path (draken/ops/bool_compare.h) must be bit-for-bit identical to the
    uniform bit-by-bit loop it fast-paths around. The C++ fuzz harness builds
    the same random logical values two ways — dense-identity (hits the fast
    path) and dict-encoded (non-identity, forces the uniform loop) — across
    varied lengths (including non-multiple-of-8) and with/without nulls, over
    all 6 ops, and reports any mismatch.
    """
    result = _bool_compare_fastpath_fuzz_test()

    assert result["cases_run"] > 0
    assert result["mismatches"] == 0, (
        f"fast path diverged from the uniform path in {result['mismatches']} "
        f"of {result['cases_run']} cases"
    )
    assert result["all_match"] is True


def test_compare_dv_smoke():
    results = _compare_dv_smoke_test()

    expected_steps = {
        "arena_create",
        "int64_eq_returns_non_null",
        "int64_eq_result_is_bool",
        "int64_eq_result_length",
        "int64_eq_bitmap",
        "float64_lt_returns_non_null",
        "float64_lt_result_is_bool",
        "float64_lt_bitmap",
        # R5 close-out — BOOL is a supported branch now (bit-packed kernel),
        # so `unsupported_type_returns_null` is asserted against ARRAY instead.
        "bool_eq_returns_non_null",
        "bool_eq_result_is_bool",
        "bool_eq_bitmap",
        "bool_lt_returns_non_null",
        "bool_lt_bitmap",
        "unsupported_type_returns_null",
        "cross_type_returns_null",
        "length_mismatch_returns_null",
        "bad_op_code_returns_null",
        "null_input_returns_null",
        # Stage C additions:
        "date32_eq_returns_non_null",
        "date32_eq_result_is_bool",
        "date32_eq_bitmap",
        "timestamp64_lt_returns_non_null",
        "timestamp64_lt_result_is_bool",
        "timestamp64_lt_bitmap",
        "varchar_smoke_skipped",
        "decimal_eq_returns_non_null",
        "decimal_eq_result_is_bool",
        "decimal_eq_bitmap",
        "decimal128_eq_returns_non_null",
        "decimal128_eq_result_is_bool",
        "decimal128_eq_result_length",
        "decimal128_eq_bitmap",
        "destroy_no_crash",
    }

    missing = expected_steps - set(results.keys())
    assert not missing, f"smoke test missing steps: {missing}"

    failed = {step: passed for step, passed in results.items() if not passed}
    assert not failed, f"smoke test failures: {failed}"
