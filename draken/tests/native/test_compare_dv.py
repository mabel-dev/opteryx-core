"""Tests for draken/ops/compare_dv.{h,cpp} — arena-backed compare entry
point for the native eval engine (Stage B: INT64 + FLOAT64).

The C API takes raw `DrakenVector*` and `DrakenFrameArena*` pointers, so
Python testing goes through a single nanobind smoke function
(`_compare_dv_smoke_test`) that exercises the dispatch end-to-end in C++
and reports per-step results.

Coverage:
  * INT64 EQ produces correct DRAKEN_BOOL bitmap
  * FLOAT64 LT produces correct DRAKEN_BOOL bitmap
  * Unsupported type returns NULL (Stage B: BOOL not covered)
  * Cross-type operands return NULL
  * Length mismatch returns NULL
  * Out-of-range op_code returns NULL
  * NULL inputs return NULL
  * arena destroy after result allocation does not crash
"""

import pytest

from draken.draken_native import _compare_dv_smoke_test


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
        "decimal_returns_null_pending_descriptor",
        "destroy_no_crash",
    }

    missing = expected_steps - set(results.keys())
    assert not missing, f"smoke test missing steps: {missing}"

    failed = {step: passed for step, passed in results.items() if not passed}
    assert not failed, f"smoke test failures: {failed}"
