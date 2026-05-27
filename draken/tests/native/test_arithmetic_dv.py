"""Tests for draken/ops/arithmetic_dv.{h,cpp} — arena-backed binary
arithmetic entry point for the native eval engine (Stage B: INT64 +
FLOAT64, ops PLUS/MINUS/MULTIPLY/DIVIDE/MODULO).

The C API takes raw `DrakenVector*` and `DrakenFrameArena*` pointers, so
testing goes through a single nanobind smoke function
(`_arithmetic_dv_smoke_test`) that exercises dispatch end-to-end in C++
and reports per-step results.
"""

import pytest

from draken.draken_native import _arithmetic_dv_smoke_test


def test_arithmetic_dv_smoke():
    results = _arithmetic_dv_smoke_test()

    expected_steps = {
        "arena_create",
        "int64_plus_returns_non_null",
        "int64_plus_result_is_int64",
        "int64_plus_length",
        "int64_plus_values",
        "float64_mul_returns_non_null",
        "float64_mul_result_is_float64",
        "float64_mul_values",
        "cross_type_returns_null",
        "bad_op_returns_null",
        "unsupported_type_returns_null",
        "length_mismatch_returns_null",
        "destroy_no_crash",
    }

    missing = expected_steps - set(results.keys())
    assert not missing, f"smoke test missing steps: {missing}"

    failed = {step: passed for step, passed in results.items() if not passed}
    assert not failed, f"smoke test failures: {failed}"
