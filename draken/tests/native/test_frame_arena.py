"""Tests for draken/core/frame_arena.{h,cpp} — per-frame allocator for the
native eval engine.

The arena's public API is C/C++ only (the eval engine uses it from Cython
via `cdef extern from "frame_arena.h"`). Python testing goes through a
single nanobind smoke function (`_frame_arena_smoke_test`) that exercises
the lifecycle in C++ and reports per-step results.
"""

import pytest

from draken.draken_native import _frame_arena_smoke_test


def test_frame_arena_lifecycle():
    """Run the C++-side smoke test and assert every step passed."""
    results = _frame_arena_smoke_test()

    expected_steps = {
        "create_returns_non_null",
        "initial_size_zero",
        "alloc1_non_null",
        "alloc2_non_null",
        "size_after_two_allocs",
        "buffers_writable",
        "size_after_release",
        "released_ptr_still_writable",
        "size_unchanged_after_noop_releases",
        "destroy_null_is_noop",
        "caller_can_free_released",
        "zero_alloc_did_not_crash",
        "zero_alloc_tracked",
        "adopt_increments_size",
        "adopt_freed_on_destroy",
        "adopt_then_release_size_zero",
        "adopt_then_release_caller_owns",
        "adopt_null_is_noop",
    }

    missing = expected_steps - set(results.keys())
    assert not missing, f"smoke test missing steps: {missing}"

    failed = {step: passed for step, passed in results.items() if not passed}
    assert not failed, f"smoke test failures: {failed}"


def test_frame_arena_smoke_test_returns_dict():
    """Sanity: the bindings return a dict (not list/tuple) so we can name steps."""
    results = _frame_arena_smoke_test()
    assert isinstance(results, dict)
    assert len(results) > 0
