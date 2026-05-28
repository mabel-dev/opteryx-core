"""
Phase 9c regression tests — verifying the three defect fixes.

Defect 1: _KernelContextWrapper.ctx_ptr AttributeError
Defect 2: NULL arithmetic SIGBUS
Defect 3: C-native telemetry counter
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import opteryx
from opteryx.expression.evaluator import evaluation


def test_defect1_array_extraction_binds():
    """Defect 1: Verify that array extraction can bind without AttributeError.

    Before fix: AttributeError: '_KernelContextWrapper' object has no attribute 'ctx_ptr'
    """
    session = opteryx.session()
    try:
        # This should bind without AttributeError
        result = session.execute_to_morsels(
            "SELECT missions[0] FROM testdata.astronauts LIMIT 1"
        )
        # Consume the result to ensure no bind-time errors
        for morsel in result:
            pass
        return True
    except AttributeError as e:
        if "ctx_ptr" in str(e):
            raise AssertionError(f"Defect 1 not fixed (array extraction): {e}")
        raise


def test_defect1_parameterized_cast_binds():
    """Defect 1: Verify that parameterized CAST can bind without AttributeError.

    Before fix: AttributeError at bind time for TIMESTAMP casts with unit.
    """
    session = opteryx.session()
    try:
        # This should bind without AttributeError
        result = session.execute_to_morsels(
            "SELECT CAST(100 AS INTEGER)::TIMESTAMP[ms]"
        )
        # Consume the result to ensure no bind-time errors
        for morsel in result:
            pass
        return True
    except AttributeError as e:
        if "ctx_ptr" in str(e):
            raise AssertionError(f"Defect 1 not fixed (param cast): {e}")
        raise


def test_defect2_null_arithmetic():
    """Defect 2: Verify that NULL arithmetic doesn't crash.

    Before fix: SIGBUS when C kernel dereferences DRAKEN_NULL data buffer.
    """
    session = opteryx.session()

    # Test NULL + literal
    try:
        result = session.execute_to_morsels("SELECT CAST(NULL AS INTEGER) + 5")
        for morsel in result:
            values = list(morsel[0])
            assert values[0] is None, f"Expected None, got {values[0]}"
    except Exception as e:
        if "Bus error" in str(e) or "SIGBUS" in str(e):
            raise AssertionError(f"Defect 2 not fixed (NULL + literal): {e}")
        raise

    # Test literal + NULL
    try:
        result = session.execute_to_morsels("SELECT 10 + CAST(NULL AS INTEGER)")
        for morsel in result:
            values = list(morsel[0])
            assert values[0] is None, f"Expected None, got {values[0]}"
    except Exception as e:
        if "Bus error" in str(e) or "SIGBUS" in str(e):
            raise AssertionError(f"Defect 2 not fixed (literal + NULL): {e}")
        raise

    return True


def test_defect3_telemetry_counter():
    """Defect 3: Verify C-native kernel calls are counted.

    Tests that the telemetry counter increments when C dispatch is used.
    """
    initial_count = evaluation.get_c_native_kernel_call_count()

    session = opteryx.session()

    # Execute NULL + integer (uses executor short-circuit for C-native dispatch).
    # The executor detects DRAKEN_NULL and returns all-null without calling the kernel,
    # but still increments the telemetry counter since it's taking the C-native path.
    result = session.execute_to_morsels("SELECT NULL + 1")
    for morsel in result:
        # Verify the result is null
        values = list(morsel[0])
        assert values[0] is None

    final_count = evaluation.get_c_native_kernel_call_count()

    assert final_count > initial_count, \
        f"Telemetry counter did not increment (initial={initial_count}, final={final_count})"

    return True


if __name__ == "__main__":
    print("Testing Defect 1 (array extraction)...", end=" ")
    test_defect1_array_extraction_binds()
    print("✓")

    print("Testing Defect 1 (parameterized cast)...", end=" ")
    test_defect1_parameterized_cast_binds()
    print("✓")

    print("Testing Defect 2 (NULL arithmetic)...", end=" ")
    test_defect2_null_arithmetic()
    print("✓")

    print("Testing Defect 3 (telemetry counter)...", end=" ")
    test_defect3_telemetry_counter()
    print("✓")

    print("\n✅ All Phase 9c defect fixes verified!")
