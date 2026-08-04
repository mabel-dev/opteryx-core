"""
Phase 9c regression tests — defects introduced by C dispatch path.

Tests for:
1. Defect 1: Extraction + parameterized casts (bind-time crash)
2. Defect 2: Null arithmetic (SIGBUS in C kernels)

Defect 3 (a C-native telemetry counter) was deleted along with the counter it
asserted on: it had ONE increment site, inside a binary op's all-null
short-circuit, so it never measured kernel dispatch — and that branch lives in
the Cython VM, which the native engine no longer runs. Verified dead: neither
`SELECT id + 2` nor `SELECT id * gravity` moved it.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import opteryx


def _extract_column_values(morsels, column_index=0):
    """Extract values from a column across all morsels.

    `morsel[i]` is ROW i, not column i — `.column(name)` is the column accessor
    (draken's morsel API deliberately splits the two). Reading the subscript here
    returned the first ROW, so a three-row single-column result measured as one
    value and every length assertion below was checking the wrong thing.
    """
    values = []
    for morsel in morsels:
        name = morsel.column_names[column_index]
        values.extend(morsel.column(name).to_pylist())
    return values


def test_extraction_array_subscript():
    """Test that array extraction works after Defect 1 fix.

    Before fix: AttributeError: '_KernelContextWrapper' object has no attribute 'ctx_ptr'
    At bind time during _linearize.
    """
    session = opteryx.session()

    try:
        # Array subscript extraction — caused AttributeError in _linearize before fix
        result = session.execute_to_morsels(
            "SELECT missions[0] FROM testdata.astronauts LIMIT 3"
        )
        values = _extract_column_values(result)
        assert len(values) == 3, f"Expected 3 values, got {len(values)}"
        # Each value should be a mission string or None
        for val in values:
            assert val is None or isinstance(val, str), \
                f"Expected string or None, got {type(val)}"
    except AttributeError as e:
        if "ctx_ptr" in str(e):
            raise AssertionError(f"Defect 1 not fixed: {e}")
        raise


def test_extraction_negative_index():
    """Test that negative array indices work."""
    session = opteryx.session()

    result = session.execute_to_morsels(
        "SELECT missions[-1] FROM testdata.astronauts LIMIT 2"
    )
    values = _extract_column_values(result)
    assert len(values) == 2
    for val in values:
        assert val is None or isinstance(val, str)


def test_parameterized_cast():
    """Test that parameterized CAST works after Defect 1 fix.

    Before fix: AttributeError: '_KernelContextWrapper' object has no attribute 'ctx_ptr'
    At bind time when resolving TIMESTAMP cast with unit parameter.
    """
    session = opteryx.session()

    try:
        # Parameterized cast (TIMESTAMP with unit) — caused AttributeError before fix
        # This test just needs to bind/execute without crashing at bind time
        result = session.execute_to_morsels(
            "SELECT CAST(1000 AS INTEGER)::TIMESTAMP[ms]"
        )
        values = _extract_column_values(result)
        assert len(values) >= 1
        # Just verify it returns something, no crash is the key
    except AttributeError as e:
        if "ctx_ptr" in str(e):
            raise AssertionError(f"Defect 1 not fixed (parameterized cast): {e}")
        raise


def test_null_plus_literal():
    """Test that NULL + 1 returns NULL (Defect 2: null arithmetic).

    Before fix: SIGBUS in C arithmetic kernel when dereferencing DRAKEN_NULL data buffer.
    """
    session = opteryx.session()

    result = session.execute_to_morsels("SELECT NULL + 1")
    values = _extract_column_values(result)
    assert len(values) == 1
    assert values[0] is None, f"Expected None, got {values[0]}"


def test_null_arithmetic_with_column():
    """Test that column + NULL returns all NULL.

    Before fix: SIGBUS in C arithmetic kernel when dereferencing DRAKEN_NULL.
    """
    session = opteryx.session()

    result = session.execute_to_morsels(
        "SELECT id + CAST(NULL AS INTEGER) FROM testdata.planets LIMIT 3"
    )
    values = _extract_column_values(result)
    assert len(values) == 3, f"Expected 3 values, got {len(values)}"
    # All values should be None due to null propagation
    for val in values:
        assert val is None, f"Expected None, got {val}"


def test_null_subtract():
    """Test that NULL - 5 returns NULL."""
    session = opteryx.session()

    result = session.execute_to_morsels("SELECT CAST(NULL AS INTEGER) - 5")
    values = _extract_column_values(result)
    assert len(values) == 1
    assert values[0] is None


def test_null_multiply():
    """Test that NULL * 10 returns NULL."""
    session = opteryx.session()

    result = session.execute_to_morsels("SELECT CAST(NULL AS INTEGER) * 10")
    values = _extract_column_values(result)
    assert len(values) == 1
    assert values[0] is None


def test_null_divide():
    """Test that division with NULL returns NULL."""
    session = opteryx.session()

    result = session.execute_to_morsels("SELECT 10 / CAST(NULL AS FLOAT)")
    values = _extract_column_values(result)
    assert len(values) == 1
    assert values[0] is None


def test_null_modulo():
    """Test that modulo with NULL returns NULL."""
    session = opteryx.session()

    result = session.execute_to_morsels("SELECT 10 % CAST(NULL AS INTEGER)")
    values = _extract_column_values(result)
    assert len(values) == 1
    assert values[0] is None


if __name__ == "__main__":
    test_extraction_array_subscript()
    print("✓ test_extraction_array_subscript")

    test_extraction_negative_index()
    print("✓ test_extraction_negative_index")

    test_parameterized_cast()
    print("✓ test_parameterized_cast")

    test_null_plus_literal()
    print("✓ test_null_plus_literal")

    test_null_arithmetic_with_column()
    print("✓ test_null_arithmetic_with_column")

    test_null_subtract()
    print("✓ test_null_subtract")

    test_null_multiply()
    print("✓ test_null_multiply")

    test_null_divide()
    print("✓ test_null_divide")

    test_null_modulo()
    print("✓ test_null_modulo")


    print("\nAll Phase 9c regression tests passed!")
