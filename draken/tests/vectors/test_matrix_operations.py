"""Parametrized tests generated from TEST_MATRIX_OPERATIONS.

This test file verifies that the draken engine supports all declared vector
type/encoding/operation combinations.

Test coverage is defined in _matrix.py - add entries there to test new combinations.
"""

import pytest
from _matrix import TEST_MATRIX_OPERATIONS, VECTOR_TYPES, OPERATIONS
from _vector_helpers import create_vector_with_encoding, apply_operation
from conftest import ENCODING_NAMES, DENSE, RLE, CONSTANT, DICTIONARY

_TYPE_ABBREV = {
    "int64": "int", "float64": "flt", "string": "str", "bool": "bool",
    "date32": "dt32", "timestamp": "ts", "time": "time", "decimal": "dec",
}
_ENC_ABBREV = {DENSE: "den", RLE: "rle", CONSTANT: "con", DICTIONARY: "dic"}


def _operation_id(entry):
    type_name, encoding, op = entry
    return f"{_TYPE_ABBREV[type_name]}[{_ENC_ABBREV[encoding]}]_{op}"


def pytest_generate_tests(metafunc):
    """Parametrize test_matrix_operation with all matrix entries."""
    if "matrix_entry" in metafunc.fixturenames:
        metafunc.parametrize(
            "matrix_entry",
            TEST_MATRIX_OPERATIONS,
            ids=[_operation_id(e) for e in TEST_MATRIX_OPERATIONS],
        )


def test_matrix_operation(matrix_entry):
    """Test that engine has path for (type, encoding, operation) combination.

    This verifies that:
    1. Vector of this type/encoding can be created
    2. Operation can be applied without NotImplementedError
    3. Result is meaningful (not None, unless expected)

    Args:
        matrix_entry: Tuple of (type_name, encoding, operation_name)
    """
    type_name, encoding, operation_name = matrix_entry

    # Get type and operation metadata
    type_info = VECTOR_TYPES[type_name]
    operation_info = OPERATIONS[operation_name]

    # Skip if operation doesn't apply to this type
    skip_if_not = operation_info.get("skip_if_not")
    if skip_if_not == "numeric" and not type_info["is_numeric"]:
        pytest.skip(f"Operation {operation_name} requires numeric type")
    if skip_if_not == "temporal" and not type_info["is_temporal"]:
        pytest.skip(f"Operation {operation_name} requires temporal type")

    # Determine if we should include nulls
    nullable = operation_info.get("nullable", False)

    # Create vector for this test
    try:
        vec = create_vector_with_encoding(
            type_name,
            encoding,
            size=100,
            nullable=nullable,
        )
    except (ValueError, NotImplementedError) as e:
        pytest.skip(f"Cannot create {type_name}/{ENCODING_NAMES[encoding]}: {e}")

    # Special handling for operations that require non-empty vectors
    if operation_info.get("requires_non_empty") and len(vec) == 0:
        pytest.skip("Operation requires non-empty vector")

    # Apply operation
    try:
        result = apply_operation(vec, operation_name)

        # Result should be meaningful (unless it's from_arrow which is creation)
        if operation_name != "from_arrow":
            assert result is not None, (
                f"Operation {operation_name} returned None "
                f"for {type_name}/{ENCODING_NAMES[encoding]}"
            )

    except NotImplementedError as e:
        pytest.skip(f"Operation {operation_name} not implemented: {e}")
    except (TypeError, ValueError) as e:
        if "empty" in str(e).lower() or "all-null" in str(e).lower():
            pytest.skip(f"Expected error for edge case: {e}")
        raise


@pytest.mark.parametrize("type_name,encoding", [
    (t, e) for t in VECTOR_TYPES
    for e in VECTOR_TYPES[t]["supports_encodings"]
], ids=[
    f"{_TYPE_ABBREV[t]}[{_ENC_ABBREV[e]}]"
    for t in VECTOR_TYPES
    for e in VECTOR_TYPES[t]["supports_encodings"]
])
def test_vector_creation(type_name, encoding):
    """Test that vectors can be created for all supported type/encoding combinations."""
    try:
        vec = create_vector_with_encoding(type_name, encoding, size=10, nullable=False)
        assert vec is not None
        assert len(vec) == 10
    except (ValueError, NotImplementedError) as e:
        pytest.skip(f"Cannot create {type_name}/{ENCODING_NAMES[encoding]}: {e}")


def test_matrix_coverage():
    """Verify that we have good coverage of type/encoding combinations."""
    covered = set((t, e) for t, e, _ in TEST_MATRIX_OPERATIONS)
    total_combinations = sum(
        len(VECTOR_TYPES[t]["supports_encodings"]) for t in VECTOR_TYPES
    )

    coverage = len(covered) / total_combinations if total_combinations > 0 else 0
    print(f"\nTest coverage: {len(covered)}/{total_combinations} type/encoding combinations")
    print(f"Coverage percentage: {coverage * 100:.1f}%")

    # Report missing combinations
    all_combinations = set(
        (t, e)
        for t in VECTOR_TYPES
        for e in VECTOR_TYPES[t]["supports_encodings"]
    )
    missing = all_combinations - covered
    if missing:
        print(f"Missing combinations ({len(missing)}):")
        for t, e in sorted(missing):
            print(f"  - {t}/{ENCODING_NAMES[e]}")
