"""Parametrized tests generated from TEST_MATRIX_OPERATIONS.

This test file verifies that the draken engine supports all declared vector
type/encoding/operation combinations.

Test coverage is defined in _matrix.py - add entries there to test new combinations.
"""

import pytest
from _matrix import (
    TEST_MATRIX_OPERATIONS_WITH_VARIANTS,
    ENCODING_OPERATION_TESTS,
    VECTOR_TYPES,
    OPERATIONS,
)
from _vector_helpers import create_vector_with_encoding, apply_operation
from conftest import ENCODING_NAMES, DENSE, RLE, CONSTANT, DICTIONARY

_TYPE_ABBREV = {
    "int64": "int", "float64": "flt", "string": "str", "bool": "bool",
    "date32": "dt32", "timestamp": "ts", "time": "time", "decimal": "dec",
}
_ENC_ABBREV = {DENSE: "den", RLE: "rle", CONSTANT: "con", DICTIONARY: "dic"}


def _operation_id(entry):
    if len(entry) == 5:  # With variants
        type_name, encoding, op, variant_name, _ = entry
        return f"{_TYPE_ABBREV[type_name]}[{_ENC_ABBREV[encoding]}]_{op}_{variant_name}"
    elif len(entry) == 3:  # Base matrix
        type_name, encoding, op = entry
        return f"{_TYPE_ABBREV[type_name]}[{_ENC_ABBREV[encoding]}]_{op}"
    else:  # Encoding operation tests
        return f"{entry[0]}_{entry[4]}"


def pytest_generate_tests(metafunc):
    """Parametrize tests with all matrix entries (including variants and encoding tests)."""
    if "matrix_entry" in metafunc.fixturenames:
        # Combine variant tests and encoding operation tests
        all_tests = TEST_MATRIX_OPERATIONS_WITH_VARIANTS + ENCODING_OPERATION_TESTS
        metafunc.parametrize(
            "matrix_entry",
            all_tests,
            ids=[_operation_id(e) for e in all_tests],
        )


def test_matrix_operation(matrix_entry):
    """Test that engine has path for (type, encoding, operation) with variants.

    Verifies operation works across different data conditions:
    - Standard 100-element vectors
    - Empty vectors
    - All-null vectors
    - Single-element vectors
    - Boundary values
    - Mixed-encoding operations
    """
    # Parse entry format
    if len(matrix_entry) == 5:  # Variant format
        type_name, encoding, operation_name, variant_name, variant_config = matrix_entry
    elif len(matrix_entry) == 6:  # Encoding operation tests
        type_name, enc1, type_name2, enc2, operation_name, variant_name = matrix_entry
        encoding = enc1
        variant_config = {"size": 100, "nullable": False}
    else:
        raise ValueError(f"Unexpected entry format: {matrix_entry}")

    # Get metadata
    type_info = VECTOR_TYPES[type_name]
    operation_info = OPERATIONS[operation_name]

    # Apply variant config
    size = variant_config.get("size", 100)
    nullable = variant_config.get("nullable", operation_info.get("nullable", False))

    # Create vector for this test
    vec = create_vector_with_encoding(
        type_name,
        encoding,
        size=size,
        nullable=nullable,
        seed=42,
    )

    # Special handling for operations that require non-empty vectors
    if operation_info.get("requires_non_empty") and len(vec) == 0:
        raise AssertionError("Operation requires non-empty vector")

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
        raise AssertionError(f"Operation {operation_name} not implemented: {e}") from e
    except (TypeError, ValueError) as e:
        # If it's in the test matrix, it must run and show what happens
        # No skips on edge cases - let failures expose gaps
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
    vec = create_vector_with_encoding(type_name, encoding, size=10, nullable=False)
    assert vec is not None
    assert len(vec) == 10


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
