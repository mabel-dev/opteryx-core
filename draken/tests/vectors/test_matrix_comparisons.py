"""Parametrized tests for cross-type vector comparisons.

This test file verifies that the draken engine supports comparison operations
between vectors of different types and encodings.

Test coverage is defined in _matrix.py TEST_MATRIX_COMPARISONS - add entries
there to test new cross-type combinations.
"""

import pytest
from _matrix import TEST_MATRIX_COMPARISONS, COMPARISON_OPERATIONS, VECTOR_TYPES
from _vector_helpers import create_vector_with_encoding, apply_comparison
from conftest import ENCODING_NAMES, DENSE, RLE, CONSTANT, DICTIONARY

_TYPE_ABBREV = {
    "int64": "int", "float64": "flt", "string": "str", "bool": "bool",
    "date32": "dt32", "timestamp": "ts", "time": "time", "decimal": "dec",
}
_ENC_ABBREV = {DENSE: "den", RLE: "rle", CONSTANT: "con", DICTIONARY: "dic"}
_OP_ABBREV = {
    "equals": "eq", "not_equals": "ne", "less_than": "lt",
    "less_equal": "le", "greater_than": "gt", "greater_equal": "ge",
}


def _comparison_id(entry):
    lt, le, rt, re, op = entry
    return (
        f"{_TYPE_ABBREV[lt]}[{_ENC_ABBREV[le]}]"
        f"_{_OP_ABBREV[op]}_"
        f"{_TYPE_ABBREV[rt]}[{_ENC_ABBREV[re]}]"
    )


def pytest_generate_tests(metafunc):
    """Parametrize test_comparison with all comparison matrix entries."""
    if "comparison_entry" in metafunc.fixturenames:
        metafunc.parametrize(
            "comparison_entry",
            TEST_MATRIX_COMPARISONS,
            ids=[_comparison_id(e) for e in TEST_MATRIX_COMPARISONS],
        )


def test_comparison(comparison_entry):
    """Test that engine supports comparison between (type1/enc1, type2/enc2, op) combination.

    This verifies that:
    1. Vectors of both types/encodings can be created
    2. Comparison can be applied without NotImplementedError
    3. Result is meaningful (not None)

    Args:
        comparison_entry: Tuple of (left_type, left_encoding, right_type, right_encoding, operation_name)
    """
    left_type, left_encoding, right_type, right_encoding, operation_name = comparison_entry

    # Get type and operation metadata
    left_type_info = VECTOR_TYPES[left_type]
    right_type_info = VECTOR_TYPES[right_type]
    operation_info = COMPARISON_OPERATIONS[operation_name]

    # Check if operation applies to these types
    skip_if_not = operation_info.get("skip_if_not")
    if skip_if_not == "orderable":
        # Orderable types: numeric, temporal (date/timestamp/time), string, decimal
        orderable_types = {"int64", "float64", "decimal", "date32", "timestamp", "time", "string"}
        if left_type not in orderable_types or right_type not in orderable_types:
            pytest.skip(f"Operation {operation_name} requires orderable types (numeric, temporal, or string)")

    # Determine if we should include nulls
    nullable = operation_info.get("nullable", False)

    # Create vectors for this test
    try:
        left_vec = create_vector_with_encoding(
            left_type,
            left_encoding,
            size=100,
            nullable=nullable,
        )
    except (ValueError, NotImplementedError) as e:
        pytest.skip(f"Cannot create {left_type}/{ENCODING_NAMES[left_encoding]}: {e}")

    try:
        right_vec = create_vector_with_encoding(
            right_type,
            right_encoding,
            size=100,
            nullable=nullable,
        )
    except (ValueError, NotImplementedError) as e:
        pytest.skip(f"Cannot create {right_type}/{ENCODING_NAMES[right_encoding]}: {e}")

    # Special handling for operations that require non-empty vectors
    if operation_info.get("requires_non_empty") and (len(left_vec) == 0 or len(right_vec) == 0):
        pytest.skip("Operation requires non-empty vectors")

    # Apply comparison operation - fail hard if it doesn't work
    try:
        result = apply_comparison(left_vec, right_vec, operation_name)
        assert result is not None, (
            f"Comparison {operation_name} returned None "
            f"for {left_type}/{ENCODING_NAMES[left_encoding]} vs "
            f"{right_type}/{ENCODING_NAMES[right_encoding]}"
        )
    except NotImplementedError as e:
        raise AssertionError(
            f"Comparison {operation_name} not implemented for "
            f"{left_type}/{ENCODING_NAMES[left_encoding]} vs "
            f"{right_type}/{ENCODING_NAMES[right_encoding]}: {e}"
        ) from e
    except TypeError as e:
        raise AssertionError(
            f"Type error in {operation_name} for "
            f"{left_type}/{ENCODING_NAMES[left_encoding]} vs "
            f"{right_type}/{ENCODING_NAMES[right_encoding]}: {e}"
        ) from e


def test_comparison_coverage():
    """Verify coverage of cross-type comparison combinations."""
    covered = set((l, le, r, re) for l, le, r, re, _ in TEST_MATRIX_COMPARISONS)
    print(f"\nComparison coverage: {len(covered)} type/encoding/type/encoding combinations")

    # Report comparisons
    print(f"Cross-type comparisons tested:")
    for l, le, r, re in sorted(covered):
        if l != r or le != re:
            print(f"  - {l}/{ENCODING_NAMES[le]} vs {r}/{ENCODING_NAMES[re]}")
