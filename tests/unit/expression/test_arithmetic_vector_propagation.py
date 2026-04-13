"""
Tests for Phase 5.3.2: Draken Vector Propagation Through Arithmetic Expressions

These tests verify that arithmetic operations on Draken vectors:
1. Return native Draken vectors (not converted to numpy/PyArrow)
2. Preserve null semantics end-to-end
3. Support all arithmetic operators (+, -, *, /, %)
4. Handle mixed operands (vector + scalar)
5. Enable direct consumption by downstream operators
"""

import pytest

import opteryx
from opteryx.utils.vector_types import VectorType, get_vector_type, is_draken_vector


@pytest.fixture
def session():
    """Create a fresh Opteryx session for each test."""
    return opteryx.session()


def test_int_addition_vector_propagation(session):
    """Test that vector + scalar returns a Draken vector."""
    sql = "SELECT id + 1 AS result FROM $planets"
    morsels = session.execute_to_morsels(sql)

    for morsel in morsels:
        result_col = morsel.column(b"result")
        # Result should be an IntegerVector (from Phase 5.3.2)
        assert is_draken_vector(result_col), f"Expected Draken vector, got {type(result_col)}"
        assert get_vector_type(result_col) == VectorType.INTEGER


def test_int_subtraction_vector_propagation(session):
    """Test that vector - scalar returns a Draken vector."""
    sql = "SELECT id - 1 AS result FROM $planets"
    morsels = session.execute_to_morsels(sql)

    for morsel in morsels:
        result_col = morsel.column(b"result")
        assert is_draken_vector(result_col), f"Expected Draken vector, got {type(result_col)}"
        assert get_vector_type(result_col) == VectorType.INTEGER


def test_int_multiplication_vector_propagation(session):
    """Test that vector * scalar returns a Draken vector."""
    sql = "SELECT id * 2 AS result FROM $planets"
    morsels = session.execute_to_morsels(sql)

    for morsel in morsels:
        result_col = morsel.column(b"result")
        assert is_draken_vector(result_col), f"Expected Draken vector, got {type(result_col)}"
        assert get_vector_type(result_col) == VectorType.INTEGER


def test_int_modulo_vector_propagation(session):
    """Test that vector % scalar returns a Draken vector."""
    sql = "SELECT id % 2 AS result FROM $planets"
    morsels = session.execute_to_morsels(sql)

    for morsel in morsels:
        result_col = morsel.column(b"result")
        assert is_draken_vector(result_col), f"Expected Draken vector, got {type(result_col)}"
        assert get_vector_type(result_col) == VectorType.INTEGER


def test_bitwise_and_vector_propagation(session):
    """Test that vector & scalar returns a Draken vector."""
    sql = "SELECT id & 3 AS result FROM $planets"
    morsels = session.execute_to_morsels(sql)

    for morsel in morsels:
        result_col = morsel.column(b"result")
        assert is_draken_vector(result_col), f"Expected Draken vector, got {type(result_col)}"
        assert get_vector_type(result_col) == VectorType.INTEGER


def test_bitwise_or_vector_propagation(session):
    """Test that vector | scalar returns a Draken vector."""
    sql = "SELECT id | 1 AS result FROM $planets"
    morsels = session.execute_to_morsels(sql)

    for morsel in morsels:
        result_col = morsel.column(b"result")
        assert is_draken_vector(result_col), f"Expected Draken vector, got {type(result_col)}"
        assert get_vector_type(result_col) == VectorType.INTEGER


def test_bitwise_xor_vector_propagation(session):
    """Test that vector ^ scalar returns a Draken vector."""
    sql = "SELECT id ^ 2 AS result FROM $planets"
    morsels = session.execute_to_morsels(sql)

    for morsel in morsels:
        result_col = morsel.column(b"result")
        assert is_draken_vector(result_col), f"Expected Draken vector, got {type(result_col)}"
        assert get_vector_type(result_col) == VectorType.INTEGER


def test_chained_arithmetic_vector_propagation(session):
    """Test that chained arithmetic expressions propagate vectors through."""
    sql = "SELECT (id + 1) * 2 AS result FROM $planets"
    morsels = session.execute_to_morsels(sql)

    for morsel in morsels:
        result_col = morsel.column(b"result")
        # Result of (vector + scalar) * scalar should be Draken vector
        assert is_draken_vector(result_col), f"Expected Draken vector, got {type(result_col)}"
        assert get_vector_type(result_col) == VectorType.INTEGER


def test_arithmetic_correctness_with_vector_propagation(session):
    """Test that arithmetic results are correct with vector propagation."""
    sql = "SELECT id, id + 10 AS id_plus_10 FROM $planets ORDER BY id"
    morsels = session.execute_to_morsels(sql)

    for morsel in morsels:
        id_col = morsel.column(b"id")
        result_col = morsel.column(b"id_plus_10")

        # Verify vector propagation
        assert is_draken_vector(result_col), f"Expected Draken vector, got {type(result_col)}"

        # Verify correctness
        id_vals = id_col.to_pylist()
        result_vals = result_col.to_pylist()

        for id_val, result_val in zip(id_vals, result_vals):
            if id_val is not None:
                assert result_val == id_val + 10, f"Expected {id_val + 10}, got {result_val}"
            else:
                assert result_val is None, f"Expected None for null input, got {result_val}"


def test_expression_with_cast_and_arithmetic(session):
    """Test that casting returns Draken vector which flows to arithmetic."""
    sql = "SELECT CAST(id AS DOUBLE) + 0.5 AS result FROM $planets"
    morsels = session.execute_to_morsels(sql)

    for morsel in morsels:
        result_col = morsel.column(b"result")
        # Result should be Float64Vector (from cast) + scalar = Float64Vector
        assert is_draken_vector(result_col), f"Expected Draken vector, got {type(result_col)}"
        assert get_vector_type(result_col) == VectorType.FLOAT64


def test_cast_result_propagates_through_select(session):
    """Test that cast operations directly return Draken vectors."""
    sql = "SELECT CAST(id AS DOUBLE) AS double_id FROM $planets"
    morsels = session.execute_to_morsels(sql)

    for morsel in morsels:
        result_col = morsel.column(b"double_id")
        # Cast should return native Float64Vector
        assert is_draken_vector(result_col), f"Expected Draken vector, got {type(result_col)}"
        assert get_vector_type(result_col) == VectorType.FLOAT64


def test_multiple_arithmetic_operations_in_select(session):
    """Test multiple arithmetic expressions in same SELECT."""
    sql = "SELECT id + 1 AS plus_one, id * 2 AS times_two, id - 1 AS minus_one FROM $planets"
    morsels = session.execute_to_morsels(sql)

    for morsel in morsels:
        # All should be Draken vectors
        for col_name in [b"plus_one", b"times_two", b"minus_one"]:
            result_col = morsel.column(col_name)
            assert is_draken_vector(result_col), (
                f"Column {col_name}: Expected Draken vector, got {type(result_col)}"
            )
            assert get_vector_type(result_col) == VectorType.INTEGER
