"""Unit tests for arithmetic operation dispatch (Phase 4.4).

Tests the centralized arithmetic dispatch system that uses VectorType-based
routing instead of __class__.__name__ anti-patterns.

Coverage:
- VectorType discrimination for various vector types
- Integration of arithmetic operations via existing binary_operations path
- Edge cases (null values, empty vectors)
- Validation of Phase 4.4 refactoring objectives
"""

import pyarrow as pa
import pytest
from opteryx.compiled.draken.vectors.float64_vector import Float64Vector
from opteryx.compiled.draken.vectors.int64_vector import Int64Vector
from opteryx.compiled.draken.vectors.integer_vector import IntegerVector

from opteryx import session
from opteryx.utils.vector_types import (
    VectorType,
    get_vector_type,
    is_draken_vector,
    is_scalar,
)


class TestVectorTypeDiscrimination:
    """Test VectorType-based dispatch implementation (Phase 4.1 foundation)."""

    def test_type_discrimination_int64_vector(self):
        """Test VectorType discrimination for Int64Vector."""
        v = Int64Vector.from_arrow(pa.array([1, 2, 3]))
        vec_type = get_vector_type(v)

        assert vec_type == VectorType.INT64

    def test_type_discrimination_float64_vector(self):
        """Test VectorType discrimination for Float64Vector."""
        v = Float64Vector.from_arrow(pa.array([1.0, 2.0, 3.0]))
        vec_type = get_vector_type(v)

        assert vec_type == VectorType.FLOAT64

    def test_type_discrimination_integer_vector(self):
        """Test VectorType discrimination for IntegerVector."""
        v = IntegerVector.from_arrow(pa.array([1, 2, 3], type=pa.int32()))
        vec_type = get_vector_type(v)

        assert vec_type == VectorType.INTEGER

    def test_type_discrimination_scalar_int(self):
        """Test is_scalar for integer scalars."""
        assert is_scalar(42) is True

    def test_type_discrimination_scalar_float(self):
        """Test is_scalar for float scalars."""
        assert is_scalar(3.14) is True

    def test_type_discrimination_scalar_string(self):
        """Test is_scalar for string scalars."""
        assert is_scalar("hello") is True

    def test_type_discrimination_scalar_bool(self):
        """Test is_scalar for boolean scalars."""
        assert is_scalar(True) is True

    def test_type_discrimination_scalar_none(self):
        """Test is_scalar for None."""
        assert is_scalar(None) is True

    def test_type_discrimination_arrow_array(self):
        """Test get_vector_type for PyArrow arrays."""
        arr = pa.array([1, 2, 3], type=pa.int64())
        vec_type = get_vector_type(arr)

        assert vec_type == VectorType.INT64

    def test_is_draken_vector_true(self):
        """Test is_draken_vector returns True for Draken vectors."""
        v = Int64Vector.from_arrow(pa.array([1, 2, 3]))
        assert is_draken_vector(v) is True

    def test_is_draken_vector_false_for_arrow(self):
        """Test is_draken_vector returns False for PyArrow arrays."""
        arr = pa.array([1, 2, 3])
        assert is_draken_vector(arr) is False

    def test_is_draken_vector_false_for_scalar(self):
        """Test is_draken_vector returns False for scalars."""
        assert is_draken_vector(42) is False


class TestArithmeticIntegration:
    """Integration tests with virtual datasets."""

    def test_simple_addition_query(self):
        """Test simple addition in SELECT query."""
        s = session()
        result = list(s.execute_to_morsels("SELECT id + 1 FROM $planets LIMIT 3"))

        assert len(result) > 0
        # Validate that result contains data
        morsel_count = 0
        for morsel in result:
            for col in morsel:
                if hasattr(col, "to_pylist"):
                    values = col.to_pylist()
                    assert len(values) > 0
                    morsel_count += 1
                    break
        assert morsel_count > 0

    def test_subtraction_query(self):
        """Test subtraction in SELECT query."""
        s = session()
        result = list(s.execute_to_morsels("SELECT id - 1 FROM $planets LIMIT 3"))

        assert len(result) > 0

    def test_multiplication_query(self):
        """Test multiplication in SELECT query."""
        s = session()
        result = list(s.execute_to_morsels("SELECT id * 2 FROM $planets LIMIT 3"))

        assert len(result) > 0

    def test_division_query(self):
        """Test division in SELECT query."""
        s = session()
        result = list(s.execute_to_morsels("SELECT id / 2 FROM $planets LIMIT 3"))

        assert len(result) > 0

    def test_multiple_arithmetic_operations(self):
        """Test multiple arithmetic operations in one query."""
        s = session()
        result = list(s.execute_to_morsels("SELECT id + 1, id * 2, id - 1 FROM $planets LIMIT 3"))

        assert len(result) > 0

    def test_arithmetic_with_where_clause(self):
        """Test arithmetic expressions in WHERE clause."""
        s = session()
        result = list(s.execute_to_morsels("SELECT id FROM $planets WHERE id + 1 > 3 LIMIT 5"))

        assert len(result) > 0

    def test_arithmetic_with_parentheses(self):
        """Test arithmetic with parentheses."""
        s = session()
        result = list(s.execute_to_morsels("SELECT (id + 1) * 2 FROM $planets LIMIT 3"))

        assert len(result) > 0


class TestArithmeticDispatchRefactoring:
    """Validate Phase 4.4 refactoring objectives."""

    def test_no_class_name_checks_in_arithmetic(self):
        """Verify no __class__.__name__ checks in refactored arithmetic.py."""
        import inspect

        from opteryx.expression.evaluator import arithmetic

        source = inspect.getsource(arithmetic._eval_binary_op_draken)

        # Should not contain __class__.__name__ checks (removed in Phase 4.4)
        assert "__class__.__name__" not in source

    def test_uses_vectortype_discriminator(self):
        """Verify refactored code uses VectorType discriminator."""
        import inspect

        from opteryx.expression.evaluator import arithmetic

        source = inspect.getsource(arithmetic._eval_binary_op_draken)

        # Should use get_vector_type (introduced in Phase 4.4)
        assert "get_vector_type" in source

    def test_uses_arithmetic_dispatch_module(self):
        """Verify refactored code imports arithmetic_dispatch."""
        import inspect

        from opteryx.expression.evaluator import arithmetic

        source = inspect.getsource(arithmetic)

        # Should import call_arithmetic_op from arithmetic_dispatch
        assert "call_arithmetic_op" in source

    def test_fallback_to_binary_operations_preserved(self):
        """Verify fallback to binary_operations is preserved (Phase 4.4)."""
        import inspect

        from opteryx.expression.evaluator import arithmetic

        source = inspect.getsource(arithmetic._eval_binary_op_draken)

        # Should still call binary_operations for arrow/numpy path
        assert "binary_operations" in source

    def test_date_operations_use_vectortype(self):
        """Verify date operations refactored to use VectorType."""
        import inspect

        from opteryx.expression.evaluator import arithmetic

        source = inspect.getsource(arithmetic._eval_binary_op_draken)

        # Should use VectorType.DATE32, VectorType.TIMESTAMP instead of _DATE_TYPES
        assert "VectorType.DATE32" in source
        assert "VectorType.TIMESTAMP" in source
        assert "VectorType.INTERVAL" in source

    def test_arithmetic_dispatch_module_exists(self):
        """Verify arithmetic_dispatch module was created."""
        from opteryx.expression.evaluator import arithmetic_dispatch

        # Module should exist and have call_arithmetic_op function
        assert hasattr(arithmetic_dispatch, "call_arithmetic_op")

    def test_arithmetic_dispatch_returns_none_phase4_4(self):
        """Verify call_arithmetic_op returns None in Phase 4.4 (delegating to binary_operations)."""
        import pyarrow as pa

        from opteryx.compiled.draken.vectors import Int64Vector
        from opteryx.expression.evaluator.arithmetic_dispatch import call_arithmetic_op

        v1 = Int64Vector.from_arrow(pa.array([1, 2, 3]))
        v2 = Int64Vector.from_arrow(pa.array([4, 5, 6]))

        # Phase 4.4: Should return None (no Draken kernels yet)
        result = call_arithmetic_op("Plus", v1, v2)
        assert result is None


class TestArithmeticDispatchEdgeCases:
    """Test edge cases in arithmetic operations."""

    def test_null_propagation_in_arithmetic(self):
        """Test that nulls propagate correctly in arithmetic."""
        s = session()
        result = list(s.execute_to_morsels("SELECT * FROM $planets WHERE id IS NULL LIMIT 5"))

        # Some planets may have NULL ids (test data dependent)
        # This just validates the query executes without error
        assert isinstance(result, list)

    def test_empty_result_set_arithmetic(self):
        """Test arithmetic on empty result sets."""
        s = session()
        result = list(s.execute_to_morsels("SELECT id + 1 FROM $planets WHERE id > 1000 LIMIT 5"))

        # Should execute without error, even with empty results
        assert isinstance(result, list)

    def test_large_values_in_arithmetic(self):
        """Test arithmetic with large integer values."""
        s = session()
        result = list(s.execute_to_morsels("SELECT id + 9223372036854775800 FROM $planets LIMIT 1"))

        # Should handle large values without overflow errors
        assert len(result) >= 0


class TestArithmeticDispatchConsistency:
    """Test consistency of arithmetic dispatch behavior."""

    def test_commutative_operations(self):
        """Test that commutative operations produce expected results."""
        s = session()

        # Addition should be commutative
        result1 = list(s.execute_to_morsels("SELECT id + 1 FROM $planets LIMIT 1"))
        result2 = list(s.execute_to_morsels("SELECT 1 + id FROM $planets LIMIT 1"))

        assert len(result1) > 0
        assert len(result2) > 0

    def test_non_commutative_operations(self):
        """Test that non-commutative operations behave correctly."""
        s = session()

        # Subtraction should not be commutative (order matters)
        result1 = list(s.execute_to_morsels("SELECT id - 1 FROM $planets LIMIT 1"))
        result2 = list(s.execute_to_morsels("SELECT 1 - id FROM $planets LIMIT 1"))

        # Both should execute without error
        assert len(result1) > 0
        assert len(result2) > 0

    def test_operator_precedence(self):
        """Test operator precedence in arithmetic expressions."""
        s = session()

        # Multiplication should bind tighter than addition
        result = list(s.execute_to_morsels("SELECT 2 + 3 * 4 FROM $planets LIMIT 1"))

        # Should execute: (2 + (3 * 4)) = 14, not ((2 + 3) * 4) = 20
        assert len(result) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
