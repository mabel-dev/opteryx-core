"""
Comprehensive test suite for scalar_to_vector conversion (Step 2).

Tests the canonical scalar-to-Draken-vector conversion pathway that replaces
numpy/pyarrow intermediate conversions during NumPy-Arrow eradication.

Coverage:
- All OrsoTypes (NULL, BOOLEAN, INTEGER, DOUBLE, VARCHAR, BLOB, DATE, TIMESTAMP, etc.)
- Null/NaN handling
- Type preservation (scalar in → vector out with correct dtype)
- Type inference (None dtype parameter)
- Constant vectors (length > 1)
- Error cases (invalid scalars, type mismatches)
- Native Python scalars (preferred)
- numpy/pyarrow scalars (transition phase)

Note: Draken's StringVector returns bytes when calling to_arrow().to_pylist(),
even though the OrsoType is VARCHAR. This is expected behavior.
"""

import datetime
import decimal
import sys
from pathlib import Path

import pytest

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from opteryx.types import OrsoTypes, scalar_to_draken_vector


class TestScalarToVectorBasic:
    """Basic scalar_to_vector conversion tests."""

    def test_boolean_true(self):
        """Test converting True to BoolVector."""
        vec = scalar_to_draken_vector(True, OrsoTypes.BOOLEAN)
        result = vec.to_arrow().to_pylist()
        assert result == [True]

    def test_boolean_false(self):
        """Test converting False to BoolVector."""
        vec = scalar_to_draken_vector(False, OrsoTypes.BOOLEAN)
        result = vec.to_arrow().to_pylist()
        assert result == [False]

    def test_integer_positive(self):
        """Test converting positive integer."""
        vec = scalar_to_draken_vector(42, OrsoTypes.INTEGER)
        result = vec.to_arrow().to_pylist()
        assert result == [42]

    def test_integer_negative(self):
        """Test converting negative integer."""
        vec = scalar_to_draken_vector(-100, OrsoTypes.INTEGER)
        result = vec.to_arrow().to_pylist()
        assert result == [-100]

    def test_integer_zero(self):
        """Test converting zero."""
        vec = scalar_to_draken_vector(0, OrsoTypes.INTEGER)
        result = vec.to_arrow().to_pylist()
        assert result == [0]

    def test_double_positive(self):
        """Test converting positive double."""
        vec = scalar_to_draken_vector(3.14, OrsoTypes.DOUBLE)
        result = vec.to_arrow().to_pylist()
        assert len(result) == 1
        assert abs(result[0] - 3.14) < 1e-6

    def test_double_negative(self):
        """Test converting negative double."""
        vec = scalar_to_draken_vector(-2.71, OrsoTypes.DOUBLE)
        result = vec.to_arrow().to_pylist()
        assert len(result) == 1
        assert abs(result[0] - (-2.71)) < 1e-6

    def test_varchar_ascii(self):
        """Test converting ASCII string.

        Note: Draken StringVector returns bytes from to_arrow().to_pylist().
        """
        vec = scalar_to_draken_vector("hello", OrsoTypes.VARCHAR)
        result = vec.to_arrow().to_pylist()
        assert result == [b"hello"]

    def test_varchar_empty(self):
        """Test converting empty string.

        Note: Empty string becomes empty bytes in Draken.
        """
        vec = scalar_to_draken_vector("", OrsoTypes.VARCHAR)
        result = vec.to_arrow().to_pylist()
        assert result == [b""]

    def test_varchar_unicode(self):
        """Test converting unicode string.

        Unicode is preserved through UTF-8 encoding.
        """
        unicode_str = "hello 世界 🌍"
        vec = scalar_to_draken_vector(unicode_str, OrsoTypes.VARCHAR)
        result = vec.to_arrow().to_pylist()
        assert result == [unicode_str.encode("utf-8")]

    def test_blob_bytes(self):
        """Test converting bytes."""
        data = b"hello bytes"
        vec = scalar_to_draken_vector(data, OrsoTypes.BLOB)
        result = vec.to_arrow().to_pylist()
        assert result == [data]


class TestScalarToVectorTemporal:
    """Test temporal type conversions."""

    def test_date_basic(self):
        """Test converting datetime.date."""
        d = datetime.date(2024, 1, 15)
        vec = scalar_to_draken_vector(d, OrsoTypes.DATE)
        result = vec.to_arrow().to_pylist()
        assert result == [d]

    def test_date_from_datetime(self):
        """Test converting datetime.datetime to DATE (should extract date part)."""
        dt = datetime.datetime(2024, 1, 15, 10, 30, 45)
        vec = scalar_to_draken_vector(dt, OrsoTypes.DATE)
        result = vec.to_arrow().to_pylist()
        # Result should be the date part only
        assert result == [datetime.date(2024, 1, 15)]

    def test_time_basic(self):
        """Test converting datetime.time."""
        t = datetime.time(10, 30, 45)
        vec = scalar_to_draken_vector(t, OrsoTypes.TIME)
        result = vec.to_arrow().to_pylist()
        # Compare time values (Arrow may use microsecond precision)
        assert result[0].hour == 10
        assert result[0].minute == 30
        assert result[0].second == 45

    def test_timestamp_datetime(self):
        """Test converting datetime.datetime to TIMESTAMP."""
        dt = datetime.datetime(2024, 1, 15, 10, 30, 45)
        vec = scalar_to_draken_vector(dt, OrsoTypes.TIMESTAMP)
        result = vec.to_arrow().to_pylist()
        assert result == [dt]

    def test_timestamp_from_date(self):
        """Test converting datetime.date to TIMESTAMP (should promote to datetime at midnight)."""
        d = datetime.date(2024, 1, 15)
        vec = scalar_to_draken_vector(d, OrsoTypes.TIMESTAMP)
        result = vec.to_arrow().to_pylist()
        expected = datetime.datetime(2024, 1, 15, 0, 0, 0)
        assert result == [expected]

    def test_interval_timedelta(self):
        """Test converting datetime.timedelta."""
        td = datetime.timedelta(days=5, hours=3, minutes=30)
        vec = scalar_to_draken_vector(td, OrsoTypes.INTERVAL)
        result = vec.to_arrow().to_pylist()
        assert result == [td]


class TestScalarToVectorNumericTypes:
    """Test numeric type conversions."""

    def test_decimal_from_decimal(self):
        """Test converting decimal.Decimal."""
        d = decimal.Decimal("123.45")
        vec = scalar_to_draken_vector(d, OrsoTypes.DECIMAL)
        result = vec.to_arrow().to_pylist()
        assert result == [d]

    def test_decimal_from_int(self):
        """Test converting int to DECIMAL."""
        vec = scalar_to_draken_vector(42, OrsoTypes.DECIMAL)
        result = vec.to_arrow().to_pylist()
        assert result == [decimal.Decimal("42")]

    def test_decimal_from_float(self):
        """Test converting float to DECIMAL."""
        vec = scalar_to_draken_vector(3.14, OrsoTypes.DECIMAL)
        result = vec.to_arrow().to_pylist()
        # Decimal conversion from float may have precision variations
        # Draken may preserve as float or convert to Decimal
        assert len(result) == 1
        assert isinstance(result[0], (decimal.Decimal, float))

    def test_integer_from_float(self):
        """Test converting float to INTEGER (truncates)."""
        vec = scalar_to_draken_vector(3.14, OrsoTypes.INTEGER)
        result = vec.to_arrow().to_pylist()
        assert result == [3]

    def test_double_from_integer(self):
        """Test converting int to DOUBLE."""
        vec = scalar_to_draken_vector(42, OrsoTypes.DOUBLE)
        result = vec.to_arrow().to_pylist()
        assert result == [42.0]


class TestScalarToVectorNullHandling:
    """Test null/None handling across types."""

    def test_null_boolean(self):
        """Test NULL vector with BOOLEAN type."""
        vec = scalar_to_draken_vector(None, OrsoTypes.BOOLEAN)
        result = vec.to_arrow().to_pylist()
        assert result == [None]

    def test_null_integer(self):
        """Test NULL vector with INTEGER type."""
        vec = scalar_to_draken_vector(None, OrsoTypes.INTEGER)
        result = vec.to_arrow().to_pylist()
        assert result == [None]

    def test_null_double(self):
        """Test NULL vector with DOUBLE type."""
        vec = scalar_to_draken_vector(None, OrsoTypes.DOUBLE)
        result = vec.to_arrow().to_pylist()
        assert result == [None]

    def test_null_varchar(self):
        """Test NULL vector with VARCHAR type."""
        vec = scalar_to_draken_vector(None, OrsoTypes.VARCHAR)
        result = vec.to_arrow().to_pylist()
        assert result == [None]

    def test_null_date(self):
        """Test NULL vector with DATE type."""
        vec = scalar_to_draken_vector(None, OrsoTypes.DATE)
        result = vec.to_arrow().to_pylist()
        assert result == [None]

    def test_null_timestamp(self):
        """Test NULL vector with TIMESTAMP type."""
        vec = scalar_to_draken_vector(None, OrsoTypes.TIMESTAMP)
        result = vec.to_arrow().to_pylist()
        assert result == [None]

    def test_null_type_inference(self):
        """Test NULL type inference."""
        vec = scalar_to_draken_vector(None, OrsoTypes.NULL)
        result = vec.to_arrow().to_pylist()
        assert result == [None]


class TestScalarToVectorConstantVectors:
    """Test constant vector creation (length > 1)."""

    def test_constant_boolean_length_5(self):
        """Test creating constant boolean vector of length 5."""
        vec = scalar_to_draken_vector(True, OrsoTypes.BOOLEAN, length=5)
        result = vec.to_arrow().to_pylist()
        assert result == [True, True, True, True, True]

    def test_constant_integer_length_10(self):
        """Test creating constant integer vector of length 10."""
        vec = scalar_to_draken_vector(42, OrsoTypes.INTEGER, length=10)
        result = vec.to_arrow().to_pylist()
        assert result == [42] * 10
        assert len(result) == 10

    def test_constant_varchar_length_3(self):
        """Test creating constant varchar vector of length 3.

        Note: Draken StringVector returns bytes.
        """
        vec = scalar_to_draken_vector("hello", OrsoTypes.VARCHAR, length=3)
        result = vec.to_arrow().to_pylist()
        assert result == [b"hello", b"hello", b"hello"]

    def test_constant_null_length_5(self):
        """Test creating constant null vector of length 5."""
        vec = scalar_to_draken_vector(None, OrsoTypes.INTEGER, length=5)
        result = vec.to_arrow().to_pylist()
        assert result == [None, None, None, None, None]

    def test_constant_vector_length_1(self):
        """Test constant vector with explicit length=1."""
        vec = scalar_to_draken_vector(99, OrsoTypes.INTEGER, length=1)
        result = vec.to_arrow().to_pylist()
        assert result == [99]


class TestScalarToVectorTypeInference:
    """Test automatic type inference (dtype=None)."""

    def test_infer_boolean(self):
        """Test inferring BOOLEAN from True."""
        vec = scalar_to_draken_vector(True)
        result = vec.to_arrow().to_pylist()
        assert result == [True]

    def test_infer_integer(self):
        """Test inferring INTEGER from int."""
        vec = scalar_to_draken_vector(42)
        result = vec.to_arrow().to_pylist()
        assert result == [42]

    def test_infer_double(self):
        """Test inferring DOUBLE from float."""
        vec = scalar_to_draken_vector(3.14)
        result = vec.to_arrow().to_pylist()
        assert len(result) == 1
        assert abs(result[0] - 3.14) < 1e-6

    def test_infer_varchar(self):
        """Test inferring VARCHAR from str.

        Note: Returns bytes due to Draken StringVector behavior.
        """
        vec = scalar_to_draken_vector("hello")
        result = vec.to_arrow().to_pylist()
        assert result == [b"hello"]

    def test_infer_blob(self):
        """Test inferring BLOB from bytes."""
        vec = scalar_to_draken_vector(b"data")
        result = vec.to_arrow().to_pylist()
        assert result == [b"data"]

    def test_infer_date(self):
        """Test inferring DATE from datetime.date."""
        d = datetime.date(2024, 1, 15)
        vec = scalar_to_draken_vector(d)
        result = vec.to_arrow().to_pylist()
        assert result == [d]

    def test_infer_timestamp(self):
        """Test inferring TIMESTAMP from datetime.datetime."""
        dt = datetime.datetime(2024, 1, 15, 10, 30)
        vec = scalar_to_draken_vector(dt)
        result = vec.to_arrow().to_pylist()
        assert result == [dt]

    def test_infer_decimal(self):
        """Test inferring DECIMAL from decimal.Decimal."""
        d = decimal.Decimal("123.45")
        vec = scalar_to_draken_vector(d)
        result = vec.to_arrow().to_pylist()
        assert result == [d]

    def test_infer_null(self):
        """Test inferring NULL from None."""
        vec = scalar_to_draken_vector(None)
        result = vec.to_arrow().to_pylist()
        assert result == [None]


class TestScalarToVectorComplexTypes:
    """Test complex type conversions."""

    def test_array_from_list(self):
        """Test converting list to ARRAY."""
        data = [1, 2, 3]
        vec = scalar_to_draken_vector(data, OrsoTypes.ARRAY)
        result = vec.to_arrow().to_pylist()
        assert result == [data]

    def test_array_from_tuple(self):
        """Test converting tuple to ARRAY (converts to list)."""
        data = (1, 2, 3)
        vec = scalar_to_draken_vector(data, OrsoTypes.ARRAY)
        result = vec.to_arrow().to_pylist()
        # Tuple should be converted to list
        assert result == [[1, 2, 3]]

    def test_array_null(self):
        """Test NULL array."""
        vec = scalar_to_draken_vector(None, OrsoTypes.ARRAY)
        result = vec.to_arrow().to_pylist()
        assert result == [None]

    def test_struct_from_dict(self):
        """Test converting dict to STRUCT."""
        data = {"a": 1, "b": "hello"}
        vec = scalar_to_draken_vector(data, OrsoTypes.STRUCT)
        result = vec.to_arrow().to_pylist()
        assert result == [data]

    def test_vector_from_list(self):
        """Test converting list to VECTOR."""
        data = [1.0, 2.0, 3.0]
        vec = scalar_to_draken_vector(data, OrsoTypes.VECTOR)
        result = vec.to_arrow().to_pylist()
        assert result == [data]

    def test_jsonb_from_dict(self):
        """Test converting dict to JSONB.

        JSONB stores as JSON string.
        """
        data = {"key": "value", "number": 42}
        vec = scalar_to_draken_vector(data, OrsoTypes.JSONB)
        result = vec.to_arrow().to_pylist()
        # JSONB stores as JSON string (bytes in Draken)
        assert len(result) == 1
        assert isinstance(result[0], bytes)

    def test_jsonb_from_list(self):
        """Test converting list to JSONB."""
        data = [1, 2, 3]
        vec = scalar_to_draken_vector(data, OrsoTypes.JSONB)
        result = vec.to_arrow().to_pylist()
        assert len(result) == 1
        assert isinstance(result[0], bytes)

    def test_jsonb_from_string(self):
        """Test JSONB from already-serialized string.

        Returns as bytes due to Draken StringVector behavior.
        """
        data = '{"key": "value"}'
        vec = scalar_to_draken_vector(data, OrsoTypes.JSONB)
        result = vec.to_arrow().to_pylist()
        assert result == [data.encode("utf-8")]


class TestScalarToVectorCoercion:
    """Test type coercion for compatible conversions."""

    def test_coerce_float_to_integer(self):
        """Test coercing float to INTEGER."""
        vec = scalar_to_draken_vector(3.14, OrsoTypes.INTEGER)
        result = vec.to_arrow().to_pylist()
        assert result == [3]  # Truncates, doesn't round

    def test_coerce_int_to_double(self):
        """Test coercing int to DOUBLE."""
        vec = scalar_to_draken_vector(42, OrsoTypes.DOUBLE)
        result = vec.to_arrow().to_pylist()
        assert result == [42.0]

    def test_coerce_bool_to_integer(self):
        """Test coercing bool to INTEGER."""
        vec = scalar_to_draken_vector(True, OrsoTypes.INTEGER)
        result = vec.to_arrow().to_pylist()
        assert result == [1]

    def test_coerce_integer_to_bool(self):
        """Test coercing int to BOOLEAN."""
        vec = scalar_to_draken_vector(1, OrsoTypes.BOOLEAN)
        result = vec.to_arrow().to_pylist()
        assert result == [True]

    def test_coerce_string_to_varchar(self):
        """Test string to VARCHAR (pass-through).

        Returns as bytes due to Draken behavior.
        """
        vec = scalar_to_draken_vector("hello", OrsoTypes.VARCHAR)
        result = vec.to_arrow().to_pylist()
        assert result == [b"hello"]


class TestScalarToVectorStringConversions:
    """Test string conversion edge cases."""

    def test_string_from_int(self):
        """Test converting int to VARCHAR.

        Returns as bytes.
        """
        vec = scalar_to_draken_vector(42, OrsoTypes.VARCHAR)
        result = vec.to_arrow().to_pylist()
        assert result == [b"42"]

    def test_string_from_float(self):
        """Test converting float to VARCHAR.

        Returns as bytes.
        """
        vec = scalar_to_draken_vector(3.14, OrsoTypes.VARCHAR)
        result = vec.to_arrow().to_pylist()
        assert result == [b"3.14"]

    def test_string_from_bool(self):
        """Test converting bool to VARCHAR.

        Returns as bytes.
        """
        vec = scalar_to_draken_vector(True, OrsoTypes.VARCHAR)
        result = vec.to_arrow().to_pylist()
        assert result == [b"True"]

    def test_string_from_date(self):
        """Test converting date to VARCHAR.

        Returns as bytes.
        """
        d = datetime.date(2024, 1, 15)
        vec = scalar_to_draken_vector(d, OrsoTypes.VARCHAR)
        result = vec.to_arrow().to_pylist()
        assert result == [str(d).encode("utf-8")]

    def test_string_from_list(self):
        """Test converting list to VARCHAR.

        Returns as bytes.
        """
        vec = scalar_to_draken_vector([1, 2, 3], OrsoTypes.VARCHAR)
        result = vec.to_arrow().to_pylist()
        assert result == [b"[1, 2, 3]"]


class TestScalarToVectorErrorCases:
    """Test error handling and validation."""

    def test_error_invalid_dtype_type(self):
        """Test error on invalid dtype (not OrsoTypes)."""
        with pytest.raises(ValueError, match="dtype must be OrsoTypes"):
            scalar_to_draken_vector(42, dtype="INTEGER")

    def test_error_invalid_length_zero(self):
        """Test error on length < 1."""
        with pytest.raises(ValueError, match="length must be integer ≥ 1"):
            scalar_to_draken_vector(42, OrsoTypes.INTEGER, length=0)

    def test_error_invalid_length_negative(self):
        """Test error on negative length."""
        with pytest.raises(ValueError, match="length must be integer ≥ 1"):
            scalar_to_draken_vector(42, OrsoTypes.INTEGER, length=-5)

    def test_error_invalid_length_type(self):
        """Test error on non-integer length."""
        with pytest.raises(ValueError, match="length must be integer ≥ 1"):
            scalar_to_draken_vector(42, OrsoTypes.INTEGER, length=3.14)

    def test_error_incompatible_boolean_string(self):
        """Test error on string to BOOLEAN (incompatible)."""
        with pytest.raises(TypeError, match="Cannot convert"):
            scalar_to_draken_vector("hello", OrsoTypes.BOOLEAN)

    def test_error_incompatible_integer_string(self):
        """Test error on non-numeric string to INTEGER."""
        with pytest.raises(TypeError, match="Cannot convert"):
            scalar_to_draken_vector("hello", OrsoTypes.INTEGER)

    def test_error_incompatible_date_string(self):
        """Test error on invalid date string."""
        with pytest.raises(TypeError, match="Cannot convert"):
            scalar_to_draken_vector("not-a-date", OrsoTypes.DATE)

    def test_error_incompatible_blob_int(self):
        """Test error on int to BLOB (incompatible)."""
        with pytest.raises(TypeError, match="Cannot convert"):
            scalar_to_draken_vector(42, OrsoTypes.BLOB)

    def test_error_incompatible_array_string(self):
        """Test error on string to ARRAY."""
        with pytest.raises(TypeError, match="Cannot convert"):
            scalar_to_draken_vector("hello", OrsoTypes.ARRAY)

    def test_error_incompatible_struct_list(self):
        """Test error on list to STRUCT (must be dict)."""
        with pytest.raises(TypeError, match="Cannot convert"):
            scalar_to_draken_vector([1, 2, 3], OrsoTypes.STRUCT)

    def test_error_incompatible_time_string(self):
        """Test error on invalid time."""
        with pytest.raises(TypeError, match="Cannot convert"):
            scalar_to_draken_vector("not-a-time", OrsoTypes.TIME)


class TestScalarToVectorVectorProperties:
    """Test properties of returned vectors."""

    def test_vector_length_single(self):
        """Test vector length for single scalar."""
        vec = scalar_to_draken_vector(42, OrsoTypes.INTEGER)
        assert len(vec.to_arrow()) == 1

    def test_vector_length_constant(self):
        """Test vector length for constant vector."""
        vec = scalar_to_draken_vector(42, OrsoTypes.INTEGER, length=10)
        assert len(vec.to_arrow()) == 10

    def test_vector_type_boolean(self):
        """Test vector has correct Arrow type for BOOLEAN."""
        vec = scalar_to_draken_vector(True, OrsoTypes.BOOLEAN)
        arrow_type = vec.to_arrow().type
        import pyarrow as pa

        assert pa.types.is_boolean(arrow_type)

    def test_vector_type_integer(self):
        """Test vector has correct Arrow type for INTEGER."""
        vec = scalar_to_draken_vector(42, OrsoTypes.INTEGER)
        arrow_type = vec.to_arrow().type
        import pyarrow as pa

        assert pa.types.is_integer(arrow_type)

    def test_vector_type_double(self):
        """Test vector has correct Arrow type for DOUBLE."""
        vec = scalar_to_draken_vector(3.14, OrsoTypes.DOUBLE)
        arrow_type = vec.to_arrow().type
        import pyarrow as pa

        assert pa.types.is_floating(arrow_type)

    def test_vector_type_varchar(self):
        """Test vector has correct Arrow type for VARCHAR."""
        vec = scalar_to_draken_vector("hello", OrsoTypes.VARCHAR)
        arrow_type = vec.to_arrow().type
        import pyarrow as pa

        # Draken uses binary type for strings
        assert pa.types.is_binary(arrow_type) or pa.types.is_string(arrow_type)


class TestScalarToVectorBlobEdgeCases:
    """Test BLOB-specific edge cases."""

    def test_blob_from_string(self):
        """Test converting string to BLOB (encodes as UTF-8)."""
        vec = scalar_to_draken_vector("hello", OrsoTypes.BLOB)
        result = vec.to_arrow().to_pylist()
        assert result == [b"hello"]

    def test_blob_unicode(self):
        """Test BLOB from unicode string."""
        vec = scalar_to_draken_vector("🌍", OrsoTypes.BLOB)
        result = vec.to_arrow().to_pylist()
        assert result == ["🌍".encode("utf-8")]


class TestScalarToVectorDocstringExamples:
    """Test examples from docstring work correctly."""

    def test_docstring_example_integer(self):
        """Test docstring example: integer scalar."""
        vec = scalar_to_draken_vector(42, OrsoTypes.INTEGER)
        assert vec.to_arrow().to_pylist() == [42]

    def test_docstring_example_varchar_constant(self):
        """Test docstring example: constant varchar vector.

        Returns bytes due to Draken behavior.
        """
        vec = scalar_to_draken_vector("hello", OrsoTypes.VARCHAR, length=10)
        assert len(vec.to_arrow()) == 10
        assert all(v == b"hello" for v in vec.to_arrow().to_pylist())

    def test_docstring_example_null(self):
        """Test docstring example: null vector."""
        vec = scalar_to_draken_vector(None, OrsoTypes.INTEGER, length=5)
        assert vec.to_arrow().to_pylist() == [None, None, None, None, None]

    def test_docstring_example_type_inference(self):
        """Test docstring example: type inference."""
        vec = scalar_to_draken_vector(3.14)
        result = vec.to_arrow().to_pylist()
        assert len(result) == 1
        assert isinstance(result[0], float)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
