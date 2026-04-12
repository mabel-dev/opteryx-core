"""Unit tests for vector type discriminator system.

Tests the centralized type discrimination utilities in opteryx.utils.vector_types,
ensuring that get_vector_type(), is_scalar(), and is_draken_vector() work correctly
with all Draken vector types and Python scalar types.
"""

import datetime
import decimal

import pyarrow as pa
import pytest

from opteryx.utils.vector_types import VectorType, get_vector_type, is_draken_vector, is_scalar


class TestIsScalar:
    """Tests for is_scalar() function."""

    def test_none_is_scalar(self):
        """None should be recognized as a scalar."""
        assert is_scalar(None) is True

    def test_bool_is_scalar(self):
        """Boolean values should be recognized as scalars."""
        assert is_scalar(True) is True
        assert is_scalar(False) is True

    def test_int_is_scalar(self):
        """Integer values should be recognized as scalars."""
        assert is_scalar(0) is True
        assert is_scalar(42) is True
        assert is_scalar(-1) is True
        assert is_scalar(2**63) is True

    def test_float_is_scalar(self):
        """Float values should be recognized as scalars."""
        assert is_scalar(0.0) is True
        assert is_scalar(3.14) is True
        assert is_scalar(-1.5) is True

    def test_str_is_scalar(self):
        """String values should be recognized as scalars."""
        assert is_scalar("") is True
        assert is_scalar("hello") is True
        assert is_scalar("👋") is True

    def test_bytes_is_scalar(self):
        """Bytes should be recognized as scalars."""
        assert is_scalar(b"") is True
        assert is_scalar(b"hello") is True

    def test_bytearray_is_scalar(self):
        """Bytearray should be recognized as scalars."""
        assert is_scalar(bytearray()) is True
        assert is_scalar(bytearray(b"hello")) is True

    def test_date_is_scalar(self):
        """datetime.date should be recognized as a scalar."""
        assert is_scalar(datetime.date(2024, 1, 1)) is True

    def test_time_is_scalar(self):
        """datetime.time should be recognized as a scalar."""
        assert is_scalar(datetime.time(12, 30, 0)) is True

    def test_datetime_is_scalar(self):
        """datetime.datetime should be recognized as a scalar."""
        assert is_scalar(datetime.datetime(2024, 1, 1, 12, 30)) is True

    def test_timedelta_is_scalar(self):
        """datetime.timedelta should be recognized as a scalar."""
        assert is_scalar(datetime.timedelta(days=1)) is True

    def test_decimal_is_scalar(self):
        """decimal.Decimal should be recognized as a scalar."""
        assert is_scalar(decimal.Decimal("3.14")) is True

    def test_list_not_scalar(self):
        """Lists should NOT be recognized as scalars."""
        assert is_scalar([]) is False
        assert is_scalar([1, 2, 3]) is False

    def test_dict_not_scalar(self):
        """Dicts should NOT be recognized as scalars."""
        assert is_scalar({}) is False
        assert is_scalar({"a": 1}) is False

    def test_arrow_array_not_scalar(self):
        """PyArrow arrays should NOT be recognized as scalars."""
        arr = pa.array([1, 2, 3], type=pa.int64())
        assert is_scalar(arr) is False

    def test_custom_object_not_scalar(self):
        """Custom objects should NOT be recognized as scalars."""

        class CustomClass:
            pass

        assert is_scalar(CustomClass()) is False


class TestGetVectorType:
    """Tests for get_vector_type() function."""

    def test_int64_vector(self):
        """Int64Vector should be discriminated correctly."""
        from opteryx.compiled.draken.vectors.int64_vector import Int64Vector

        vec = Int64Vector.from_arrow(pa.array([1, 2, 3], type=pa.int64()))
        assert get_vector_type(vec) == VectorType.INT64

    def test_float64_vector(self):
        """Float64Vector should be discriminated correctly."""
        from opteryx.compiled.draken.vectors.float64_vector import Float64Vector

        vec = Float64Vector.from_arrow(pa.array([1.0, 2.0, 3.0], type=pa.float64()))
        assert get_vector_type(vec) == VectorType.FLOAT64

    def test_bool_vector(self):
        """BoolVector should be discriminated correctly."""
        from opteryx.compiled.draken.vectors.bool_vector import BoolVector

        vec = BoolVector.from_arrow(pa.array([True, False, True], type=pa.bool_()))
        assert get_vector_type(vec) == VectorType.BOOL

    def test_string_vector(self):
        """StringVector should be discriminated correctly."""
        from opteryx.compiled.draken.vectors.string_vector import StringVector

        arr = pa.array(["a", "b", "c"], type=pa.utf8())
        vec = StringVector.from_arrow(arr)
        assert get_vector_type(vec) == VectorType.STRING

    def test_timestamp_vector(self):
        """TimestampVector should be discriminated correctly."""
        from opteryx.compiled.draken.vectors.timestamp_vector import TimestampVector

        arr = pa.array(
            [datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 2)], type=pa.timestamp("us")
        )
        vec = TimestampVector.from_arrow(arr)
        assert get_vector_type(vec) == VectorType.TIMESTAMP

    def test_date32_vector(self):
        """Date32Vector should be discriminated correctly."""
        from opteryx.compiled.draken.vectors.date32_vector import Date32Vector

        arr = pa.array([datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)], type=pa.date32())
        vec = Date32Vector.from_arrow(arr)
        assert get_vector_type(vec) == VectorType.DATE32

    def test_unknown_type_returns_unknown(self):
        """Unknown types should return VectorType.UNKNOWN."""
        assert get_vector_type("not a vector") == VectorType.UNKNOWN
        assert get_vector_type(42) == VectorType.UNKNOWN
        assert get_vector_type(object()) == VectorType.UNKNOWN


class TestIsDrakenVector:
    """Tests for is_draken_vector() function."""

    def test_int64_vector_is_draken(self):
        """Int64Vector should be recognized as a Draken vector."""
        from opteryx.compiled.draken.vectors.int64_vector import Int64Vector

        vec = Int64Vector.from_arrow(pa.array([1, 2, 3], type=pa.int64()))
        assert is_draken_vector(vec) is True

    def test_float64_vector_is_draken(self):
        """Float64Vector should be recognized as a Draken vector."""
        from opteryx.compiled.draken.vectors.float64_vector import Float64Vector

        vec = Float64Vector.from_arrow(pa.array([1.0, 2.0, 3.0], type=pa.float64()))
        assert is_draken_vector(vec) is True

    def test_bool_vector_is_draken(self):
        """BoolVector should be recognized as a Draken vector."""
        from opteryx.compiled.draken.vectors.bool_vector import BoolVector

        vec = BoolVector.from_arrow(pa.array([True, False, True], type=pa.bool_()))
        assert is_draken_vector(vec) is True

    def test_string_vector_is_draken(self):
        """StringVector should be recognized as a Draken vector."""
        from opteryx.compiled.draken.vectors.string_vector import StringVector

        arr = pa.array(["a", "b", "c"], type=pa.utf8())
        vec = StringVector.from_arrow(arr)
        assert is_draken_vector(vec) is True

    def test_scalar_not_draken_vector(self):
        """Scalars should NOT be recognized as Draken vectors."""
        assert is_draken_vector(42) is False
        assert is_draken_vector("hello") is False
        assert is_draken_vector(None) is False

    def test_arrow_array_not_draken_vector(self):
        """Raw PyArrow arrays should NOT be recognized as Draken vectors."""
        arr = pa.array([1, 2, 3], type=pa.int64())
        assert is_draken_vector(arr) is False


class TestVectorTypeEnum:
    """Tests for VectorType enum completeness."""

    def test_all_vector_types_defined(self):
        """Ensure all expected vector types are defined in enum."""
        expected_types = {
            "STRING",
            "INT64",
            "INTEGER",
            "FLOAT64",
            "BOOL",
            "TIMESTAMP",
            "DATE32",
            "INTERVAL",
            "ARRAY",
            "VECTOR",
            "CONSTANT_ENCODED",
            "DICTIONARY_ENCODED",
            "UNKNOWN",
        }
        actual_types = {member.name for member in VectorType}
        assert expected_types == actual_types


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
