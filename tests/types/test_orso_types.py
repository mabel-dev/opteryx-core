"""Unit tests for inlined OrsoTypes type system."""

import datetime
import decimal

import pytest

from opteryx.types._orso_types import (
    ORSO_TO_PYTHON_MAP,
    PYTHON_TO_ORSO_MAP,
    OrsoTypes,
    find_compatible_type,
)


class TestOrsoTypesConstants:
    """Test OrsoTypes enum constants are defined."""

    def test_core_scalar_types_exist(self):
        """Test core scalar types are defined."""
        assert hasattr(OrsoTypes, "NULL")
        assert hasattr(OrsoTypes, "BOOLEAN")
        assert hasattr(OrsoTypes, "INTEGER")
        assert hasattr(OrsoTypes, "DOUBLE")
        assert hasattr(OrsoTypes, "VARCHAR")
        assert hasattr(OrsoTypes, "BLOB")

    def test_temporal_types_exist(self):
        """Test temporal types are defined."""
        assert hasattr(OrsoTypes, "DATE")
        assert hasattr(OrsoTypes, "TIME")
        assert hasattr(OrsoTypes, "TIMESTAMP")
        assert hasattr(OrsoTypes, "INTERVAL")

    def test_complex_types_exist(self):
        """Test complex types are defined."""
        assert hasattr(OrsoTypes, "DECIMAL")
        assert hasattr(OrsoTypes, "ARRAY")
        assert hasattr(OrsoTypes, "STRUCT")
        assert hasattr(OrsoTypes, "VECTOR")
        assert hasattr(OrsoTypes, "JSONB")


class TestPythonType:
    """Test python_type property."""

    def test_scalar_python_types(self):
        """Test python_type for scalar types."""
        assert OrsoTypes.NULL.python_type == type(None)
        assert OrsoTypes.BOOLEAN.python_type == bool
        assert OrsoTypes.INTEGER.python_type == int
        assert OrsoTypes.DOUBLE.python_type == float
        assert OrsoTypes.VARCHAR.python_type == str
        assert OrsoTypes.BLOB.python_type == bytes

    def test_temporal_python_types(self):
        """Test python_type for temporal types."""
        assert OrsoTypes.DATE.python_type == datetime.date
        assert OrsoTypes.TIME.python_type == datetime.time
        assert OrsoTypes.TIMESTAMP.python_type == datetime.datetime
        assert OrsoTypes.INTERVAL.python_type == datetime.timedelta

    def test_complex_python_types(self):
        """Test python_type for complex types."""
        assert OrsoTypes.DECIMAL.python_type == decimal.Decimal
        assert OrsoTypes.ARRAY.python_type == list
        assert OrsoTypes.STRUCT.python_type == dict
        assert OrsoTypes.VECTOR.python_type == list
        assert OrsoTypes.JSONB.python_type == dict


class TestParse:
    """Test parse() method."""

    def test_parse_boolean(self):
        """Test parsing to boolean."""
        assert OrsoTypes.BOOLEAN.parse(True) is True
        assert OrsoTypes.BOOLEAN.parse(False) is False
        assert OrsoTypes.BOOLEAN.parse("true") is True
        assert OrsoTypes.BOOLEAN.parse("false") is False
        assert OrsoTypes.BOOLEAN.parse("1") is True
        assert OrsoTypes.BOOLEAN.parse("0") is False

    def test_parse_integer(self):
        """Test parsing to integer."""
        assert OrsoTypes.INTEGER.parse(42) == 42
        assert OrsoTypes.INTEGER.parse("42") == 42
        assert OrsoTypes.INTEGER.parse(3.14) == 3
        assert OrsoTypes.INTEGER.parse("-100") == -100

    def test_parse_double(self):
        """Test parsing to double."""
        assert OrsoTypes.DOUBLE.parse(3.14) == 3.14
        assert OrsoTypes.DOUBLE.parse("3.14") == 3.14
        assert OrsoTypes.DOUBLE.parse(42) == 42.0
        assert OrsoTypes.DOUBLE.parse("-2.5") == -2.5

    def test_parse_decimal(self):
        """Test parsing to decimal."""
        result = OrsoTypes.DECIMAL.parse("123.45")
        assert result == decimal.Decimal("123.45")
        assert isinstance(result, decimal.Decimal)

    def test_parse_varchar(self):
        """Test parsing to varchar."""
        assert OrsoTypes.VARCHAR.parse("hello") == "hello"
        assert OrsoTypes.VARCHAR.parse(42) == "42"
        assert OrsoTypes.VARCHAR.parse(b"hello") == "hello"

    def test_parse_blob(self):
        """Test parsing to blob."""
        assert OrsoTypes.BLOB.parse(b"hello") == b"hello"
        assert OrsoTypes.BLOB.parse("hello") == b"hello"
        assert isinstance(OrsoTypes.BLOB.parse("test"), bytes)

    def test_parse_date(self):
        """Test parsing to date."""
        d = datetime.date(2024, 1, 15)
        assert OrsoTypes.DATE.parse(d) == d
        assert OrsoTypes.DATE.parse("2024-01-15") == d
        assert OrsoTypes.DATE.parse(datetime.datetime(2024, 1, 15, 10, 30)) == d

    def test_parse_time(self):
        """Test parsing to time."""
        t = datetime.time(14, 30, 0)
        assert OrsoTypes.TIME.parse(t) == t
        assert OrsoTypes.TIME.parse("14:30:00") == t
        assert OrsoTypes.TIME.parse("14:30") == datetime.time(14, 30, 0)

    def test_parse_timestamp(self):
        """Test parsing to timestamp."""
        dt = datetime.datetime(2024, 1, 15, 14, 30, 0)
        assert OrsoTypes.TIMESTAMP.parse(dt) == dt
        assert OrsoTypes.TIMESTAMP.parse("2024-01-15 14:30:00") == dt
        assert OrsoTypes.TIMESTAMP.parse("2024-01-15T14:30:00") == dt

    def test_parse_interval(self):
        """Test parsing to interval."""
        td = datetime.timedelta(seconds=60)
        assert OrsoTypes.INTERVAL.parse(td) == td
        assert OrsoTypes.INTERVAL.parse(60) == td

    def test_parse_none(self):
        """Test parsing None."""
        for otype in OrsoTypes:
            result = otype.parse(None)
            assert result is None


class TestIsNumeric:
    """Test is_numeric() method."""

    def test_numeric_types(self):
        """Test numeric type identification."""
        assert OrsoTypes.INTEGER.is_numeric() is True
        assert OrsoTypes.DOUBLE.is_numeric() is True
        assert OrsoTypes.DECIMAL.is_numeric() is True

    def test_non_numeric_types(self):
        """Test non-numeric types."""
        assert OrsoTypes.VARCHAR.is_numeric() is False
        assert OrsoTypes.BOOLEAN.is_numeric() is False
        assert OrsoTypes.DATE.is_numeric() is False
        assert OrsoTypes.BLOB.is_numeric() is False


class TestIsTemporal:
    """Test is_temporal() method."""

    def test_temporal_types(self):
        """Test temporal type identification."""
        assert OrsoTypes.DATE.is_temporal() is True
        assert OrsoTypes.TIME.is_temporal() is True
        assert OrsoTypes.TIMESTAMP.is_temporal() is True
        assert OrsoTypes.INTERVAL.is_temporal() is True

    def test_non_temporal_types(self):
        """Test non-temporal types."""
        assert OrsoTypes.INTEGER.is_temporal() is False
        assert OrsoTypes.VARCHAR.is_temporal() is False
        assert OrsoTypes.BLOB.is_temporal() is False


class TestIsComplex:
    """Test is_complex() method."""

    def test_complex_types(self):
        """Test complex type identification."""
        assert OrsoTypes.ARRAY.is_complex() is True
        assert OrsoTypes.STRUCT.is_complex() is True
        assert OrsoTypes.VECTOR.is_complex() is True
        assert OrsoTypes.JSONB.is_complex() is True

    def test_non_complex_types(self):
        """Test non-complex types."""
        assert OrsoTypes.INTEGER.is_complex() is False
        assert OrsoTypes.VARCHAR.is_complex() is False
        assert OrsoTypes.DATE.is_complex() is False


class TestIsLargeObject:
    """Test is_large_object() method."""

    def test_large_object_types(self):
        """Test large object type identification."""
        assert OrsoTypes.BLOB.is_large_object() is True
        assert OrsoTypes.JSONB.is_large_object() is True
        assert OrsoTypes.ARRAY.is_large_object() is True
        assert OrsoTypes.STRUCT.is_large_object() is True
        assert OrsoTypes.VECTOR.is_large_object() is True

    def test_non_large_object_types(self):
        """Test non-large object types."""
        assert OrsoTypes.INTEGER.is_large_object() is False
        assert OrsoTypes.VARCHAR.is_large_object() is False
        assert OrsoTypes.DATE.is_large_object() is False


class TestFromName:
    """Test from_name() classmethod."""

    def test_from_name_valid(self):
        """Test from_name with valid type names."""
        result = OrsoTypes.from_name("INTEGER")
        assert result == (OrsoTypes.INTEGER, None, None, None, None)

        result = OrsoTypes.from_name("VARCHAR")
        assert result == (OrsoTypes.VARCHAR, None, None, None, None)

        result = OrsoTypes.from_name("TIMESTAMP")
        assert result == (OrsoTypes.TIMESTAMP, None, None, None, None)

    def test_from_name_invalid(self):
        """Test from_name with invalid type names."""
        with pytest.raises(ValueError):
            OrsoTypes.from_name("INVALID_TYPE")

        with pytest.raises(ValueError):
            OrsoTypes.from_name("int")  # lowercase


class TestPythonToOrsoMap:
    """Test PYTHON_TO_ORSO_MAP bidirectional mapping."""

    def test_basic_python_types_mapped(self):
        """Test basic Python types are mapped."""
        assert PYTHON_TO_ORSO_MAP[bool] == OrsoTypes.BOOLEAN
        assert PYTHON_TO_ORSO_MAP[int] == OrsoTypes.INTEGER
        assert PYTHON_TO_ORSO_MAP[float] == OrsoTypes.DOUBLE
        assert PYTHON_TO_ORSO_MAP[str] == OrsoTypes.VARCHAR
        assert PYTHON_TO_ORSO_MAP[bytes] == OrsoTypes.BLOB

    def test_temporal_python_types_mapped(self):
        """Test temporal Python types are mapped."""
        assert PYTHON_TO_ORSO_MAP[datetime.date] == OrsoTypes.DATE
        assert PYTHON_TO_ORSO_MAP[datetime.time] == OrsoTypes.TIME
        assert PYTHON_TO_ORSO_MAP[datetime.datetime] == OrsoTypes.TIMESTAMP
        assert PYTHON_TO_ORSO_MAP[datetime.timedelta] == OrsoTypes.INTERVAL

    def test_none_mapped(self):
        """Test None is mapped to NULL."""
        assert PYTHON_TO_ORSO_MAP[type(None)] == OrsoTypes.NULL


class TestOrsoToPythonMap:
    """Test ORSO_TO_PYTHON_MAP reverse mapping."""

    def test_basic_orso_types_mapped(self):
        """Test basic OrsoTypes are reverse mapped."""
        assert ORSO_TO_PYTHON_MAP[OrsoTypes.BOOLEAN] == bool
        assert ORSO_TO_PYTHON_MAP[OrsoTypes.INTEGER] == int
        assert ORSO_TO_PYTHON_MAP[OrsoTypes.DOUBLE] == float
        assert ORSO_TO_PYTHON_MAP[OrsoTypes.VARCHAR] == str
        assert ORSO_TO_PYTHON_MAP[OrsoTypes.BLOB] == bytes
        assert ORSO_TO_PYTHON_MAP[OrsoTypes.DATE] == datetime.date
        assert ORSO_TO_PYTHON_MAP[OrsoTypes.TIMESTAMP] == datetime.datetime


class TestFindCompatibleType:
    """Test find_compatible_type() function."""

    def test_empty_list(self):
        """Test with empty list."""
        assert find_compatible_type([]) == OrsoTypes.NULL

    def test_single_type(self):
        """Test with single type."""
        assert find_compatible_type([OrsoTypes.INTEGER]) == OrsoTypes.INTEGER
        assert find_compatible_type([OrsoTypes.VARCHAR]) == OrsoTypes.VARCHAR

    def test_same_type(self):
        """Test with all same types."""
        assert find_compatible_type([OrsoTypes.INTEGER, OrsoTypes.INTEGER]) == OrsoTypes.INTEGER

    def test_null_promotion(self):
        """Test NULL promotes to other types."""
        assert find_compatible_type([OrsoTypes.NULL, OrsoTypes.INTEGER]) == OrsoTypes.INTEGER
        assert find_compatible_type([OrsoTypes.NULL, OrsoTypes.VARCHAR]) == OrsoTypes.VARCHAR

    def test_numeric_promotion(self):
        """Test numeric type promotion."""
        # BOOLEAN < INTEGER < DOUBLE < DECIMAL
        assert find_compatible_type([OrsoTypes.BOOLEAN, OrsoTypes.INTEGER]) == OrsoTypes.INTEGER
        assert find_compatible_type([OrsoTypes.INTEGER, OrsoTypes.DOUBLE]) == OrsoTypes.DOUBLE
        assert find_compatible_type([OrsoTypes.DOUBLE, OrsoTypes.DECIMAL]) == OrsoTypes.DECIMAL
        assert (
            find_compatible_type([OrsoTypes.BOOLEAN, OrsoTypes.INTEGER, OrsoTypes.DOUBLE])
            == OrsoTypes.DOUBLE
        )

    def test_temporal_mixed(self):
        """Test mixed temporal types fall back to VARCHAR."""
        result = find_compatible_type([OrsoTypes.DATE, OrsoTypes.TIMESTAMP])
        assert result == OrsoTypes.VARCHAR

    def test_complex_mixed(self):
        """Test mixed complex types fall back to JSONB."""
        result = find_compatible_type([OrsoTypes.ARRAY, OrsoTypes.STRUCT])
        assert result == OrsoTypes.JSONB

    def test_incompatible_fallback(self):
        """Test incompatible types fall back to VARCHAR."""
        result = find_compatible_type([OrsoTypes.VARCHAR, OrsoTypes.INTEGER])
        assert result == OrsoTypes.VARCHAR


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
