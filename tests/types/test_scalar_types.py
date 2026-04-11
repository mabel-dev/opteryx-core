"""Unit tests for internal scalar type system."""

import datetime
import decimal

import pytest

from opteryx.types import (
    ScalarType,
    classify_scalar,
    extract_python_scalar,
    is_null_scalar,
    is_numeric_scalar,
    is_scalar,
    is_temporal_scalar,
    unwrap_scalar,
)


class TestClassifyScalar:
    """Test classify_scalar() function."""

    def test_none_type(self):
        """Test None classification."""
        assert classify_scalar(None) == ScalarType.NONE

    def test_boolean_type(self):
        """Test boolean classification."""
        assert classify_scalar(True) == ScalarType.BOOLEAN
        assert classify_scalar(False) == ScalarType.BOOLEAN

    def test_integer_type(self):
        """Test integer classification."""
        assert classify_scalar(0) == ScalarType.INT64
        assert classify_scalar(42) == ScalarType.INT64
        assert classify_scalar(-100) == ScalarType.INT64

    def test_float_type(self):
        """Test float classification."""
        assert classify_scalar(3.14) == ScalarType.FLOAT64
        assert classify_scalar(0.0) == ScalarType.FLOAT64
        assert classify_scalar(-2.5) == ScalarType.FLOAT64

    def test_string_type(self):
        """Test string classification."""
        assert classify_scalar("hello") == ScalarType.STRING
        assert classify_scalar("") == ScalarType.STRING

    def test_bytes_type(self):
        """Test bytes classification."""
        assert classify_scalar(b"hello") == ScalarType.BYTES
        assert classify_scalar(bytearray(b"test")) == ScalarType.BYTES
        assert classify_scalar(memoryview(b"test")) == ScalarType.BYTES

    def test_date_type(self):
        """Test date classification."""
        d = datetime.date(2024, 1, 15)
        assert classify_scalar(d) == ScalarType.DATE

    def test_time_type(self):
        """Test time classification."""
        t = datetime.time(14, 30, 0)
        assert classify_scalar(t) == ScalarType.TIME

    def test_datetime_type(self):
        """Test datetime classification."""
        dt = datetime.datetime(2024, 1, 15, 14, 30, 0)
        assert classify_scalar(dt) == ScalarType.DATETIME

    def test_timedelta_type(self):
        """Test timedelta classification."""
        td = datetime.timedelta(days=5, hours=3)
        assert classify_scalar(td) == ScalarType.TIMEDELTA

    def test_decimal_type(self):
        """Test Decimal classification."""
        dec = decimal.Decimal("123.45")
        assert classify_scalar(dec) == ScalarType.DECIMAL

    def test_numpy_types(self):
        """Test numpy type classification (if numpy available)."""
        pytest.importorskip("numpy")
        import numpy as np

        # NumPy integers
        assert classify_scalar(np.int64(42)) == ScalarType.INT64
        assert classify_scalar(np.int32(10)) == ScalarType.INT64
        assert classify_scalar(np.uint64(50)) == ScalarType.UINT64

        # NumPy floats
        assert classify_scalar(np.float64(3.14)) == ScalarType.FLOAT64
        assert classify_scalar(np.float32(2.5)) == ScalarType.FLOAT64

        # NumPy booleans
        assert classify_scalar(np.bool_(True)) == ScalarType.BOOLEAN

        # NumPy datetime64
        dt64 = np.datetime64("2024-01-15")
        assert classify_scalar(dt64) == ScalarType.DATETIME64

        # NumPy timedelta64
        td64 = np.timedelta64(5, "D")
        assert classify_scalar(td64) == ScalarType.TIMEDELTA64

    def test_pyarrow_types(self):
        """Test pyarrow scalar classification (if pyarrow available)."""
        pytest.importorskip("pyarrow")
        import pyarrow as pa

        scalar = pa.scalar(42, type=pa.int64())
        assert classify_scalar(scalar) == ScalarType.PYARROW_SCALAR


class TestIsScalar:
    """Test is_scalar() function."""

    def test_recognized_scalars(self):
        """Test that recognized scalars return True."""
        assert is_scalar(None) is True
        assert is_scalar(42) is True
        assert is_scalar(3.14) is True
        assert is_scalar("hello") is True
        assert is_scalar(True) is True
        assert is_scalar(datetime.date.today()) is True

    def test_non_scalars(self):
        """Test that non-scalars return False."""
        assert is_scalar([1, 2, 3]) is False
        assert is_scalar({"key": "value"}) is False
        assert is_scalar((1, 2)) is False


class TestIsNumericScalar:
    """Test is_numeric_scalar() function."""

    def test_numeric_scalars(self):
        """Test that numeric scalars return True."""
        assert is_numeric_scalar(42) is True
        assert is_numeric_scalar(3.14) is True
        assert is_numeric_scalar(0) is True

    def test_non_numeric_scalars(self):
        """Test that non-numeric scalars return False."""
        assert is_numeric_scalar("42") is False
        assert is_numeric_scalar(None) is False
        assert is_numeric_scalar(True) is False  # bool is not classified as numeric
        assert is_numeric_scalar(datetime.date.today()) is False

    def test_numpy_numeric(self):
        """Test numpy numeric scalars (if numpy available)."""
        pytest.importorskip("numpy")
        import numpy as np

        assert is_numeric_scalar(np.int64(42)) is True
        assert is_numeric_scalar(np.float32(2.5)) is True
        assert is_numeric_scalar(np.uint64(100)) is True


class TestIsTemporalScalar:
    """Test is_temporal_scalar() function."""

    def test_temporal_scalars(self):
        """Test that temporal scalars return True."""
        assert is_temporal_scalar(datetime.date.today()) is True
        assert is_temporal_scalar(datetime.time(12, 0)) is True
        assert is_temporal_scalar(datetime.datetime.now()) is True
        assert is_temporal_scalar(datetime.timedelta(days=1)) is True

    def test_non_temporal_scalars(self):
        """Test that non-temporal scalars return False."""
        assert is_temporal_scalar(42) is False
        assert is_temporal_scalar("2024-01-15") is False
        assert is_temporal_scalar(None) is False

    def test_numpy_temporal(self):
        """Test numpy temporal scalars (if numpy available)."""
        pytest.importorskip("numpy")
        import numpy as np

        dt64 = np.datetime64("2024-01-15")
        td64 = np.timedelta64(5, "D")
        assert is_temporal_scalar(dt64) is True
        assert is_temporal_scalar(td64) is True


class TestIsNullScalar:
    """Test is_null_scalar() function."""

    def test_null_scalar(self):
        """Test that None returns True."""
        assert is_null_scalar(None) is True

    def test_non_null_scalars(self):
        """Test that non-None scalars return False."""
        assert is_null_scalar(0) is False
        assert is_null_scalar("") is False
        assert is_null_scalar(False) is False
        assert is_null_scalar([]) is False


class TestExtractPythonScalar:
    """Test extract_python_scalar() function."""

    def test_native_python_types_passthrough(self):
        """Test that native Python types pass through unchanged."""
        assert extract_python_scalar(42) == 42
        assert extract_python_scalar(3.14) == 3.14
        assert extract_python_scalar("hello") == "hello"
        assert extract_python_scalar(None) is None

    def test_numpy_generic_extraction(self):
        """Test numpy.generic extraction (if numpy available)."""
        pytest.importorskip("numpy")
        import numpy as np

        # numpy scalars have .item() method
        np_int = np.int64(42)
        extracted = extract_python_scalar(np_int)
        assert extracted == 42
        assert isinstance(extracted, int)

        np_float = np.float64(3.14)
        extracted = extract_python_scalar(np_float)
        assert extracted == 3.14
        assert isinstance(extracted, float)

    def test_pyarrow_scalar_extraction(self):
        """Test pyarrow scalar extraction (if pyarrow available)."""
        pytest.importorskip("pyarrow")
        import pyarrow as pa

        pa_scalar = pa.scalar(42, type=pa.int64())
        extracted = extract_python_scalar(pa_scalar)
        assert extracted == 42
        assert isinstance(extracted, int)


class TestUnwrapScalar:
    """Test unwrap_scalar() function."""

    def test_native_scalars_passthrough(self):
        """Test that native Python scalars pass through."""
        assert unwrap_scalar(42) == 42
        assert unwrap_scalar("hello") == "hello"

    def test_numpy_0d_array(self):
        """Test unwrapping 0-d numpy arrays (if numpy available)."""
        pytest.importorskip("numpy")
        import numpy as np

        arr_0d = np.array(42)
        assert arr_0d.ndim == 0
        unwrapped = unwrap_scalar(arr_0d)
        assert unwrapped == 42

    def test_numpy_single_element_array(self):
        """Test unwrapping single-element numpy arrays (if numpy available)."""
        pytest.importorskip("numpy")
        import numpy as np

        arr_1d = np.array([42])
        assert arr_1d.size == 1
        unwrapped = unwrap_scalar(arr_1d)
        assert unwrapped == 42

    def test_numpy_tolist_conversion(self):
        """Test tolist() based unwrapping (if numpy available)."""
        pytest.importorskip("numpy")
        import numpy as np

        arr = np.array([3.14, 2.71])
        unwrapped = unwrap_scalar(arr)
        assert unwrapped == [3.14, 2.71]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
