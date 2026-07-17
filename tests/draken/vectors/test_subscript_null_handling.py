"""Test for subscript access returning None for null values (GitHub Issue)."""
import datetime

import draken.draken_native as dn


class TestSubscriptNullHandling:
    """Test that subscript access (__getitem__) returns None for null values."""

    def test_string_vector_subscript_null(self):
        """String vectors should return None for null values, not empty bytes."""
        vec = dn.vector_from_string_sequence([b'abc123', b'xyz789', None])

        assert vec[0] == 'abc123'
        assert vec[1] == 'xyz789'
        assert vec[2] is None

    def test_int64_vector_subscript_null(self):
        """Int64 vectors should return None for null values, not raise ValueError."""
        vec = dn.vector_from_sequence([1, 2, None])

        assert vec[0] == 1
        assert vec[1] == 2
        assert vec[2] is None

    def test_float64_vector_subscript_null(self):
        """Float64 vectors should return None for null values, not raise ValueError."""
        vec = dn.vector_float64_from_sequence([1.5, 2.5, None])

        assert vec[0] == 1.5
        assert vec[1] == 2.5
        assert vec[2] is None

    def test_bool_vector_subscript_null(self):
        """Bool vectors should return None for null values, not raise ValueError."""
        vec = dn.vector_from_bool_sequence([True, False, None])

        assert vec[0] is True
        assert vec[1] is False
        assert vec[2] is None

    def test_date32_vector_subscript_null(self):
        """Date32 vectors should return None for null values."""
        vec = dn.vector_date32_from_sequence(
            [datetime.date(1970, 1, 1), datetime.date(1970, 1, 2), None]
        )

        assert vec[0] == datetime.date(1970, 1, 1)
        assert vec[1] == datetime.date(1970, 1, 2)
        assert vec[2] is None

    def test_timestamp_vector_subscript_null(self):
        """Timestamp vectors should return None for null values."""
        vec = dn.vector_timestamp_from_sequence(
            [datetime.datetime(1970, 1, 1), datetime.datetime(1970, 1, 1, 0, 0, 1), None]
        )

        assert vec[0] is not None
        assert vec[1] is not None
        assert vec[2] is None

    def test_time_vector_subscript_null(self):
        """Time64 vectors should return None for null values."""
        vec = dn.vector_time64_from_sequence(
            [datetime.time(0, 0, 0), datetime.time(0, 0, 0, 1000), None]
        )

        assert vec[0] == datetime.time(0, 0, 0)
        assert vec[1] == datetime.time(0, 0, 0, 1000)
        assert vec[2] is None

    def test_multiple_nulls(self):
        """Test vector with multiple nulls."""
        vec = dn.vector_from_string_sequence([None, b'hello', None, b'world', None])

        assert vec[0] is None
        assert vec[1] == 'hello'
        assert vec[2] is None
        assert vec[3] == 'world'
        assert vec[4] is None

    def test_all_nulls(self):
        """Test vector with all nulls."""
        vec = dn.vector_from_sequence([None, None, None])

        assert vec[0] is None
        assert vec[1] is None
        assert vec[2] is None

    def test_to_pylist_consistency_with_subscript(self):
        """Ensure to_pylist() and subscript access return consistent null representation."""
        vec = dn.vector_from_string_sequence([b'hello', None, b'world'])

        # Subscript access should return None for nulls
        assert vec[0] == 'hello'
        assert vec[1] is None
        assert vec[2] == 'world'

        # to_pylist() should also return None for nulls
        pylist = vec.to_pylist()
        assert pylist[0] == 'hello'
        assert pylist[1] is None
        assert pylist[2] == 'world'
