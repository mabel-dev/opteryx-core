"""Unit tests for vector type discriminator system.

Tests the centralized type discrimination utilities in opteryx.utils.vector_types,
ensuring that get_vector_type(), is_scalar(), and is_draken_vector() work correctly
with all Draken vector types and Python scalar types.

Vectors are built natively (typed `draken.draken_native` sequence constructors
wrapped in the Cython shim `Vector`). The per-type vector classes the old
fixtures used — Integer64Vector, Float64Vector, StringVector, TimestampVector,
Date32Vector — no longer exist: draken has a single unified `Vector` carrying a
`DrakenType` tag (CLAUDE.md §11), plus `BoolVector` for boolean results.
Discrimination is therefore driven by the physical type tag, and the tests
below pin the full tag → VectorType mapping.
"""

import array
import datetime
import decimal

import pytest

import draken.draken_native as dn
from draken.vectors.bool_vector import BoolVector
from draken.vectors.vector import Vector
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

    def test_typed_buffer_not_scalar(self):
        """A raw typed buffer is a container, not a scalar."""
        assert is_scalar(array.array("q", [1, 2, 3])) is False

    def test_draken_vector_not_scalar(self):
        """A Draken vector is not a scalar."""
        assert is_scalar(Vector(dn.vector_from_sequence([1, 2, 3]))) is False

    def test_custom_object_not_scalar(self):
        """Custom objects should NOT be recognized as scalars."""

        class CustomClass:
            pass

        assert is_scalar(CustomClass()) is False


class TestGetVectorType:
    """Tests for get_vector_type() function.

    Discrimination is by the vector's DrakenType tag, so each case names the
    physical type it is pinning — several distinct tags deliberately collapse
    onto one VectorType.
    """

    def test_int64_vector(self):
        """INT64 discriminates as INT64."""
        vec = Vector(dn.vector_from_sequence([1, 2, 3]))
        assert vec.type == dn.DrakenType.INT64
        assert get_vector_type(vec) == VectorType.INT64

    def test_uint64_vector_collapses_to_int64(self):
        """UINT64 shares the 64-bit-wide dispatch slot with INT64 (E33)."""
        vec = Vector(dn.vector_uint64_from_sequence([1, 2, 3]))
        assert vec.type == dn.DrakenType.UINT64
        assert get_vector_type(vec) == VectorType.INT64

    def test_narrow_int_vectors_collapse_to_integer(self):
        """INT8/16/32 and their unsigned counterparts collapse to INTEGER."""
        narrow = [
            dn.vector_int8_from_sequence([1, 2, 3]),
            dn.vector_int16_from_sequence([1, 2, 3]),
            dn.vector_int32_from_sequence([1, 2, 3]),
            dn.vector_uint8_from_sequence([1, 2, 3]),
            dn.vector_uint16_from_sequence([1, 2, 3]),
            dn.vector_uint32_from_sequence([1, 2, 3]),
        ]
        for nb in narrow:
            assert get_vector_type(Vector(nb)) == VectorType.INTEGER

    def test_float64_vector(self):
        """FLOAT64 discriminates as FLOAT64."""
        vec = Vector(dn.vector_float64_from_sequence([1.0, 2.0, 3.0]))
        assert vec.type == dn.DrakenType.FLOAT64
        assert get_vector_type(vec) == VectorType.FLOAT64

    def test_float32_vector_collapses_to_float64(self):
        """FLOAT32 shares the float dispatch slot with FLOAT64."""
        vec = Vector(dn.vector_float32_from_sequence([1.0, 2.0, 3.0]))
        assert vec.type == dn.DrakenType.FLOAT32
        assert get_vector_type(vec) == VectorType.FLOAT64

    def test_bool_vector(self):
        """BOOL discriminates as BOOL."""
        vec = Vector(dn.vector_from_bool_sequence([True, False, True]))
        assert vec.type == dn.DrakenType.BOOL
        assert get_vector_type(vec) == VectorType.BOOL

    def test_bool_vector_class(self):
        """The dedicated BoolVector class (comparison results) also discriminates
        as BOOL — it is a different Python class from the unified Vector."""
        vec = BoolVector.from_constant(True, 3)
        assert type(vec).__name__ == "BoolVector"
        assert get_vector_type(vec) == VectorType.BOOL

    def test_string_vector(self):
        """VARCHAR discriminates as STRING."""
        vec = Vector(dn.vector_from_string_sequence([b"a", b"b", b"c"]))
        assert vec.type == dn.DrakenType.VARCHAR
        assert get_vector_type(vec) == VectorType.STRING

    def test_nvarchar_and_varbinary_collapse_to_string(self):
        """The rest of the string family shares the STRING dispatch slot."""
        assert (
            get_vector_type(Vector(dn.vector_from_nvarchar_sequence([b"a"]))) == VectorType.STRING
        )
        assert get_vector_type(Vector(dn.vector_from_bytes_sequence([b"a"]))) == VectorType.STRING

    def test_timestamp_vector(self):
        """TIMESTAMP64 discriminates as TIMESTAMP."""
        vec = Vector(
            dn.vector_timestamp_from_sequence(
                [datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 2)]
            )
        )
        assert vec.type == dn.DrakenType.TIMESTAMP64
        assert get_vector_type(vec) == VectorType.TIMESTAMP

    def test_date32_vector(self):
        """DATE32 discriminates as DATE32."""
        vec = Vector(
            dn.vector_date32_from_sequence([datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)])
        )
        assert vec.type == dn.DrakenType.DATE32
        assert get_vector_type(vec) == VectorType.DATE32

    def test_interval_vector(self):
        """INTERVAL discriminates as INTERVAL. Elements are (months, us) tuples."""
        vec = Vector(dn.vector_interval_from_sequence([(0, 86_400_000_000), (1, 0)]))
        assert vec.type == dn.DrakenType.INTERVAL
        assert get_vector_type(vec) == VectorType.INTERVAL

    def test_array_vector(self):
        """ARRAY discriminates as ARRAY."""
        vec = Vector(dn.vector_array_from_sequence([[1, 2], [3, 4]]))
        assert vec.type == dn.DrakenType.ARRAY
        assert get_vector_type(vec) == VectorType.ARRAY

    def test_decimal_vector(self):
        """DECIMAL discriminates as DECIMAL."""
        vec = Vector(dn.vector_decimal_from_sequence([decimal.Decimal("1.23")], 5, 2))
        assert vec.type == dn.DrakenType.DECIMAL
        assert get_vector_type(vec) == VectorType.DECIMAL

    def test_decimal128_vector_collapses_to_decimal(self):
        """DECIMAL128 (int128 tier) also dispatches as DECIMAL — the scale-aware
        kernels intercept the physical tier at the native boundary."""
        vec = Vector(dn.vector_decimal128_from_sequence([decimal.Decimal("1.23")], 20, 2))
        assert vec.type == dn.DrakenType.DECIMAL128
        assert get_vector_type(vec) == VectorType.DECIMAL

    def test_fp16_embedding_vector(self):
        """VECTOR_FP16 (embedding columns) discriminates as VECTOR.

        VECTOR_FP16 is the only embedding-vector tag draken carries — there is
        no bare DrakenType.VECTOR — so this single mapping is what makes
        VectorType.VECTOR reachable at all.
        """
        vec = Vector(dn.vector_fp16_from_sequence([[1.0, 2.0], [3.0, 4.0]], 2))
        assert vec.type == dn.DrakenType.VECTOR_FP16
        assert get_vector_type(vec) == VectorType.VECTOR

    def test_unknown_type_returns_unknown(self):
        """Unknown types should return VectorType.UNKNOWN."""
        assert get_vector_type("not a vector") == VectorType.UNKNOWN
        assert get_vector_type(42) == VectorType.UNKNOWN
        assert get_vector_type(object()) == VectorType.UNKNOWN
        assert get_vector_type([1, 2, 3]) == VectorType.UNKNOWN
        # A raw typed buffer is not a Draken vector, however vector-shaped it looks.
        assert get_vector_type(array.array("q", [1, 2, 3])) == VectorType.UNKNOWN


class TestIsDrakenVector:
    """Tests for is_draken_vector() function."""

    def test_int64_vector_is_draken(self):
        """An INT64 vector should be recognized as a Draken vector."""
        assert is_draken_vector(Vector(dn.vector_from_sequence([1, 2, 3]))) is True

    def test_float64_vector_is_draken(self):
        """A FLOAT64 vector should be recognized as a Draken vector."""
        assert is_draken_vector(Vector(dn.vector_float64_from_sequence([1.0, 2.0, 3.0]))) is True

    def test_bool_vector_is_draken(self):
        """A BOOL vector should be recognized as a Draken vector."""
        assert is_draken_vector(Vector(dn.vector_from_bool_sequence([True, False, True]))) is True
        assert is_draken_vector(BoolVector.from_constant(True, 3)) is True

    def test_string_vector_is_draken(self):
        """A VARCHAR vector should be recognized as a Draken vector."""
        assert is_draken_vector(Vector(dn.vector_from_string_sequence([b"a", b"b"]))) is True

    def test_fp16_embedding_vector_is_draken(self):
        """An embedding vector is a Draken vector like any other."""
        vec = Vector(dn.vector_fp16_from_sequence([[1.0, 2.0]], 2))
        assert is_draken_vector(vec) is True

    def test_scalar_not_draken_vector(self):
        """Scalars should NOT be recognized as Draken vectors."""
        assert is_draken_vector(42) is False
        assert is_draken_vector("hello") is False
        assert is_draken_vector(None) is False

    def test_foreign_container_not_draken_vector(self):
        """Raw containers should NOT be recognized as Draken vectors."""
        assert is_draken_vector([1, 2, 3]) is False
        assert is_draken_vector(array.array("q", [1, 2, 3])) is False


class TestVectorTypeEnum:
    """Tests for VectorType enum completeness."""

    def test_all_vector_types_defined(self):
        """VectorType is a closed dispatch vocabulary — adding or removing a
        member is a deliberate act, not an accident."""
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
            "DECIMAL",
            "UNKNOWN",
        }
        actual_types = {member.name for member in VectorType}
        assert expected_types == actual_types


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
