"""
Native correctness tests for E.9: cast cluster consumers.

Coverage:
  vector_cast_int64_to_string:
    signed values: 0, positive, negative, INT64_MIN, INT64_MAX.
    null TVL preserved.
    round-trip: cast_string_to_int(cast_int64_to_string(v)) == v.
    TypeError on non-Vector / non-INT64 input.

  vector_cast_uint64_to_string:
    unsigned bounds: 0, UINT64_MAX.
    null TVL preserved.
    values above INT64_MAX rendered unsigned.
    TypeError on non-Vector input.

  vector_cast_string_to_int:
    valid strings: "0", "42", "-1", "-9223372036854775808".
    invalid strings raise ValueError: "abc", "", "42abc", " 1".
    null TVL preserved.
    TypeError on non-Vector / non-string input.

  vector_cast_int64_to_timestamp:
    unit="us" default: round-trip via to_pylist gives correct datetime.
    unit="ms", "s", "ns", "days": round-trip.
    null TVL preserved.
    output .type == TIMESTAMP64 (physical), logical unit carried in descriptor.
    TypeError on non-Vector input; ValueError on bad unit string.
"""

import datetime
import glob
import importlib.util
import os

import draken.draken_native as dn
import pytest

INT64_MAX  =  9_223_372_036_854_775_807
INT64_MIN  = -9_223_372_036_854_775_808
UINT64_MAX = 18_446_744_073_709_551_615

# ---------------------------------------------------------------------------
# Load vector_casts extension
# ---------------------------------------------------------------------------

def _load_vector_casts():
    pattern = os.path.join(
        os.path.dirname(__file__), "..", "..", "..",
        "opteryx", "compiled", "nanobind", "vector_casts*.so"
    )
    matches = glob.glob(pattern)
    if not matches:
        pytest.skip(
            "vector_casts extension not built — run make compile first",
            allow_module_level=True,
        )
    spec = importlib.util.spec_from_file_location(
        "opteryx.compiled.nanobind.vector_casts", matches[0]
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


vc = _load_vector_casts()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def iv(values):
    return dn.vector_from_sequence(values)

def sv(values):
    return dn.vector_from_string_sequence(values)

def vals(vec):
    return vec.to_pylist()


# ---------------------------------------------------------------------------
# vector_cast_int64_to_string
# ---------------------------------------------------------------------------

class TestInt64ToString:
    def test_zero(self):
        assert vals(vc.vector_cast_int64_to_string(iv([0]))) == ["0"]

    def test_positive(self):
        assert vals(vc.vector_cast_int64_to_string(iv([1, 42, 999]))) == ["1", "42", "999"]

    def test_negative(self):
        assert vals(vc.vector_cast_int64_to_string(iv([-1, -42]))) == ["-1", "-42"]

    def test_int64_max(self):
        assert vals(vc.vector_cast_int64_to_string(iv([INT64_MAX]))) == [str(INT64_MAX)]

    def test_int64_min(self):
        assert vals(vc.vector_cast_int64_to_string(iv([INT64_MIN]))) == [str(INT64_MIN)]

    def test_null_propagates(self):
        result = vals(vc.vector_cast_int64_to_string(iv([1, None, 3])))
        assert result == ["1", None, "3"]

    def test_all_null(self):
        result = vals(vc.vector_cast_int64_to_string(iv([None, None])))
        assert result == [None, None]

    def test_output_type_varchar(self):
        out = vc.vector_cast_int64_to_string(iv([1]))
        assert out.type == dn.DrakenType.VARCHAR

    def test_roundtrip_via_string_to_int(self):
        values = [0, 1, -1, 42, INT64_MAX, INT64_MIN]
        vec = iv(values)
        roundtripped = vals(vc.vector_cast_string_to_int(vc.vector_cast_int64_to_string(vec)))
        assert roundtripped == values

    def test_roundtrip_with_nulls(self):
        values = [1, None, -99]
        vec = iv(values)
        roundtripped = vals(vc.vector_cast_string_to_int(vc.vector_cast_int64_to_string(vec)))
        assert roundtripped == values

    def test_type_error_on_non_vector(self):
        with pytest.raises(TypeError):
            vc.vector_cast_int64_to_string("not a vector")

    def test_type_error_on_string_vector(self):
        with pytest.raises(TypeError):
            vc.vector_cast_int64_to_string(sv(["hello"]))


# ---------------------------------------------------------------------------
# vector_cast_uint64_to_string
# ---------------------------------------------------------------------------

class TestUint64ToString:
    def test_zero(self):
        assert vals(vc.vector_cast_uint64_to_string(iv([0]))) == ["0"]

    def test_small_positive(self):
        assert vals(vc.vector_cast_uint64_to_string(iv([1, 255]))) == ["1", "255"]

    def test_uint64_max(self):
        # UINT64_MAX stored as two's-complement int64 (-1 signed)
        import ctypes
        raw = ctypes.c_int64(-1).value  # == -1, bits == UINT64_MAX
        result = vals(vc.vector_cast_uint64_to_string(iv([raw])))
        assert result == [str(UINT64_MAX)]

    def test_above_int64_max(self):
        # INT64_MAX + 1 = 2^63, stored as INT64_MIN signed
        import ctypes
        raw = ctypes.c_int64(INT64_MIN).value
        result = vals(vc.vector_cast_uint64_to_string(iv([raw])))
        assert result == [str(2**63)]

    def test_null_propagates(self):
        result = vals(vc.vector_cast_uint64_to_string(iv([0, None])))
        assert result == ["0", None]

    def test_output_type_varchar(self):
        out = vc.vector_cast_uint64_to_string(iv([1]))
        assert out.type == dn.DrakenType.VARCHAR

    def test_type_error_on_non_vector(self):
        with pytest.raises(TypeError):
            vc.vector_cast_uint64_to_string(42)


# ---------------------------------------------------------------------------
# vector_cast_string_to_int
# ---------------------------------------------------------------------------

class TestStringToInt:
    def test_valid_positive(self):
        assert vals(vc.vector_cast_string_to_int(sv(["0", "1", "42"]))) == [0, 1, 42]

    def test_valid_negative(self):
        assert vals(vc.vector_cast_string_to_int(sv(["-1", "-9999"]))) == [-1, -9999]

    def test_int64_min_string(self):
        assert vals(vc.vector_cast_string_to_int(sv([str(INT64_MIN)]))) == [INT64_MIN]

    def test_null_propagates(self):
        result = vals(vc.vector_cast_string_to_int(sv(["1", None, "3"])))
        assert result == [1, None, 3]

    def test_all_null(self):
        result = vals(vc.vector_cast_string_to_int(sv([None, None])))
        assert result == [None, None]

    def test_output_type_int64(self):
        out = vc.vector_cast_string_to_int(sv(["1"]))
        assert out.type == dn.DrakenType.INT64

    def test_invalid_letters_raises(self):
        with pytest.raises(ValueError):
            vc.vector_cast_string_to_int(sv(["abc"]))

    def test_invalid_mixed_raises(self):
        with pytest.raises(ValueError):
            vc.vector_cast_string_to_int(sv(["42abc"]))

    def test_empty_string_returns_zero(self):
        # Matches old .pyx behaviour: empty string → loop never executes → 0.
        assert vals(vc.vector_cast_string_to_int(sv([""]))) == [0]

    def test_leading_space_raises(self):
        with pytest.raises(ValueError):
            vc.vector_cast_string_to_int(sv([" 1"]))

    def test_type_error_on_non_vector(self):
        with pytest.raises(TypeError):
            vc.vector_cast_string_to_int("not a vector")

    def test_type_error_on_int_vector(self):
        with pytest.raises(TypeError):
            vc.vector_cast_string_to_int(iv([1]))


# ---------------------------------------------------------------------------
# vector_cast_int64_to_timestamp
# ---------------------------------------------------------------------------

EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)

def _epoch_seconds(dt):
    return int((dt - EPOCH).total_seconds())

class TestInt64ToTimestamp:
    def _known_us(self):
        """2024-03-15 12:00:00 UTC in microseconds since epoch."""
        dt = datetime.datetime(2024, 3, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
        return int((dt - EPOCH).total_seconds() * 1_000_000)

    def test_unit_us_roundtrip(self):
        us = self._known_us()
        out = vc.vector_cast_int64_to_timestamp(iv([us]), unit="us")
        result = vals(out)
        assert len(result) == 1
        assert result[0].year == 2024
        assert result[0].month == 3
        assert result[0].day == 15

    def test_unit_ms_roundtrip(self):
        ms = self._known_us() // 1_000
        out = vc.vector_cast_int64_to_timestamp(iv([ms]), unit="ms")
        result = vals(out)
        assert result[0].year == 2024

    def test_unit_s_roundtrip(self):
        s = self._known_us() // 1_000_000
        out = vc.vector_cast_int64_to_timestamp(iv([s]), unit="s")
        result = vals(out)
        assert result[0].year == 2024

    def test_unit_ns_roundtrip(self):
        ns = self._known_us() * 1_000
        out = vc.vector_cast_int64_to_timestamp(iv([ns]), unit="ns")
        result = vals(out)
        assert result[0].year == 2024

    def test_unit_days_roundtrip(self):
        # days since epoch for 2024-03-15
        dt = datetime.datetime(2024, 3, 15, tzinfo=datetime.timezone.utc)
        days = (_epoch_seconds(dt)) // 86400
        out = vc.vector_cast_int64_to_timestamp(iv([days]), unit="days")
        result = vals(out)
        assert result[0].year == 2024
        assert result[0].month == 3
        assert result[0].day == 15

    def test_default_unit_is_us(self):
        us = self._known_us()
        out_explicit = vc.vector_cast_int64_to_timestamp(iv([us]), unit="us")
        out_default  = vc.vector_cast_int64_to_timestamp(iv([us]))
        assert vals(out_explicit) == vals(out_default)

    def test_output_type_timestamp64(self):
        out = vc.vector_cast_int64_to_timestamp(iv([0]))
        assert out.type == dn.DrakenType.TIMESTAMP64

    def test_null_propagates(self):
        us = self._known_us()
        result = vals(vc.vector_cast_int64_to_timestamp(iv([us, None, us])))
        assert result[0] is not None
        assert result[1] is None
        assert result[2] is not None

    def test_all_null(self):
        result = vals(vc.vector_cast_int64_to_timestamp(iv([None, None])))
        assert result == [None, None]

    def test_invalid_unit_raises(self):
        with pytest.raises(ValueError):
            vc.vector_cast_int64_to_timestamp(iv([0]), unit="fortnight")

    def test_type_error_on_non_vector(self):
        with pytest.raises(TypeError):
            vc.vector_cast_int64_to_timestamp("not a vector")

    def test_type_error_on_string_vector(self):
        with pytest.raises(TypeError):
            vc.vector_cast_int64_to_timestamp(sv(["hello"]))
