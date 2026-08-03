"""
Tests for E.19: vector_map_access_string / vector_map_access_array via vector_special.
"""

import glob
import importlib.util
import os

import draken.draken_native as dn
import pytest


# ---------------------------------------------------------------------------
# Load extensions
# ---------------------------------------------------------------------------

def _load(name):
    pattern = os.path.join(
        os.path.dirname(__file__), "..", "..", "..",
        "opteryx", "compiled", "nanobind", f"{name}*.so"
    )
    matches = glob.glob(pattern)
    if not matches:
        raise RuntimeError(f"{name} extension not built — run make compile")
    spec = importlib.util.spec_from_file_location(
        f"opteryx.compiled.nanobind.{name}", matches[0]
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


vs = _load("vectors")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_string_vec(*strings):
    """Build a VARCHAR DrakenVector from Python str/None values."""
    return dn.vector_from_string_sequence(
        [v.encode("utf-8") if isinstance(v, str) else v for v in strings]
    )


def make_int64_vec(value):
    """Build an Integer64Vector with one constant value."""
    return dn.vector_from_sequence([value])


# ---------------------------------------------------------------------------
# vector_map_access_string
# ---------------------------------------------------------------------------

class TestMapAccessString:

    def test_positive_index_first_char(self):
        vec = make_string_vec("hello", "world")
        key = make_int64_vec(0)
        result = vs.vector_map_access_string(vec, key)
        assert result[0] == "h"
        assert result[1] == "w"

    def test_positive_index_mid(self):
        vec = make_string_vec("hello", "world")
        key = make_int64_vec(1)
        result = vs.vector_map_access_string(vec, key)
        assert result[0] == "e"
        assert result[1] == "o"

    def test_negative_index_last_char(self):
        vec = make_string_vec("hello", "world")
        key = make_int64_vec(-1)
        result = vs.vector_map_access_string(vec, key)
        assert result[0] == "o"
        assert result[1] == "d"

    def test_negative_index_second_to_last(self):
        vec = make_string_vec("hello")
        key = make_int64_vec(-2)
        result = vs.vector_map_access_string(vec, key)
        assert result[0] == "l"

    def test_out_of_bounds_positive(self):
        vec = make_string_vec("hi")
        key = make_int64_vec(5)
        result = vs.vector_map_access_string(vec, key)
        assert result[0] is None

    def test_out_of_bounds_negative(self):
        vec = make_string_vec("hi")
        key = make_int64_vec(-10)
        result = vs.vector_map_access_string(vec, key)
        assert result[0] is None

    def test_null_input_propagates(self):
        vec = make_string_vec("hello", None, "world")
        key = make_int64_vec(0)
        result = vs.vector_map_access_string(vec, key)
        assert result[0] == "h"
        assert result[1] is None
        assert result[2] == "w"

    def test_all_null(self):
        vec = make_string_vec(None, None)
        key = make_int64_vec(0)
        result = vs.vector_map_access_string(vec, key)
        assert result[0] is None
        assert result[1] is None

    def test_empty_string_out_of_bounds(self):
        vec = make_string_vec("")
        key = make_int64_vec(0)
        result = vs.vector_map_access_string(vec, key)
        assert result[0] is None

    def test_output_length_one_char(self):
        vec = make_string_vec("abc")
        key = make_int64_vec(0)
        result = vs.vector_map_access_string(vec, key)
        assert len(result[0]) == 1

    def test_type_error_on_non_integer_key(self):
        vec = make_string_vec("hello")
        key = dn.vector_float64_from_sequence([0.0])
        with pytest.raises((TypeError, Exception)):
            vs.vector_map_access_string(vec, key)


# ---------------------------------------------------------------------------
# vector_map_access_array  (returns Python list)
# ---------------------------------------------------------------------------

class TestMapAccessArray:

    def _make_array_vec(self, rows):
        """Build an ArrayVector from a list of Python lists."""
        return dn.vector_array_from_sequence(rows)

    def test_positive_index(self):
        av = self._make_array_vec([[10, 20, 30], [40, 50, 60]])
        key = make_int64_vec(1)
        result = vs.vector_map_access_array(av, key)
        assert result == [20, 50]

    def test_first_element(self):
        av = self._make_array_vec([[1, 2], [3, 4]])
        key = make_int64_vec(0)
        result = vs.vector_map_access_array(av, key)
        assert result == [1, 3]

    def test_negative_index(self):
        av = self._make_array_vec([[1, 2, 3], [4, 5, 6]])
        key = make_int64_vec(-1)
        result = vs.vector_map_access_array(av, key)
        assert result == [3, 6]

    def test_out_of_bounds_positive(self):
        av = self._make_array_vec([[1, 2], [3, 4]])
        key = make_int64_vec(5)
        result = vs.vector_map_access_array(av, key)
        assert result == [None, None]

    def test_out_of_bounds_negative(self):
        av = self._make_array_vec([[1, 2], [3, 4]])
        key = make_int64_vec(-5)
        result = vs.vector_map_access_array(av, key)
        assert result == [None, None]

    def test_null_row_propagates(self):
        av = self._make_array_vec([[1, 2], None, [5, 6]])
        key = make_int64_vec(0)
        result = vs.vector_map_access_array(av, key)
        assert result[0] == 1
        assert result[1] is None
        assert result[2] == 5

    def test_mixed_length_rows(self):
        av = self._make_array_vec([[1], [2, 3], [4, 5, 6]])
        key = make_int64_vec(1)
        result = vs.vector_map_access_array(av, key)
        assert result[0] is None  # len=1, index 1 out-of-bounds
        assert result[1] == 3
        assert result[2] == 5

    def test_result_is_list(self):
        av = self._make_array_vec([[1, 2]])
        key = make_int64_vec(0)
        result = vs.vector_map_access_array(av, key)
        assert isinstance(result, list)
