"""
Dictionary-encoding tests for vector types that support from_dict.

Dictionary vectors store a codes array (int32 indices) plus a dictionary of
unique values.  Aggregations and reads must dereference through the dictionary.

Types with from_dict: Int64, Float64, Float32, Integer, Date32, Timestamp,
                      Time, String.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pytest

from draken.vectors.date32_vector import Date32Vector
from draken.vectors.float32_vector import Float32Vector
from draken.vectors.float64_vector import Float64Vector
from draken.vectors.integer64_vector import Integer64Vector
from draken.vectors.integer32_vector import Integer32Vector
from draken.vectors.string_vector import StringVector
from draken.vectors.time_vector import TimeVector
from draken.vectors.timestamp_vector import TimestampVector


# ---------------------------------------------------------------------------
# Int64
# ---------------------------------------------------------------------------

class TestInt64Dict:
    def test_to_pylist(self):
        v = Integer64Vector.from_dict([0, 1, 2, 0], [10, 20, 30])
        assert v.to_pylist() == [10, 20, 30, 10]

    def test_sum(self):
        v = Integer64Vector.from_dict([0, 1, 2, 0], [10, 20, 30])
        assert v.sum() == 70

    def test_min(self):
        v = Integer64Vector.from_dict([0, 1, 2, 0], [10, 20, 30])
        assert v.min() == 10

    def test_max(self):
        v = Integer64Vector.from_dict([0, 1, 2, 0], [10, 20, 30])
        assert v.max() == 30

    def test_with_nulls(self):
        v = Integer64Vector.from_dict([0, 1, 2, 0], [10, 20, 30],
                                  row_validity=[1, 1, 1, 0])
        assert v.to_pylist() == [10, 20, 30, None]
        assert v.null_count == 1
        assert v.sum() == 60
        assert v.min() == 10

    def test_length(self):
        assert len(Integer64Vector.from_dict([0, 0, 1], [5, 10])) == 3

    def test_repeated_code(self):
        v = Integer64Vector.from_dict([0, 0, 0, 0], [42])
        assert v.sum() == 168
        assert v.min() == 42
        assert v.max() == 42

    def test_negative_values(self):
        v = Integer64Vector.from_dict([0, 1, 2], [-10, 0, 10])
        assert v.sum() == 0
        assert v.min() == -10
        assert v.max() == 10


# ---------------------------------------------------------------------------
# Float64
# ---------------------------------------------------------------------------

class TestFloat64Dict:
    def test_sum(self):
        v = Float64Vector.from_dict([0, 1, 2, 0], [1.5, 2.5, 3.5])
        assert v.sum() == pytest.approx(9.0)

    def test_min_max(self):
        v = Float64Vector.from_dict([0, 1, 2], [1.5, 2.5, 3.5])
        assert v.min() == pytest.approx(1.5)
        assert v.max() == pytest.approx(3.5)

    def test_with_nulls(self):
        v = Float64Vector.from_dict([0, 1, 2], [1.0, 2.0, 3.0],
                                    row_validity=[1, 0, 1])
        assert v.null_count == 1
        assert v.sum() == pytest.approx(4.0)

    def test_to_pylist(self):
        v = Float64Vector.from_dict([1, 0, 1], [10.0, 20.0])
        assert v.to_pylist() == [20.0, 10.0, 20.0]


# ---------------------------------------------------------------------------
# Float32
# ---------------------------------------------------------------------------

class TestFloat32Dict:
    def test_sum(self):
        v = Float32Vector.from_dict([0, 1, 2, 0], [1.0, 2.0, 3.0])
        assert v.sum() == pytest.approx(7.0)

    def test_min_max(self):
        v = Float32Vector.from_dict([0, 1, 2], [1.0, 2.0, 3.0])
        assert v.min() == pytest.approx(1.0)
        assert v.max() == pytest.approx(3.0)

    def test_with_nulls(self):
        v = Float32Vector.from_dict([0, 1], [10.0, 20.0],
                                    row_validity=[0, 1])
        assert v.null_count == 1
        assert v.sum() == pytest.approx(20.0)


# ---------------------------------------------------------------------------
# Integer (small int)
# ---------------------------------------------------------------------------

class TestIntegerDict:
    def test_to_pylist(self):
        v = Integer32Vector.from_dict([0, 1, 0, 1], [5, 10])
        assert v.to_pylist() == [5, 10, 5, 10]

    def test_sum(self):
        assert Integer32Vector.from_dict([0, 1, 0, 1], [5, 10]).sum() == 30

    def test_min_max(self):
        v = Integer32Vector.from_dict([0, 1, 2], [3, 1, 5])
        assert v.min() == 1
        assert v.max() == 5

    def test_with_nulls(self):
        v = Integer32Vector.from_dict([0, 1, 0], [5, 10],
                                    row_validity=[1, 0, 1])
        assert v.null_count == 1
        assert v.sum() == 10


# ---------------------------------------------------------------------------
# Date32
# ---------------------------------------------------------------------------

class TestDate32Dict:
    def test_to_pylist_raw(self):
        # Date32 stores days-since-epoch; to_pylist returns datetime.date
        v = Date32Vector.from_dict([0, 1, 0, 1], [100, 200])
        py = v.to_pylist()
        assert len(py) == 4

    def test_min_max(self):
        v = Date32Vector.from_dict([0, 1, 0], [100, 200])
        assert v.min() == 100
        assert v.max() == 200

    def test_sum(self):
        v = Date32Vector.from_dict([0, 1, 0, 1], [100, 200])
        assert v.sum() == 600

    def test_with_nulls(self):
        v = Date32Vector.from_dict([0, 1, 0], [100, 200],
                                   row_validity=[1, 0, 1])
        assert v.null_count == 1
        assert v.sum() == 200


# ---------------------------------------------------------------------------
# Timestamp
# ---------------------------------------------------------------------------

class TestTimestampDict:
    def test_min_max(self):
        v = TimestampVector.from_dict([0, 1, 2], [1000, 2000, 3000])
        assert v.min() == 1000
        assert v.max() == 3000

    def test_sum(self):
        assert TimestampVector.from_dict([0, 1, 2], [1000, 2000, 3000]).sum() == 6000

    def test_with_nulls(self):
        v = TimestampVector.from_dict([0, 1, 2], [1000, 2000, 3000],
                                      row_validity=[1, 0, 1])
        assert v.null_count == 1
        assert v.sum() == 4000

    def test_to_pylist(self):
        v = TimestampVector.from_dict([0, 0, 1], [1000, 2000])
        py = v.to_pylist()
        assert len(py) == 3


# ---------------------------------------------------------------------------
# Time
# ---------------------------------------------------------------------------

class TestTimeDict:
    def test_min_max_time64(self):
        v = TimeVector.from_dict([0, 1, 2], [100, 200, 300], is_time64=True)
        assert v.min() == 100
        assert v.max() == 300

    def test_sum_time64(self):
        v = TimeVector.from_dict([0, 1, 2], [100, 200, 300], is_time64=True)
        assert v.sum() == 600

    def test_with_nulls_time64(self):
        v = TimeVector.from_dict([0, 1, 2], [100, 200, 300],
                                 row_validity=[1, 0, 1], is_time64=True)
        assert v.null_count == 1
        assert v.sum() == 400

    def test_repeated_codes(self):
        v = TimeVector.from_dict([0, 0, 0], [500], is_time64=True)
        assert v.sum() == 1500
        assert v.min() == 500
        assert v.max() == 500


# ---------------------------------------------------------------------------
# String
# ---------------------------------------------------------------------------

class TestStringDict:
    def test_to_pylist(self):
        v = StringVector.from_dict([0, 1, 2, 1, 0],
                                   ["apple", "banana", "cherry"])
        assert v.to_pylist() == [b"apple", b"banana", b"cherry", b"banana", b"apple"]

    def test_min(self):
        v = StringVector.from_dict([0, 1, 2],
                                   ["cherry", "apple", "banana"])
        assert v.min() == b"apple"

    def test_max(self):
        v = StringVector.from_dict([0, 1, 2],
                                   ["cherry", "apple", "banana"])
        assert v.max() == b"cherry"

    def test_with_nulls(self):
        v = StringVector.from_dict([0, 1, 2], ["a", "b", "c"],
                                   row_validity=[1, 0, 1])
        assert v.null_count == 1
        py = v.to_pylist()
        assert py[1] is None

    def test_prefix_ordering(self):
        # Dictionary containing prefix-extension pairs.
        v = StringVector.from_dict([0, 1, 2], ["ab", "abcd", "abc"])
        assert v.min() == b"ab"
        assert v.max() == b"abcd"

    def test_repeated_codes(self):
        v = StringVector.from_dict([0, 0, 0], ["hello"])
        assert v.to_pylist() == [b"hello", b"hello", b"hello"]
        assert v.min() == b"hello"
        assert v.max() == b"hello"

    def test_sum_raises(self):
        v = StringVector.from_dict([0, 1], ["a", "b"])
        with pytest.raises(NotImplementedError):
            v.sum()

    def test_length(self):
        assert len(StringVector.from_dict([0, 1, 2, 0], ["x", "y", "z"])) == 4
