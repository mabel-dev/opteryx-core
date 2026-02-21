"""IntervalVector tests covering Arrow interop and normalization."""

import datetime

import pytest
import pyarrow as pa

from opteryx.draken import Vector

MICROSECONDS_PER_DAY = 24 * 60 * 60 * 1_000_000
_MONTH_INTERVAL_FACTORY = getattr(pa, "month_interval", None)
_DAY_TIME_INTERVAL_FACTORY = getattr(pa, "day_time_interval", None)
MONTH_INTERVAL_TYPE = _MONTH_INTERVAL_FACTORY() if _MONTH_INTERVAL_FACTORY else None
DAY_TIME_INTERVAL_TYPE = _DAY_TIME_INTERVAL_FACTORY() if _DAY_TIME_INTERVAL_FACTORY else None


def _is_interval_vector(vec):
    return vec.__class__.__name__ == "IntervalVector"


def _build_month_day_nano():
    return pa.array(
        [
            (2, 0, 0),
            (0, 1, 1_500_000_000),
            None,
        ],
        type=pa.month_day_nano_interval(),
    )


def test_from_arrow_month_day_nano():
    arr = _build_month_day_nano()
    vec = Vector.from_arrow(arr)

    assert _is_interval_vector(vec)
    assert vec.to_pylist() == [
        (2, 0),
        (0, MICROSECONDS_PER_DAY + 1_500_000),
        None,
    ]


def test_to_arrow_interval_roundtrip():
    arr = _build_month_day_nano()
    vec = Vector.from_arrow(arr)

    rebuilt = vec.to_arrow_interval()
    assert rebuilt.equals(arr)


def test_fixed_size_binary_roundtrip():
    arr = _build_month_day_nano()
    vec = Vector.from_arrow(arr)

    binary = vec.to_arrow_binary()
    assert pa.types.is_fixed_size_binary(binary.type)

    vec2 = Vector.from_arrow(binary)
    assert _is_interval_vector(vec2)
    assert vec2.to_pylist() == vec.to_pylist()


def test_interval_vector_add_subtract():
    left = pa.array([(1, 0, 0), None, (0, 1, 0)], type=pa.month_day_nano_interval())
    right = pa.array([(0, 2, 0), (0, 0, 0), (0, 1, 0)], type=pa.month_day_nano_interval())

    left_vec = Vector.from_arrow(left)
    right_vec = Vector.from_arrow(right)

    added = left_vec.add_vector(right_vec).to_arrow_interval().to_pylist()
    subtracted = left_vec.subtract_vector(right_vec).to_arrow_interval().to_pylist()

    expected_added = pa.array(
        [(1, 2, 0), None, (0, 2, 0)],
        type=pa.month_day_nano_interval(),
    ).to_pylist()
    expected_subtracted = pa.array(
        [(1, -2, 0), None, (0, 0, 0)],
        type=pa.month_day_nano_interval(),
    ).to_pylist()

    assert added == expected_added
    assert subtracted == expected_subtracted


def test_interval_vector_compare_and_temporal_apply():
    left = pa.array([(0, 2, 0), None], type=pa.month_day_nano_interval())
    right = pa.array([(0, 1, 0), (0, 1, 0)], type=pa.month_day_nano_interval())

    left_vec = Vector.from_arrow(left)
    right_vec = Vector.from_arrow(right)

    compared = left_vec.compare_vector(right_vec, 2, False).to_arrow().to_pylist()
    assert compared == [True, None]

    with pytest.raises(ValueError, match="MONTH or YEAR"):
        months_left = Vector.from_arrow(pa.array([(1, 0, 0)], type=pa.month_day_nano_interval()))
        months_right = Vector.from_arrow(pa.array([(1, 0, 0)], type=pa.month_day_nano_interval()))
        months_left.compare_vector(months_right, 0, True)

    temporal = pa.array([datetime.date(2024, 1, 31), None], type=pa.date32())
    applied = right_vec.apply_to_temporal(temporal, 1)
    assert applied.to_pylist() == [datetime.datetime(2024, 2, 1, 0, 0), None]


def test_interval_vector_temporal_apply_proleptic_year_support():
    interval = pa.array([(-768, 0, 0)], type=pa.month_day_nano_interval())  # 64 years
    values = pa.array([datetime.date(1, 4, 23)], type=pa.date32())
    vec = Vector.from_arrow(interval)

    applied = vec.apply_to_temporal(values, 1)

    assert applied.type == pa.timestamp("us")
    assert applied.cast(pa.int64()).to_pylist() == [-64_145_606_400_000_000]


@pytest.mark.skipif(MONTH_INTERVAL_TYPE is None, reason="PyArrow build lacks month interval type")
def test_month_interval_normalization():
    arr = pa.array([3, None, -1], type=MONTH_INTERVAL_TYPE)
    vec = Vector.from_arrow(arr)

    assert vec.to_pylist() == [(3, 0), None, (-1, 0)]


@pytest.mark.skipif(DAY_TIME_INTERVAL_TYPE is None, reason="PyArrow build lacks day_time interval type")
def test_day_time_interval_normalization():
    arr = pa.array(
        [
            (0, 5_000),  # 5 seconds
            (1, 0),      # 1 day
            None,
        ],
        type=DAY_TIME_INTERVAL_TYPE,
    )
    vec = Vector.from_arrow(arr)

    expected = [
        (0, 5_000 * 1_000),
        (0, MICROSECONDS_PER_DAY),
        None,
    ]
    assert vec.to_pylist() == expected
