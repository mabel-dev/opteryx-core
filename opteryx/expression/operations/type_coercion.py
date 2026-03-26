"""Type coercion utilities for filter operations."""

import numpy
import pyarrow
from orso.types import OrsoTypes


def to_temporal_array(values, source_type, target_type):
    """
    Coerce values to temporal type (TIMESTAMP or DATE).
    Handles conversion from various input types including integers and strings.
    """
    from opteryx.expression.casts import parse_timestamp_value
    from opteryx.expression.functions.implementations.temporal import (
        convert_int64_array_to_pyarrow_datetime,
    )

    if isinstance(values, pyarrow.ChunkedArray):
        arr = values.combine_chunks() if values.num_chunks > 1 else values.chunk(0)
    elif isinstance(values, pyarrow.Array):
        arr = values
    elif isinstance(values, numpy.ndarray):
        arr = pyarrow.array(values.tolist())
    else:
        arr = pyarrow.array(values)

    if target_type == OrsoTypes.TIMESTAMP:
        if pyarrow.types.is_timestamp(arr.type):
            return (
                arr
                if arr.type == pyarrow.timestamp("us")
                else arr.cast(pyarrow.timestamp("us"))
            )
        if pyarrow.types.is_date32(arr.type):
            return arr.cast(pyarrow.timestamp("us"))
        if pyarrow.types.is_integer(arr.type):
            if source_type == OrsoTypes.DATE:
                date_arr = pyarrow.array(
                    [v.as_py() if hasattr(v, "as_py") else v for v in arr],
                    type=pyarrow.date32(),
                )
                return date_arr.cast(pyarrow.timestamp("us"))
            if source_type == OrsoTypes.TIMESTAMP:
                import datetime as _dt

                raw_values = [v.as_py() if hasattr(v, "as_py") else v for v in arr]
                if raw_values and all(
                    v is None or (abs(int(v)) < 100_000_000_000 and int(v) % 1_000_000 == 0)
                    for v in raw_values
                ):
                    return pyarrow.array(
                        [
                            None
                            if v is None
                            else _dt.datetime(1970, 1, 1)
                            + _dt.timedelta(days=int(v) // 1_000_000)
                            for v in raw_values
                        ],
                        type=pyarrow.timestamp("us"),
                    )
            return convert_int64_array_to_pyarrow_datetime(arr)
        return pyarrow.array(
            [parse_timestamp_value(v.as_py() if hasattr(v, "as_py") else v) for v in arr],
            type=pyarrow.timestamp("us"),
        )

    if target_type == OrsoTypes.DATE:
        if pyarrow.types.is_date32(arr.type):
            return arr
        if pyarrow.types.is_timestamp(arr.type):
            return arr.cast(pyarrow.date32())
        if pyarrow.types.is_integer(arr.type):
            return pyarrow.array(
                [v.as_py() if hasattr(v, "as_py") else v for v in arr],
                type=pyarrow.date32(),
            )
        return pyarrow.array(arr, type=pyarrow.date32())

    return arr
