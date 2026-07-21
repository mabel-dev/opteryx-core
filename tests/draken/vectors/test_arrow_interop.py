"""Vector construction + basic op smoke tests across dtypes.

This module used to test "Arrow interoperability" (Vector.from_arrow/to_arrow,
zero-copy wrapping of Arrow arrays). That capability doesn't exist in the
current Vector API — there is no from_arrow or to_arrow at all. The real,
current "Python list -> Vector" entry point is
draken.interop.vector_sequence.vector_from_sequence(values, dtype); see
draken/interop/vector_sequence.py.

PyArrow is kept here only as an independent oracle for the expected aggregate
value (computed from the same plain Python list, not used to construct the
Draken vector), matching CLAUDE.md's "PyArrow may be used for testing".
"""
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest
import pyarrow as pa
import pyarrow.compute as pc

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence

TESTS = [
    # Boolean: count trues
    (
        [True, False, True, None, False],
        DrakenType.BOOL,
        lambda values: pc.sum(pa.array(values).cast(pa.int8())).as_py(),
        lambda vec: sum(1 for v in vec.to_pylist() if v),
    ),
    # Int64: sum
    (
        [1, 2, 3, None, 5],
        DrakenType.INT64,
        lambda values: pc.sum(pa.array(values, type=pa.int64())).as_py(),
        lambda vec: vec.sum(),
    ),
    # Float64: sum
    (
        [1.5, 2.5, None, -1.0],
        DrakenType.FLOAT64,
        lambda values: pc.sum(pa.array(values, type=pa.float64())).as_py(),
        lambda vec: vec.sum(),
    ),
    # Binary: total length of all buffers
    (
        [b"a", b"bb", None, b"ccc"],
        DrakenType.VARBINARY,
        lambda values: pc.sum(pc.binary_length(pa.array(values, type=pa.binary()))).as_py(),
        lambda vec: sum(len(s) for s in vec.to_pylist() if s is not None),
    ),
    # Date32: min
    (
        [datetime.date(2019, 4, 4), datetime.date(2020, 8, 9), None, datetime.date(2022, 1, 15)],
        DrakenType.DATE32,
        lambda values: pc.min(pa.array(values, type=pa.date32())).as_py(),
        lambda vec: vec.min(),
    ),
    # Timestamp: max
    (
        [
            datetime.datetime(2021, 1, 1, 0, 16, 40),
            None,
            datetime.datetime(2021, 1, 1, 0, 50, 0),
            datetime.datetime(2021, 1, 1, 0, 33, 20),
        ],
        DrakenType.TIMESTAMP64,
        lambda values: pc.max(pa.array(values, type=pa.timestamp("us"))).as_py().replace(tzinfo=datetime.timezone.utc),
        lambda vec: vec.max(),
    ),
    # List/Array: count non-null
    (
        [[1, 2], [3], None, [4, 5, 6]],
        DrakenType.ARRAY,
        lambda values: len(values) - sum(1 for v in values if v is None),
        lambda vec: vec.length - sum(1 for i in range(vec.length) if vec.is_null_at(i)),
    ),
]


@pytest.mark.parametrize("values,dtype,op_arrow,op_draken", TESTS)
def test_draken_matches_arrow(values, dtype, op_arrow, op_draken):
    vec = vector_from_sequence(values, dtype=dtype)

    result_arrow = op_arrow(values)
    result_draken = op_draken(vec)
    assert result_arrow == result_draken, f"Draken and Arrow results differ: {result_draken} != {result_arrow}"


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__])
