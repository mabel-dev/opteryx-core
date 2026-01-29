import numpy as np
import pyarrow as pa
from opteryx.compiled.list_ops import list_length
from opteryx.nanobind.list_length import offsets_to_lengths as offsets_to_lengths_native


def test_offsets_to_lengths_matches_numpy():
    n = 1000
    values = [[str(i)] * (i % 5) for i in range(n)]
    arr = pa.array(values)
    offsets = np.frombuffer(arr.buffers()[1], dtype=np.int32, count=n + 1)

    expected = (offsets[1:] - offsets[:-1]).astype(np.uint32)
    native = offsets_to_lengths_native(offsets)
    native_np = np.frombuffer(native, dtype=np.uint32)
    assert np.array_equal(expected, native_np)


def test_list_length_on_listarray():
    n = 1000
    values = [[str(i)] * (i % 5) for i in range(n)]
    arr = pa.array(values)

    res = list_length(arr)
    res_np = np.frombuffer(res, dtype=np.uint32)
    offsets = np.frombuffer(arr.buffers()[1], dtype=np.int32, count=n + 1)
    expected = (offsets[1:] - offsets[:-1]).astype(np.uint32)
    assert np.array_equal(expected, res_np)
