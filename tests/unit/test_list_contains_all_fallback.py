import draken.draken_native as dn
from opteryx.compiled.nanobind.vectors import vector_contains_all


def test_list_contains_all_basic():
    arr = dn.vector_array_from_sequence([[1, 2, 3], [2, 3], [None], []])
    items = {2, 3}
    res = vector_contains_all(arr, items)
    assert res.to_pylist() == [True, True, False, False]
