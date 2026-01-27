from opteryx.compiled.list_ops import list_contains_all


def test_list_contains_all_basic():
    arr = [[1,2,3],[2,3],[None],[]]
    items = {2,3}
    res = list_contains_all(arr, items)
    assert list(res) == [1,0,0,0]
