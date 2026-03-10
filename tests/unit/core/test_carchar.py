import os
import sys


sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from tests.performance.benchmarks.python_carchar_reference import CarcharIndex
from tests.performance.benchmarks.python_carchar_reference import CarcharJoinIndex


def test_carchar_index_find_or_insert_and_lookup():
    index = CarcharIndex(initial_capacity=16)

    payload_ref, created = index.find_or_insert(0x1234, lambda: 7)
    assert created is True
    assert payload_ref == 7

    payload_ref, created = index.find_or_insert(0x1234, lambda: 99)
    assert created is False
    assert payload_ref == 7
    assert index.lookup(0x1234) == 7
    assert index.lookup(0x5678) is None


def test_carchar_index_resizes_and_preserves_entries():
    index = CarcharIndex(initial_capacity=16, load_factor=0.50)

    for i in range(400):
        index.insert_new(i, i * 10)

    for i in range(400):
        assert index.lookup(i) == i * 10

    stats = index.stats()
    assert stats.size == 400
    assert stats.capacity >= 64
    assert stats.resize_count >= 1


def test_carchar_join_index_stores_duplicate_rows_inline_and_overflow():
    index = CarcharJoinIndex(initial_capacity=16)

    payload_ref, created = index.insert_row(0xAAAA, 10)
    assert created is True
    assert payload_ref == 0

    _, created = index.insert_row(0xAAAA, 20)
    assert created is False

    _, created = index.insert_row(0xAAAA, 30)
    assert created is False

    _, created = index.insert_row(0xAAAA, 40)
    assert created is False

    assert index.rows_for(0xAAAA) == [10, 20, 30, 40]
    assert index.row_count_for(0xAAAA) == 4
    assert index.rows_from_payload(payload_ref) == [10, 20, 30, 40]
    assert index.rows_for(0xBBBB) == []
    assert index.row_count_for(0xBBBB) == 0


def test_carchar_insert_new_rejects_duplicate_key():
    index = CarcharIndex(initial_capacity=16)
    index.insert_new(1, 11)

    try:
        index.insert_new(1, 22)
    except KeyError:
        pass
    else:
        assert False, "expected duplicate insert to raise KeyError"

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
