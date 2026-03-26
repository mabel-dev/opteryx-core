import pytest

from tests.performance.benchmarks.python_carchar_reference import (
    CarcharIndex as PythonCarcharIndex,
)
from tests.performance.benchmarks.python_carchar_reference import (
    CarcharJoinIndex as PythonCarcharJoinIndex,
)

cpp_carchar = pytest.importorskip("opteryx.compiled.nanobind.carchar_native")


def test_cpp_carchar_join_index_matches_python_duplicate_rows():
    py_index = PythonCarcharJoinIndex(initial_capacity=16)
    cpp_index = cpp_carchar.CarcharJoinIndex(16, 0.80)

    for row_id in (10, 20, 30, 40):
        py_index.insert_row(0xAAAA, row_id)
        cpp_index.insert_row(0xAAAA, row_id)

    assert cpp_index.rows_for(0xAAAA) == py_index.rows_for(0xAAAA)
    assert cpp_index.get(0xAAAA) == py_index.rows_for(0xAAAA)
    assert cpp_index.row_count_for(0xAAAA) == py_index.row_count_for(0xAAAA)
    assert cpp_index.rows_for(0xBBBB) == []
    assert cpp_index.row_count_for(0xBBBB) == 0


def test_cpp_carchar_index_lookup_matches_python():
    py_index = PythonCarcharIndex(initial_capacity=16, load_factor=0.50)
    cpp_index = cpp_carchar.CarcharIndex(16, 0.50)

    for i in range(40):
        py_index.insert_new(i, i * 10)
        cpp_index.insert_new(i, i * 10)

    for i in range(40):
        assert cpp_index.lookup(i) == py_index.lookup(i)

    assert cpp_index.lookup(10_000) is None


def test_cpp_carchar_stats_are_populated():
    index = cpp_carchar.CarcharJoinIndex(16, 0.80)
    index.insert_row(1, 100)
    index.insert_row(1, 200)
    index.rows_for(1)

    stats = index.stats()
    assert stats.size == 1
    assert stats.capacity >= 16
    assert stats.insert_count >= 2
    assert stats.lookup_count >= 1


def test_cpp_carchar_set_correctness_matches_python_and_abseil():
    absl_containers = pytest.importorskip("opteryx.third_party.abseil.containers")
    FlatHashSet = absl_containers.FlatHashSet

    cpp_set = cpp_carchar.CarcharSet(16, 0.80)
    absl_set = FlatHashSet()
    py_set = set()

    values = [1, 2, 2, 3, 7, 7, 7, 11, 13, 13, 0, 2**32 + 9]

    for value in values:
        cpp_added = cpp_set.insert_or_ignore(value)
        absl_added = bool(absl_set.add(value))
        py_added = value not in py_set
        py_set.add(value)

        assert cpp_added == py_added
        assert absl_added == py_added

    assert cpp_set.size() == len(py_set)
    assert absl_set.items() == len(py_set)

    for value in [0, 1, 3, 7, 13, 2**32 + 9, 999999]:
        assert cpp_set.contains(value) == (value in py_set)
        assert bool(absl_set.has(value)) == (value in py_set)


def test_cpp_partitioned_carchar_join_engine_matches_rows_and_counts():
    engine_cls = getattr(cpp_carchar, "CarcharJoinEngine", None)
    if engine_cls is None:
        pytest.skip("partitioned Carchar engine not available")

    engine = engine_cls(16, 4, 0.80)
    for row_id in (10, 20, 30, 40):
        engine.insert_row(0xAAAA, row_id)
    engine.insert_row(0xBBBB, 99)
    engine.seal()

    assert engine.rows_for(0xAAAA) == [10, 20, 30, 40]
    assert engine.row_count_for(0xAAAA) == 4
    assert engine.rows_for(0xBBBB) == [99]
    assert engine.row_count_for(0xBBBB) == 1
    assert engine.row_count_for(0xCCCC) == 0
