import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from tests.helpers import execute_and_get_rowcount, execute_and_get_shape


def test_predicate_compaction_prefers_strongest_lower_bound():
    # Optimizer should prefer id > 4 over id > 1
    count = execute_and_get_rowcount("SELECT id FROM testdata.planets WHERE id > 4 AND id > 1")
    assert count == 5


def test_predicate_compaction_collapse_to_equality():
    count = execute_and_get_rowcount(
        "SELECT id FROM testdata.planets WHERE id = 3 AND id > 1 AND id < 9"
    )
    assert count == 1


def test_predicate_compaction_contradiction_preserves_schema():
    shape = execute_and_get_shape("SELECT * FROM testdata.planets WHERE id > 1 AND id == 0")
    assert shape[0] == 0  # 0 rows
    assert shape[1] == 20  # 20 columns


def test_predicate_compaction_prefers_strongest_upper_bound():
    count = execute_and_get_rowcount(
        "SELECT id FROM testdata.planets WHERE id < 8 AND id < 5"
    )
    assert count == 4


def test_predicate_compaction_handles_mixed_order_bounds():
    count = execute_and_get_rowcount(
        "SELECT id FROM testdata.planets WHERE id < 8 AND id > 1 AND id > 5 AND id < 9"
    )
    assert count == 2


def test_predicate_compaction_respects_other_column_filters():
    count = execute_and_get_rowcount(
        "SELECT id FROM testdata.planets WHERE id > 1 AND mass > 0 AND id > 4"
    )
    assert count == 5


def test_predicate_compaction_across_subquery_boundary():
    count = execute_and_get_rowcount(
        "SELECT name FROM (SELECT * FROM testdata.planets WHERE id > 4 AND id > 1) AS p"
    )
    assert count == 5


def test_predicate_compaction_inherited_from_outer_query():
    count = execute_and_get_rowcount(
        "SELECT * FROM (SELECT id FROM testdata.planets WHERE id > 1) AS p WHERE id > 4"
    )
    assert count == 5


def test_predicate_compaction_with_three_bounds():
    count = execute_and_get_rowcount(
        "SELECT id FROM testdata.planets WHERE id > 0 AND id > 3 AND id > 4"
    )
    assert count == 5


def test_predicate_compaction_handles_different_columns_and_bounds():
    count = execute_and_get_rowcount(
        "SELECT id FROM testdata.planets WHERE id > 1 AND id > 4 AND diameter > 5000"
    )
    assert count == 4


def test_predicate_compaction_handles_alias_qualified_columns():
    count = execute_and_get_rowcount(
        "SELECT p.id FROM $planets AS p WHERE p.id > 1 AND p.id > 4"
    )
    assert count == 5


def test_predicate_compaction_applied_to_other_dataset():
    count = execute_and_get_rowcount(
        "SELECT planetId FROM testdata.satellites WHERE planetId > 1 AND planetId > 4"
    )
    assert count == 174


def test_predicate_compaction_prefers_exclusive_over_inclusive_lower():
    count = execute_and_get_rowcount(
        "SELECT id FROM testdata.planets WHERE id >= 4 AND id > 4"
    )
    assert count == 5


def test_predicate_compaction_prefers_exclusive_over_inclusive_upper():
    count = execute_and_get_rowcount(
        "SELECT id FROM testdata.planets WHERE id <= 8 AND id < 8"
    )
    assert count == 7


def test_predicate_compaction_keeps_equality_with_additional_filters():
    count = execute_and_get_rowcount(
        "SELECT id FROM testdata.planets WHERE id > 1 AND id = 3 AND diameter < 15_000"
    )
    assert count == 1


def test_predicate_compaction_contradiction_inside_subquery():
    shape = execute_and_get_shape(
        "SELECT * FROM (SELECT * FROM testdata.planets WHERE id > 1 AND id == 0) AS p"
    )
    assert shape[0] == 0  # 0 rows
    assert shape[1] == 20  # 20 columns

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
