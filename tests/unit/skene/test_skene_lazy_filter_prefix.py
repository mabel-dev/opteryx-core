"""A lazily filtered column round-trips through skene (the spill format).

The engine's filter borrows its source's whole payload (draken lazy_select_column), so
`id <= 2` over $planets hands over two rows with data_length 9 and codes [0, 1]. The
writer refused that as "identity but data_length != length" — the intermittent GROUP BY
output spill crash (`spill (GroupedAggregateHashedNode): column 'c0': ...`).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "..", "..", ".."))

import pytest

import opteryx
import skene


@pytest.mark.parametrize(
    "predicate, expected",
    [("id <= 2", [1, 2]), ("id = 1", [1]), ("id IN (3, 1)", [1, 3]), ("id >= 8", [8, 9])],
)
def test_filtered_column_round_trips(predicate, expected):
    morsels = list(opteryx.session().execute_to_morsels(f"SELECT id, name FROM $planets WHERE {predicate}"))
    assert len(morsels) == 1
    back = skene.read_morsel(skene.write_morsel(morsels[0]), 0)
    back.materialize()
    assert back.column("id").to_pylist() == expected
    assert back.column("name").to_pylist() == morsels[0].column("name").to_pylist()


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
