# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""WP-10: IN-list row-group pruning (Python pruning path).

Guards the conservative, fail-open semantics of ``IN``/``NOT IN`` row-group
pruning. A row group may only be pruned when it is *impossible* for any list
value to fall within the column's [min, max]; missing or incomparable stats
must keep the row group (correctness over performance).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.connectors.parquet_io.predicates import _can_prune_rowgroup
from opteryx.connectors.parquet_io.predicates import row_group_may_satisfy


# --- _can_prune_rowgroup: InList --------------------------------------------


def test_in_all_values_outside_range_prunes():
    # column in [10, 20]; none of {1, 2, 3} can match -> prune
    assert _can_prune_rowgroup("InList", [1, 2, 3], 10, 20) is True


def test_in_one_value_inside_range_keeps():
    # 15 falls in [10, 20] -> cannot prune
    assert _can_prune_rowgroup("InList", [1, 15, 99], 10, 20) is False


def test_in_value_on_boundary_keeps():
    assert _can_prune_rowgroup("InList", [20], 10, 20) is False  # inclusive max
    assert _can_prune_rowgroup("InList", [10], 10, 20) is False  # inclusive min


def test_in_empty_list_prunes():
    # IN () matches nothing
    assert _can_prune_rowgroup("InList", [], 10, 20) is True


def test_in_null_in_list_keeps():
    # A NULL makes a comparison raise -> fail open (keep).
    assert _can_prune_rowgroup("InList", [1, None, 3], 10, 20) is False


def test_in_type_mismatch_keeps():
    # str vs int is incomparable -> keep.
    assert _can_prune_rowgroup("InList", ["a", "b"], 10, 20) is False


def test_in_missing_stats_keeps():
    assert _can_prune_rowgroup("InList", [1, 2], None, 20) is False
    assert _can_prune_rowgroup("InList", [1, 2], 10, None) is False


# --- _can_prune_rowgroup: NotInList -----------------------------------------


def test_not_in_single_value_group_excluded_prunes():
    # whole group is exactly 5, and 5 is excluded -> prune
    assert _can_prune_rowgroup("NotInList", [5, 6], 5, 5) is True


def test_not_in_single_value_group_not_excluded_keeps():
    assert _can_prune_rowgroup("NotInList", [6, 7], 5, 5) is False


def test_not_in_range_group_keeps():
    # a multi-value group can never be fully excluded by a finite NOT IN list
    assert _can_prune_rowgroup("NotInList", [5], 1, 100) is False


# --- row_group_may_satisfy end-to-end ---------------------------------------


def _rg(col, mn, mx):
    return {"columns": [{"name": col, "min": mn, "max": mx}]}


def test_row_group_pruned_when_in_disjoint():
    rg = _rg("region", 100, 200)
    assert row_group_may_satisfy(rg, [("region", "InList", [1, 2, 3])]) is False


def test_row_group_kept_when_in_overlaps():
    rg = _rg("region", 100, 200)
    assert row_group_may_satisfy(rg, [("region", "InList", [1, 150])]) is True


def test_row_group_kept_when_column_absent():
    rg = _rg("other", 100, 200)
    # predicate column not present in this row group -> fail open
    assert row_group_may_satisfy(rg, [("region", "InList", [1, 2])]) is True


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
