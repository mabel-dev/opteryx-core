"""`OPTIMIZE TABLE x FINAL` selection: one contiguous key window, as large as the
disk budget allows, whose single sorted rewrite leaves no output overlapping a file
outside it.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.planner.compaction import CompactionFile
from opteryx.planner.compaction import MIN_SIZE_BYTES
from opteryx.planner.compaction import SelectionOutcome
from opteryx.planner.compaction import select_final_compaction_plan
from opteryx.planner.compaction.selection import _overlap_components
from opteryx.planner.compaction.selection import FileRange

MB = 1024 * 1024
SETTLED = MIN_SIZE_BYTES + MB


def _files(specs):
    """specs: (size, low, high[, deleted]) -> (entries, key_ranges)."""
    entries, ranges = [], []
    for row, spec in enumerate(specs):
        size, low, high = spec[:3]
        deleted = spec[3] if len(spec) > 3 else 0
        entry = CompactionFile(row, f"f{row}", 100, size, deleted)
        entries.append(entry)
        ranges.append((entry, low, high))
    return entries, ranges


def _rows(result):
    return sorted(entry.row for entry in result.plan.files)


def test_components_follow_rule_b_overlap():
    entries, ranges = _files([(1, 0, 10), (1, 10, 20), (1, 20, 20), (1, 5, 8), (1, 30, 40)])
    components = _overlap_components([FileRange(e, lo, hi) for e, lo, hi in ranges])
    # [0,10] holds [5,8]; [10,20] only touches it (a clean split) but the degenerate
    # 20 touches [10,20]; [30,40] stands alone.
    assert [sorted(m.entry.row for m in c) for c in components] == [[0, 3], [1, 2], [4]]


def test_window_spans_clusters_and_bridges_a_settled_file():
    entries, ranges = _files(
        [
            (100 * MB, 0, 10),
            (100 * MB, 5, 15),  # cluster with 0
            (SETTLED, 20, 30),  # settled bridge
            (100 * MB, 40, 50),
            (100 * MB, 45, 55),  # cluster with 3
        ]
    )
    result = select_final_compaction_plan(entries, "k", ranges, disk_budget=100 * 1024 * MB)
    assert result.outcome is SelectionOutcome.PLANNED
    assert _rows(result) == [0, 1, 2, 3, 4]
    assert result.plan.mode == "sort-aware"


def test_settled_files_are_trimmed_off_the_ends():
    entries, ranges = _files([(SETTLED, 0, 1), (100 * MB, 2, 5), (100 * MB, 3, 6), (SETTLED, 7, 8)])
    result = select_final_compaction_plan(entries, "k", ranges, disk_budget=100 * 1024 * MB)
    assert _rows(result) == [1, 2]


def test_budget_picks_the_window_with_most_work():
    entries, ranges = _files(
        [
            (300 * MB, 0, 10),
            (300 * MB, 5, 15),
            (SETTLED, 20, 30),
            (900 * MB, 40, 50),
            (900 * MB, 45, 55),
        ]
    )
    result = select_final_compaction_plan(entries, "k", ranges, disk_budget=2 * 1024 * MB)
    assert _rows(result) == [3, 4]


def test_a_cluster_over_budget_cannot_fit_two_files_is_nothing_to_do():
    entries, ranges = _files([(900 * MB, 0, 10), (900 * MB, 5, 15), (SETTLED, 20, 30)])
    result = select_final_compaction_plan(entries, "k", ranges, disk_budget=1024 * MB)
    assert result.outcome is SelectionOutcome.NOTHING_TO_DO


def test_a_cluster_over_budget_gives_up_its_smallest_files():
    # Every file spans the key range: one component, far over budget.
    entries, ranges = _files([(size * MB, 0, 100) for size in (400, 10, 300, 20, 30, 500)])
    result = select_final_compaction_plan(entries, "k", ranges, disk_budget=400 * MB)
    assert result.outcome is SelectionOutcome.PLANNED
    assert _rows(result) == [1, 2, 3, 4]  # 10 + 20 + 30 + 300 MB
    assert result.plan.reason.startswith("final-partial")


def test_a_partial_beats_a_smaller_window():
    entries, ranges = _files(
        [(100 * MB, 0, 5), (100 * MB, 2, 6)]  # a small cluster that fits
        + [(size * MB, 10, 100) for size in (300, 300, 300)]  # one too large
    )
    result = select_final_compaction_plan(entries, "k", ranges, disk_budget=700 * MB)
    assert _rows(result) == [2, 3]


def test_a_settled_dataset_needs_nothing():
    entries, ranges = _files([(SETTLED, 0, 10), (SETTLED, 20, 30)])
    result = select_final_compaction_plan(entries, "k", ranges, disk_budget=100 * 1024 * MB)
    assert result.outcome is SelectionOutcome.NOTHING_TO_DO


def test_a_settled_file_with_delete_debt_is_rewritten():
    entries, ranges = _files([(SETTLED, 0, 10, 50), (SETTLED, 20, 30)])
    result = select_final_compaction_plan(entries, "k", ranges, disk_budget=100 * 1024 * MB)
    assert _rows(result) == [0]


def test_unknown_sizes_still_count_as_work():
    entries, ranges = _files([(None, 0, 10), (None, 5, 15)])
    result = select_final_compaction_plan(entries, "k", ranges, disk_budget=MB)
    assert _rows(result) == [0, 1]


def test_without_a_sort_key_every_unsettled_and_indebted_file_is_concatenated():
    entries, _ = _files([(MB, 0, 0), (MB, 0, 0), (SETTLED, 0, 0), (SETTLED, 0, 0, 50)])
    result = select_final_compaction_plan(entries, None, None, disk_budget=0)
    assert result.plan.mode == "brute"
    assert _rows(result) == [0, 1, 3]
