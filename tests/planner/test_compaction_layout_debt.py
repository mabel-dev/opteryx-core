"""Rule D, layout debt: a file written without the per-column layout the engine
writes now (`CompactionFile.layout_stale`, set from the footers when
PARQUET_WRITE_NARROW_CHUNK_BYTES is on) is a compaction candidate, so existing
data moves to the layout. Without it OPTIMIZE — and OPTIMIZE FINAL on files
already compacted — selects nothing and the data never moves
(docs/PARQUET_WRITE_FOR_READ_SPEED.md §4.7.3).

Pinned: D runs last, after A-C, with or without a sort key; one file per pass,
rewritten in place (single-file brute combine, so key range and size are kept);
the largest that fits the pass budget first; a single-file dataset qualifies;
FINAL treats a stale file as needing work, alone or in a window; nothing stale,
nothing selected.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.planner.compaction import CompactionFile
from opteryx.planner.compaction import MIN_SIZE_BYTES
from opteryx.planner.compaction import PASS_BUDGET_BYTES
from opteryx.planner.compaction import SMALL_FILE_BYTES
from opteryx.planner.compaction import SelectionOutcome
from opteryx.planner.compaction import select_compaction_plan
from opteryx.planner.compaction import select_final_compaction_plan

MB = 1024 * 1024
SETTLED = max(MIN_SIZE_BYTES, SMALL_FILE_BYTES) + 64 * MB


def _files(specs):
    """specs: (size, low, high, stale) -> (entries, key_ranges)."""
    entries, ranges = [], []
    for row, (size, low, high, stale) in enumerate(specs):
        entry = CompactionFile(row, f"f{row}", 100, size, 0, layout_stale=stale)
        entries.append(entry)
        ranges.append((entry, low, high))
    return entries, ranges


def test_nothing_stale_selects_nothing():
    entries, ranges = _files([(SETTLED, 0, 10, False), (SETTLED, 20, 30, False)])
    assert select_compaction_plan(entries, "k", ranges).outcome is SelectionOutcome.NOTHING_TO_DO
    assert select_compaction_plan(entries, None).outcome is SelectionOutcome.NOTHING_TO_DO
    assert (select_final_compaction_plan(entries, "k", ranges, disk_budget=1 << 40).outcome
            is SelectionOutcome.NOTHING_TO_DO)


def test_a_stale_file_is_rewritten_in_place_one_per_pass():
    entries, ranges = _files([(SETTLED, 0, 10, True), (SETTLED + MB, 20, 30, True),
                              (SETTLED, 40, 50, False)])
    for sort_column, key_ranges in (("k", ranges), (None, None)):
        result = select_compaction_plan(entries, sort_column, key_ranges)
        assert result.outcome is SelectionOutcome.PLANNED
        plan = result.plan
        assert [e.row for e in plan.files] == [1]          # the largest stale file
        assert (plan.strategy, plan.mode) == ("combine", "brute")
        assert plan.reason.startswith("layout-debt")
        assert plan.sort_column == sort_column


def test_layout_debt_runs_after_the_other_rules():
    # Two small files: rule A (or the no-key brute rule) wins the pass.
    small = min(SMALL_FILE_BYTES, MIN_SIZE_BYTES) // 4
    entries, ranges = _files([(small, 0, 1, False), (small, 2, 3, False), (SETTLED, 10, 20, True)])
    for sort_column, key_ranges in (("k", ranges), (None, None)):
        result = select_compaction_plan(entries, sort_column, key_ranges)
        assert result.outcome is SelectionOutcome.PLANNED
        assert not result.plan.reason.startswith("layout-debt"), result.plan.reason


def test_a_single_stale_file_is_planned():
    entries, ranges = _files([(SETTLED, 0, 10, True)])
    result = select_compaction_plan(entries, "k", ranges)
    assert result.outcome is SelectionOutcome.PLANNED
    assert [e.row for e in result.plan.files] == [0]
    entries[0].layout_stale = False
    assert select_compaction_plan(entries, "k", ranges).outcome is SelectionOutcome.SINGLE_FILE


def test_largest_fitting_the_pass_budget_first():
    entries, _ = _files([(PASS_BUDGET_BYTES + MB, 0, 1, True), (SETTLED, 2, 3, True)])
    assert [e.row for e in select_compaction_plan(entries, None).plan.files] == [1]
    entries, _ = _files([(PASS_BUDGET_BYTES + 2 * MB, 0, 1, True), (PASS_BUDGET_BYTES + MB, 2, 3, True)])
    assert [e.row for e in select_compaction_plan(entries, None).plan.files] == [1]


def test_final_counts_a_stale_file_as_needing_work():
    # Settled, disjoint files: FINAL on compacted data — a no-op — until one is stale.
    entries, ranges = _files([(SETTLED, 0, 10, False), (SETTLED, 20, 30, False),
                              (SETTLED, 40, 50, False)])
    assert (select_final_compaction_plan(entries, "k", ranges, disk_budget=1 << 40).outcome
            is SelectionOutcome.NOTHING_TO_DO)
    entries[1].layout_stale = True
    result = select_final_compaction_plan(entries, "k", ranges, disk_budget=1 << 40)
    assert result.outcome is SelectionOutcome.PLANNED
    assert [e.row for e in result.plan.files] == [1]
    # Two stale files: the window spans them (and the settled bridge between).
    entries[2].layout_stale = True
    result = select_final_compaction_plan(entries, "k", ranges, disk_budget=1 << 40)
    assert sorted(e.row for e in result.plan.files) == [1, 2]
    assert result.plan.mode == "sort-aware"


def test_final_without_a_sort_key_takes_every_stale_file():
    entries, _ = _files([(SETTLED, 0, 0, True), (SETTLED, 0, 0, False), (SETTLED, 0, 0, True)])
    result = select_final_compaction_plan(entries, None, None, disk_budget=1 << 40)
    assert sorted(e.row for e in result.plan.files) == [0, 2]
    entries[2].layout_stale = False
    result = select_final_compaction_plan(entries, None, None, disk_budget=1 << 40)
    assert [e.row for e in result.plan.files] == [0]
    assert result.plan.reason.startswith("layout-debt")


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
